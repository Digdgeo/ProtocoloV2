import os
import sys
import rasterio
import subprocess
import stat
import shutil
import re
import time

import numpy as np
import seaborn as sns; sns.set(color_codes=True)
import matplotlib.pyplot as plt

from osgeo import gdal, gdalconst
from urllib.request import urlopen
from datetime import datetime
from scipy import ndimage
from scipy.stats import linregress

# MongoDB Database
from pymongo import MongoClient
client = MongoClient()

database = client.Satelites
db = database.Landsat



class Landsat:
    
    
    """
    Handles USGS Landsat Collection 2 Level-2 Surface Reflectance products.

    This class provides tools for preprocessing Landsat images, including cloud masking,
    radiometric calibration, thermal correction, reprojection, and normalization using
    pseudo-invariant features (PIFs). It also organizes the output into a structured
    folder hierarchy and inserts metadata into MongoDB.

    The class is designed to work with Collection 2 products from Landsat 5, 7, 8, and 9.

    See Also
    --------
    __init__ : Initializes scene attributes, reads MTL metadata, creates output folders, 
               and prepares MongoDB insertion document.
    """
    
    def __init__(self, ruta_escena, inicializar=True):

        """
        Initialize a Landsat object from a given scene path.

        This constructor parses the scene's directory and metadata, identifies the sensor type, 
        sets up internal paths, downloads the quicklook image, and uploads initial metadata 
        to the MongoDB database. If `inicializar` is False, only basic attributes are set 
        without accessing files or the database.

        Parameters
        ----------
        ruta_escena : str
            Path to the directory containing the original downloaded Landsat scene.

        inicializar : bool, optional
            Whether to perform full initialization, including metadata parsing, 
            folder creation, and MongoDB insertion (default is True).

        Attributes
        ----------
        escena : str
            Scene folder name extracted from the given path.

        last_name : str
            Internal ID for the scene, used as MongoDB `_id`, based on date, sensor, path and row.

        sensor : str
            Sensor name, one of 'OLI', 'ETM+', or 'TM'.

        path : str
            Path value extracted from the scene ID (3 digits).

        row : str
            Row value extracted from the scene ID (2 digits, zero-padded).

        base, ori, pro, geo, rad, nor, data, temp : str
            Paths to the root folder and its subdirectories.

        pro_escena, geo_escena, rad_escena, nor_escena : str
            Scene-specific folders for storing outputs.

        mtl : dict
            Dictionary with parsed metadata from the MTL file.

        bandas_normalizadas : list of str
            List of spectral bands successfully normalized for this scene.

        cloud_mask_values : list of int
            Values used to identify cloud or fill pixels, depending on the sensor.

        qk_name : str
            Path to the downloaded Landsat quicklook JPEG.

        pn_cover : float or None
            Percentage of cloud cover over Doñana, set later in processing.

        newesc : dict
            Document inserted into MongoDB with basic scene metadata and processing info.
        """

        
        self.ruta_escena = ruta_escena

        if not inicializar:
            return

        self.escena = os.path.split(self.ruta_escena)[1]
        self.ori = os.path.split(self.ruta_escena)[0]
        self.base = os.path.split(self.ori)[0]

        self.data = os.path.join(self.base, 'data')
        self.geo = os.path.join(self.base, 'geo')
        self.rad = os.path.join(self.base, 'rad')
        self.nor = os.path.join(self.base, 'nor')
        self.pro = os.path.join(self.base, 'pro')

        data_escena = self.escena.split('_')
        self.sat = "L" + data_escena[0][-1]
        sensores = {'L4': 'TM', 'L5': 'TM', 'L7': 'ETM+', 'L8': 'OLI', 'L9': 'OLI'}
        self.sensor = sensores[self.sat]
        self.nprocesado = data_escena[1]
        self.path = data_escena[2][:3]
        self.row = data_escena[2][-3:]
        self.escena_date = data_escena[3]
        self.escena_procesado_date = data_escena[4]
        self.collection = data_escena[5]
        self.tier = data_escena[6]

        self.cloud_mask_values = [21824, 21952] if self.sensor == 'OLI' else [5440, 5504]

        if self.sensor == 'ETM+':
            self.last_name = (self.escena_date + self.sat + self.sensor[:-1] + self.path + '_' + self.row[1:]).lower()
        else:
            self.last_name = (self.escena_date + self.sat + self.sensor + self.path + '_' + self.row[1:]).lower()

        self.pro_escena = os.path.join(self.pro, self.last_name)
        os.makedirs(self.pro_escena, exist_ok=True)

        self.geo_escena = os.path.join(self.geo, self.last_name)
        os.makedirs(self.geo_escena, exist_ok=True)

        self.rad_escena = os.path.join(self.rad, self.last_name)
        os.makedirs(self.rad_escena, exist_ok=True)

        self.nor_escena = os.path.join(self.nor, self.last_name)
        os.makedirs(self.nor_escena, exist_ok=True)

        self.equilibrado = os.path.join(self.data, 'Equilibrada.tif')
        self.noequilibrado = os.path.join(self.data, 'NoEquilibrada.tif')
        self.parametrosnor = {}
        self.iter = 1

        # Lista para guardar las bandas que se normalizan y dar la información en el correo
        self.bandas_normalizadas = []

        self.mtl = {}
        for i in os.listdir(self.ruta_escena):
            if i.endswith('MTL.txt'):
                mtl = os.path.join(self.ruta_escena, i)
                with open(mtl, 'r') as f:
                    for line in f.readlines():
                        if "=" in line:
                            l = line.split("=")
                            self.mtl[l[0].strip()] = l[1].strip()

        if self.sat in ['L8', 'L9']:
            for i in os.listdir(self.ruta_escena):
                if i.endswith('.TIF'):
                    banda = os.path.splitext(i)[0].split('_')[-1]
                    setattr(self, banda.lower(), os.path.join(self.ruta_escena, i))
        elif self.sat in ['L7', 'L5']:
            for i in os.listdir(self.ruta_escena):
                if i.endswith('.TIF'):
                    banda = os.path.splitext(i)[0].split('_')[-1]
                    setattr(self, banda.lower(), os.path.join(self.ruta_escena, i))

        url_base = 'https://landsatlook.usgs.gov/gen-browse?size=rrb&type=refl&product_id={}'.format(self.mtl['LANDSAT_PRODUCT_ID'].strip('""'))
        self.qk_name = os.path.join(self.ruta_escena, self.escena + '_Quicklook.jpeg')

        if not os.path.exists(self.qk_name):
            with open(self.qk_name, 'wb') as qk:
                qk.write(urlopen(url_base).read())
            print('Quicklook descargado')
        else:
            print('El Quicklook ya estaba previamente descargado')

        print('Landsat iniciada con éxito')

        self.pn_cover = None
        self.newesc = {
            '_id': self.last_name,
            'usgs_id': self.mtl['LANDSAT_SCENE_ID'][1:-1],
            'tier_id': self.mtl['LANDSAT_PRODUCT_ID'][1:-1],
            'lpgs': self.mtl['PROCESSING_SOFTWARE_VERSION'][1:-1],
            'category': self.mtl['COLLECTION_CATEGORY'][1:-1],
            'Clouds': {
                'cloud_scene': float(self.mtl['CLOUD_COVER']),
                'land cloud cover': float(self.mtl['CLOUD_COVER_LAND'])
            },
            'Info': {
                'Tecnico': 'LAST-EBD Auto',
                'Iniciada': datetime.now(),
                'Pasos': {'rad': '', 'nor': ''}
            }
        }

        try:
            db.insert_one(self.newesc)
        except Exception:
            db.update_one({'_id': self.last_name}, {'$set': {'Info.Iniciada': datetime.now()}}, upsert=True)

        print('Landsat instanciada y subida a la base de datos')


    def get_hillshade(self):

        """
        Generate a hillshade raster using a fixed DTM and scene solar metadata.

        This method uses the solar azimuth and elevation values from the scene's MTL file
        to compute a hillshade raster from a pre-defined Digital Terrain Model (DTM)
        specific to path/row 202/034. The result is saved in the `nor` folder of the scene.

        Raises
        ------
        RuntimeError
            If the `gdaldem hillshade` command fails during execution.
        """

        dtm = os.path.join(self.data, 'dtm_202_34.tif') #Por defecto esta en 29 y solo para la 202_34
        azimuth = self.mtl['SUN_AZIMUTH']
        elevation = self.mtl['SUN_ELEVATION']
        
        #Una vez tenemos estos parametros generamos el hillshade
        salida = os.path.join(self.nor_escena, 'hillshade.tif')
        cmd = ["gdaldem", "hillshade", "-az", "-alt", "-of", "GTIFF"]
        cmd.append(dtm)
        cmd.append(salida)
        cmd.insert(3, str(azimuth))
        cmd.insert(5, str(elevation))
        
        proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout,stderr=proc.communicate()
        exit_code=proc.wait()

        if exit_code: 
            raise RuntimeError(stderr)
        else:
            print(stdout)
            print('Hillshade generado')
        
        
    def get_cloud_pn(self):

        """
        Calculate the cloud coverage percentage over Doñana National Park.

        This method uses the QA_PIXEL cloud mask and a shapefile representing the 
        boundaries of Doñana National Park to compute the percentage of valid pixels 
        (i.e., clear land or clear water) within the park. The result is stored 
        in the MongoDB document under the field `cloud_PN`.

        Raises
        ------
        RuntimeError
            If the `gdalwarp` command fails during execution.

        Exception
            If an error occurs while updating the MongoDB document.
        """

        shape = os.path.join(self.data, 'Limites_PN_Donana.shp')
        crop = "-crop_to_cutline"

        for i in os.listdir(self.ruta_escena):
            if i.endswith('QA_PIXEL.TIF'):
                cloud = os.path.join(self.ruta_escena, i)

        #usamos Gdalwarp para realizar las mascaras, llamandolo desde el modulo subprocess
        cmd = ["gdalwarp", "-dstnodata" , "0" , "-cutline", ]
        path_masks = os.path.join(self.ruta_escena, 'masks')
        os.makedirs(path_masks, exist_ok=True)


        salida = os.path.join(path_masks, 'cloud_PN.TIF')
        cmd.insert(4, shape)
        cmd.insert(5, crop)
        cmd.insert(6, cloud)
        cmd.insert(7, salida)

        proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout,stderr=proc.communicate()
        exit_code=proc.wait()

        if exit_code: 
            raise RuntimeError(stderr)


        ds = gdal.Open(salida)
        cloud = np.array(ds.GetRasterBand(1).ReadAsArray())
        
        mask = (cloud == self.cloud_mask_values[0]) | (cloud == self.cloud_mask_values[1])
        
        #################
        ##### DEFINIR LOS VALORES PARA LA MASCARA Y AÑADIR EL DATO A MONGO

        cloud_msk = cloud[mask]
        #print(cloud_msk.size)
        clouds = float(cloud_msk.size * 900)
        #print(clouds)
        PN = 533740500 
        self.pn_cover = round(100 - (clouds/PN) * 100, 2)
        ds = None
        cloud = None
        cloud_msk = None
        clouds = None        

        try:

            db.update_one({'_id': self.last_name}, {'$set':{'Clouds.cloud_PN': self.pn_cover}},  upsert=True)

        except Exception as e:
            print("Unexpected error:", type(e), e)

        print("El porcentaje de nubes en el Parque Nacional es de " + str(self.pn_cover))
        

    def get_cloud_rbios(self):
        """
        Calculate the cloud coverage percentage over the Doñana Biosphere Reserve.
    
        This method uses the QA_PIXEL cloud mask and a shapefile representing the 
        boundaries of the Doñana Biosphere Reserve to compute the percentage of valid 
        pixels (i.e., clear land or clear water) within the reserve. The result is 
        stored in the MongoDB document under the field `Clouds.cloud_RBIOS`.
    
        The calculation is performed by:
        1. Clipping the QA_PIXEL band to the Biosphere Reserve boundaries
        2. Counting pixels classified as clear land (21824/5440) or clear water (21952/5504)
        3. Computing the percentage of cloud-free area
    
        Notes
        -----
        The area constant (RBIOS_AREA) must be adjusted to match the actual area 
        of the RBIOS.shp shapefile in square meters.
    
        Raises
        ------
        RuntimeError
            If the `gdalwarp` command fails during execution.
        Exception
            If an error occurs while updating the MongoDB document.
    
        See Also
        --------
        get_cloud_pn : Calculate cloud coverage over Doñana National Park.
        """
        
        shape = os.path.join(self.data, 'RBIOS.shp')
        crop = "-crop_to_cutline"
    
        for i in os.listdir(self.ruta_escena):
            if i.endswith('QA_PIXEL.TIF'):
                cloud = os.path.join(self.ruta_escena, i)
    
        cmd = ["gdalwarp", "-dstnodata", "0", "-cutline"]
        path_masks = os.path.join(self.ruta_escena, 'masks')
        os.makedirs(path_masks, exist_ok=True)
    
        salida = os.path.join(path_masks, 'cloud_RBIOS.TIF')
        cmd.insert(4, shape)
        cmd.insert(5, crop)
        cmd.insert(6, cloud)
        cmd.insert(7, salida)
    
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        exit_code = proc.wait()
    
        if exit_code: 
            raise RuntimeError(stderr)
    
        ds = gdal.Open(salida)
        cloud = np.array(ds.GetRasterBand(1).ReadAsArray())
        
        mask = (cloud == self.cloud_mask_values[0]) | (cloud == self.cloud_mask_values[1])
        
        cloud_msk = cloud[mask]
        clouds = float(cloud_msk.size * 900)
        
        # TODO: Adjust this value to match the actual area of RBIOS.shp
        RBIOS_AREA = 2680000000  # Approximately 268,000 ha in m²
        
        rbios_cover = round(100 - (clouds/RBIOS_AREA) * 100, 2)
        self.rbios_cover = rbios_cover
        
        ds = None
        cloud = None
        cloud_msk = None
        clouds = None        
    
        try:
            db.update_one(
                {'_id': self.last_name}, 
                {'$set': {'Clouds.cloud_RBIOS': rbios_cover}}, 
                upsert=True
            )
        except Exception as e:
            print("Unexpected error:", type(e), e)
    
        print("El porcentaje de nubes en la Reserva de la Biosfera es de " + str(rbios_cover))


    def remove_masks(self):

        """
        Delete temporary cloud mask files generated during processing.

        This method removes all files inside the `masks/` subfolder of the Landsat scene 
        directory and then deletes the folder itself. It is typically called after 
        cloud coverage has been calculated and the intermediate mask files are no longer needed.

        Raises
        ------
        OSError
            If any file or the `masks/` folder cannot be deleted.
        """

        path_masks = os.path.join(self.ruta_escena, 'masks')
        for i in os.listdir(path_masks):

            name = os.path.join(path_masks, i)
            os.chmod(name, stat.S_IWRITE)
            os.remove(name)

        shutil.rmtree(path_masks)


    def apply_gapfill(self):

        """
        Apply gap-filling to Landsat 7 bands acquired after June 2003.

        This method detects if the input scene corresponds to Landsat 7 after the 
        Scan Line Corrector (SLC) failure (June 2003). If so, it uses GDAL's 
        `FillNodata` algorithm to interpolate missing pixels (gaps) in each 
        reflectance band.

        The method only processes the following bands: blue, green, red, nir, swir1, and swir2.

        Raises
        ------
        RuntimeError
            If a band cannot be opened or processed during the gap-filling operation.
        """

        # Verificar si es Landsat 7 y la fecha de adquisición es posterior a junio de 2003
        if self.sat == "L7" and datetime.strptime(self.escena_date, "%Y%m%d") > datetime(2003, 6, 1):
            print("Aplicando gapfill a las bandas de Landsat 7 posteriores a junio de 2003.")

            # Mapeo de bandas para Landsat 7 (ETM+)
            band_mapping = {
                'b1': 'blue',
                'b2': 'green', 
                'b3': 'red',
                'b4': 'nir',
                'b5': 'swir1',
                'b7': 'swir2'
            }
            
            for band_attr, band_name in band_mapping.items():
                if hasattr(self, band_attr):
                    band_path = getattr(self, band_attr)
                    if band_path and os.path.exists(band_path):
                        print(f"Aplicando gapfill a la banda {band_name} ({band_path})")

                        # Usar GDAL para llenar los valores NoData en el archivo original
                        src_ds = gdal.Open(band_path, gdalconst.GA_Update)
                        if src_ds is not None:
                            gdal.FillNodata(src_ds.GetRasterBand(1), maskBand=None, maxSearchDist=10, smoothingIterations=1)
                            src_ds = None  # Liberar el dataset después de modificar
                        else:
                            print(f"No se pudo abrir la banda {band_path} para gapfill.")
                    else:
                        print(f"Banda {band_name} no encontrada o no existe.")
                else:
                    print(f"Atributo {band_attr} no encontrado en el objeto Landsat.")

            print("Gapfill aplicado exitosamente a las bandas de Landsat 7.")


    def projwin(self):

        """
        Apply projection, resolution, and geographic extent to all valid bands.

        This method uses `gdalwarp` to reproject the reflectance and thermal bands 
        to a common spatial reference system, apply a standard 30-meter resolution, 
        and clip them to a predefined extent defined by a WRS-2 shapefile. It accounts 
        for differences in band naming between OLI (Landsat 8/9) and TM/ETM+ (Landsat 4/5/7) sensors.

        The output files are saved in the `geo` directory of the scene.

        Raises
        ------
        RuntimeError
            If any band fails to process during the reprojection or clipping steps.
        """     
        
        olibands = {'B1': 'cblue_b1', 'B2': 'blue_b2', 'B3': 'green_b3', 'B4': 'red_b4', 'B5': 'nir_b5', 'B6': 'swir1_b6',
                   'B7': 'swir2_b7', 'PIXEL': 'fmask', 'B10': 'lst'}
        
        etmbands = {'B1': 'blue_b1', 'B2': 'green_b2', 'B3': 'red_b3', 'B4': 'nir_b4', 'B5': 'swir1_b5',
                   'B7': 'swir2_b7', 'PIXEL': 'fmask', 'B6': 'lst'}
        
        
        #geo = '/media/diego/31F8C0B3792FC3B6/EBD/Protocolo_v2_2024/geo'
        #path_rad = os.path.join(self.geo, self.escena)
        #os.makedirs(path_rad, exist_ok=True)
        wrs = os.path.join(self.data, 'wrs_202034.shp')
        
        for i in os.listdir(self.ruta_escena):
            
            if self.sat in ['L8', 'L9']:
                if i.endswith('.TIF'):

                    banda = i.split('_')[-1][:-4]

                    if banda in olibands.keys():
                        ins = os.path.join(self.ruta_escena, i)

                        name = self.escena_date + self.sat + self.sensor + self.path + '_' + self.row[1:] + '_g2_' + olibands[banda] + '.tif'
                        out = os.path.join(self.geo_escena, name.lower())

                        cmd = "gdalwarp -ot Int32 -srcnodata 0 -dstnodata '-9999' -tr 30 30 -te 633570 4053510 851160 4249530 -tap -cutline {} {} {}".format(wrs, ins, out)
                        print(cmd)
                        os.system(cmd)

                    else:
                        continue
                        
            elif self.sat in ['L7', 'L5', 'L4']:
                if i.endswith('.TIF'):

                    banda = i.split('_')[-1][:-4]

                    if banda in etmbands.keys():
                        ins = os.path.join(self.ruta_escena, i)

                        name = self.escena_date + self.sat + self.sensor + self.path + '_' + self.row[1:] + '_g2_' + etmbands[banda] + '.tif'
                        out = os.path.join(self.geo_escena, name.lower())

                        cmd = "gdalwarp -ot Int32 -srcnodata 0 -dstnodata '-9999' -tr 30 30 -te 633570 4053510 851160 4249530 -tap -cutline {} {} {}".format(wrs, ins, out)
                        print(cmd)
                        os.system(cmd)

                    else:
                        continue
                
                
            else:
                print('Lo siento, pero no encuentro el satélite')
                
                
    def coef_sr_st(self):

        """
        Apply surface reflectance and land surface temperature coefficients to image bands.

        This method scales the radiometrically corrected bands using standard coefficients
        to produce physically meaningful values:

        - Reflectance bands (blue, green, red, NIR, SWIR1, SWIR2) are rescaled to the [0, 1] range.
        - The thermal band (LST) is converted from digital numbers to degrees Celsius.
        - The fmask (cloud mask) band is copied directly without modification.

        The processed bands are saved in the `rad` (radiometric correction) and 
        `pro` (final products) directories.

        Raises
        ------
        RuntimeError
            If any band cannot be processed or written to disk.
        """
    
        #path_geo = os.path.join(self.geo, self.escena)
        #path_rad = os.path.join(self.rad, self.escena)
        #os.makedirs(path_rad, exist_ok=True)
        
        for i in os.listdir(self.geo_escena):
    
            if i.endswith('.tif'):
    
                banda = i.split('_')[-1][:-4]
                
    
                if banda not in ['fmask', 'lst']:

                    print("Aplicando coeficientes a banda", banda)
                    
                    #nombre de salida que reemplace la  _g2_ del nombre original por _gr2_
                    rs = os.path.join(self.geo_escena, i)
                    out = os.path.join(self.rad_escena, i.replace('_g2_', '_gr2_'))
    
                    with rasterio.open(rs) as src:
                        RS = src.read(1)
                        meta = src.meta
    
                    # Aplicar coeficientes de reflectancia
                    sr = RS * 0.0000275 - 0.2
    
                    # Ajustar los valores al rango 0-1
                    sr = np.clip(sr, 0, 1)
                    
                    # Mantener los valores NoData
                    sr = np.where(RS == -9999, -9999, sr)
                    
                    meta.update(dtype=rasterio.float32)
    
                    with rasterio.open(out, 'w', **meta) as dst:
                        dst.write(sr.astype(rasterio.float32), 1)
    
                elif banda == 'lst':

                    print("Aplicando coeficientes a banda", banda)
                
                    #nombre de salida que reemplace la  _g2_ del nombre original por _gr2_
                    rs = os.path.join(self.geo_escena, i)
                    out = os.path.join(self.pro_escena, i.replace('_g2_', '_'))
    
                    with rasterio.open(rs) as src:
                        RS = src.read(1)
                        meta = src.meta
    
                    # Aplicar coeficientes de temperatura
                    lst = RS * 0.00341802 + 149.0
                    lst -= 273.15
    
                    # Mantener los valores NoData
                    lst = np.where(RS == -9999, -9999, lst)
                    
                    meta.update(dtype=rasterio.float32)
    
                    with rasterio.open(out, 'w', **meta) as dst:
                        dst.write(lst.astype(rasterio.float32), 1)
    
                elif banda == 'fmask':

                    print("Copiando", banda)
                    src = rs = os.path.join(self.geo_escena, i)
                    dst = os.path.join(self.rad_escena, i.replace('_g2_', '_'))                
                    shutil.copy(src, dst)
    
                else:                                       
                    continue
    
        print('Coeficientes aplicados con éxito')

                 
            
    def normalize(self):
        
        """
        Perform full band normalization using invariant areas and cloud masking.

        This method normalizes all spectral bands by comparing them with a reference image 
        over predefined pseudo-invariant features (PIFs). It applies cloud masking and iteratively 
        adjusts regression parameters to meet quality thresholds.

        For each band:
        - Calls `nor1()` to compute regression parameters.
        - If parameters satisfy quality criteria (R² > 0.85 and at least 10 valid pixels 
        per invariant area), normalization is applied using `nor2l8()`.
        - Normalization parameters and diagnostics are saved and stored in MongoDB.

        Outputs
        -------
        - Normalized bands saved in `nor_escena` with `_grn2_` suffix.
        - Diagnostic plots and normalization logs.
        - A `coeficientes.txt` file with per-band regression statistics.
        - Normalization metadata inserted into MongoDB.

        Raises
        ------
        RuntimeError
            If normalization fails for one or more bands.
        """

        #path_rad = os.path.join(self.rad, self.escena)
                
        bandas = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']

        #Vamos a pasar las bandas recortadas desde temp
        for i in os.listdir(self.rad_escena):
                    
            if re.search('b[1-7].tif$', i):
                
                banda = os.path.join(self.rad_escena, i)
                banda_num = i.split('_')[-2]
                
                print('Estamos en NORMALIZE con la banda', banda, 'que es la', banda_num, 'desde normalize')
                #Primera llamada a nor1
                self.iter = 1
                self.nor1(banda, self.noequilibrado)
                
                #Esto es un poco feo, pero funciona. Probar a hacerlo con una lista de funciones
                if banda_num not in self.parametrosnor.keys():
                    
                    self.iter += 1
                    print('Iteracion', self.iter)
                    self.nor1(banda, self.noequilibrado, coef = 2)
                    if banda_num not in self.parametrosnor.keys():
                        self.iter += 1
                        print('Iteracion', self.iter)
                        self.nor1(banda, self.equilibrado)
                        if banda_num not in self.parametrosnor.keys():
                            self.iter += 1
                            print('Iteracion', self.iter)
                            self.nor1(banda, self.equilibrado, coef = 2)
                            if banda_num not in self.parametrosnor.keys():
                                self.iter += 1
                                print('Iteracion', self.iter)
                                self.nor1(banda, self.noequilibrado, coef = 3,)
                                if banda_num not in self.parametrosnor.keys():
                                    self.iter += 1
                                    print('Iteracion', self.iter)
                                    self.nor1(banda, self.equilibrado, coef = 3)
                                else:
                                    print('No se ha podido normalizar la banda ', banda_num)
                                    
            #Una vez acabados los bucles guardamos los coeficientes en un txt. Redundante pero asi hay 
            #que hacerlo porque quiere David
            #path_nor = os.path.join(self.nor, self.escena)
            #os.makedirs(path_nor, exist_ok=True)
            arc = os.path.join(self.nor_escena, 'coeficientes.txt')
            f = open(arc, 'w')
            for i in sorted(self.parametrosnor.items()):
                f.write(str(i)+'\n')
            f.close()  
            
            #Insertamos los Kls en la base de datos
            #connection = pymongo.MongoClient("mongodb://localhost")
            #db=connection.teledeteccion
            #landsat = db.landsat

            # Lista con las bandas normalizadas para el mail
            self.bandas_normalizadas = sorted(self.parametrosnor.keys())

            try:

                db.update_one({'_id': self.last_name}, {'$set':{'Info.Pasos.nor': 
                        {'Normalize': 'True', 'Nor-Values': self.parametrosnor, 'Fecha': datetime.now()}}})

            except Exception as e:
                print("Unexpected error:", type(e), e)
        
        
        
    def nor1(self, banda, mascara, coef = 1):
        
        """
        Perform linear normalization on a single band using pseudo-invariant areas (PIFs).

        This method compares a target image band with its homologous band from a fixed 
        reference scene (e.g., August 2022) using only pixels that are cloud-free and 
        fall within predefined regions of interest defined in the PIFs mask.

        It performs a first linear regression, computes residuals, and removes outliers
        based on a standard deviation threshold multiplied by `coef`. A second regression 
        is then fitted to the filtered data.

        If the second regression meets quality criteria (R > 0.85 and at least 10 valid pixels 
        per class), the slope and intercept are stored, and the normalized image is 
        subsequently generated via `nor2l8()`.

        Parameters
        ----------
        banda : str
            Path to the input image band to be normalized.

        mascara : str
            Path to the PIFs mask image (e.g., `Equilibrada.tif`, `NoEquilibrada.tif`).

        coef : int, optional
            Multiplier for the standard deviation of residuals used to filter outliers 
            in the second regression. Default is 1.

        Results
        -------
        - Regression coefficients and pixel counts are saved to `self.parametrosnor`.
        - Normalized image is generated via `nor2l8()` if criteria are met.
        - Diagnostic plots are produced comparing both regressions.
        """

        print('comenzando nor1')
        
        #Ruta a las bandas usadas para normalizar  /media/diego/Datos4/EBD/Protocolo_v2_2024/data/ref
        path_blue = os.path.join(self.data, '20220802l8oli202_34_gr2_blue_b2.tif')
        path_green = os.path.join(self.data, '20220802l8oli202_34_gr2_green_b3.tif')
        path_red = os.path.join(self.data, '20220802l8oli202_34_gr2_red_b4.tif')
        path_nir = os.path.join(self.data, '20220802l8oli202_34_gr2_nir_b5.tif')
        path_swir1 = os.path.join(self.data, '20220802l8oli202_34_gr2_swir1_b6.tif')
        path_swir2 = os.path.join(self.data, '20220802l8oli202_34_gr2_swir2_b7.tif')
        
        dnorbandas = {'blue': path_blue, 'green': path_green, 'red': path_red, 'nir': path_nir, 'swir1': path_swir1, 'swir2': path_swir2}
        #dnorbandasl7 = {'B1': path_blue, 'B2': path_green, 'B3': path_red, 'B4': path_nir, 'B5': path_swir1, 'B7': path_swir2}
        
        #if self.sensore == 'OLI':
            #dnorbandas = dnorbandasl8
        #else:
            #dnorbandas = dnorbandasl7
            
        #path_nor = os.path.join(self.nor, self.escena)
        #path_rad = os.path.join(self.rad, self.escena)

        # Copiamos la banda de Fmask a nor

        clouds = [i for i in os.listdir(self.rad_escena) if 'fmask' in i][0]
        src = os.path.join(self.rad_escena, clouds)
        dst = os.path.join(self.nor_escena, clouds.replace('_gr2_', '_grn2_'))
        
        shutil.copy(src, dst)
                    
        mask_nubes = dst
        print('Mascara de nubes: ', mask_nubes)
        
        if mascara == self.noequilibrado:
            poly_inv_tipo = os.path.join(self.data, 'NoEquilibrada.tif')
        else:
            poly_inv_tipo = os.path.join(self.data, 'Equilibrada.tif')

        print('mascara: ', mascara)
                            
        with rasterio.open(mask_nubes) as nubes:
            CLOUD = nubes.read()
                
        #Abrimos el raster con los rois
        with rasterio.open(poly_inv_tipo) as pias:
            PIAS = pias.read()

        banda_num = banda.split('_')[-2]
        print('----------------La banda num en nor 1 es----------------', banda_num)
        if banda_num in dnorbandas.keys():
            with rasterio.open(banda) as current:
                CURRENT = current.read()
                print('Banda actual: ', banda, 'Shape:', CURRENT.shape)
            #Aqui con el diccionario nos aseguramos de que estamos comparando cada banda con su homologa del 20020718
            with rasterio.open(dnorbandas[banda_num]) as ref:
                REF = ref.read()
                print('Referencia: ', dnorbandas[banda_num], 'Shape:', REF.shape)
            
            #Ya tenemos todas las bandas de la imagen actual y de la imagen de referencia leidas como array
            REF2 = REF[((CURRENT != -9999) & (PIAS != 0)) & ((CLOUD == self.cloud_mask_values[0]) | (CLOUD == self.cloud_mask_values[1]))] #los valores de Fmask usados son: 21824 Tierra limpia y 21952 Agua clara
            BANDA2 = CURRENT[((CURRENT != -9999) & (PIAS != 0)) & ((CLOUD == self.cloud_mask_values[0]) | (CLOUD == self.cloud_mask_values[1]))]
            PIAS2 = PIAS[((CURRENT != -9999) & (PIAS != 0)) & ((CLOUD == self.cloud_mask_values[0]) | (CLOUD == self.cloud_mask_values[1]))]
            
            #Realizamos la primera regresion
            First_slope, First_intercept, r_value, p_value, std_err = linregress(BANDA2,REF2)
            print ('\n++++++++++++++++++++++++++++++++++')
            print('slope: '+ str(First_slope), 'intercept:', First_intercept, 'r', r_value, 'N:', PIAS2.size)
            print ('++++++++++++++++++++++++++++++++++\n')
                        
            esperado = BANDA2 * First_slope + First_intercept
            residuo = REF2 - esperado
            #print('DESVIACION TÍPICA PRIMERA REGRESION:', std_err) COMO DE BUENO ES EL AJUSTE (SLOPE DAVID)
            print('RESIDUO STD:', residuo.std())
            print('RESIDUO STD_DDOF:', residuo.std(ddof=1))
            std = residuo.std() * coef
            print('STD:', std, 'COEF:', coef)
                        
            #Ahora calculamos el residuo para hacer la segunda regresion

            mask_current_PIA_NoData_STD = np.ma.masked_where(abs(residuo)>=std, BANDA2)
            mask_ref_PIA_NoData_STD = np.ma.masked_where(abs(residuo)>=std,REF2)
            mask_pias_PIA_NoData_STD = np.ma.masked_where(abs(residuo)>=std,PIAS2)

            current_PIA_NoData_STD = np.ma.compressed(mask_current_PIA_NoData_STD)
            ref_PIA_NoData_STD = np.ma.compressed(mask_ref_PIA_NoData_STD)
            pias_PIA_NoData_STD = np.ma.compressed(mask_pias_PIA_NoData_STD)
                       
            
            #Hemos enmascarado los resiudos, ahora calculamos la 2 regresion
            slope, intercept, r_value, p_value, std_err = linregress(current_PIA_NoData_STD,ref_PIA_NoData_STD)
            print ('\n++++++++++++++++++++++++++++++++++')
            print ('slope: '+ str(slope), 'intercept:', intercept, 'r', r_value, 'N:', len(ref_PIA_NoData_STD))
            print ('++++++++++++++++++++++++++++++++++\n')
            
            
            #Comprobamos el numero de pixeles por cada area pseudo invariante
            values = {}
            values_str = {1: 'Mar', 2: 'Embalses', 3: 'Pinar', 
                          4: 'Urbano-1', 5: 'Urbano-2', 6: 'Aeropuertos', 7: 'Arena', 8: 'Pastizales', 9: 'Mineria'}
            
            print('Vamos a sacar el count de cada zona (dict)')
            for i in range(1,10):

                mask_pia_= np.ma.masked_where(pias_PIA_NoData_STD != i, pias_PIA_NoData_STD)
                PIA = np.ma.compressed(mask_pia_)
                a = PIA.tolist()
                values[values_str[i]] = len(a)
                print('Values_dict:', values)
            
            #pasamos las claves de cada zona a string
            print(banda_num)
            #Generamos el raster de salida despues de aplicarle la ecuacion de regresion. Esto seria el nor2
            #Por aqui hay que ver como se soluciona
            if r_value > 0.85 and min(values.values()) >= 10:
                self.parametrosnor[banda_num]= {'Parametros':{'slope': slope, 'intercept': intercept, 'std': std,
                        'r': r_value, 'N': len(ref_PIA_NoData_STD), 'iter': self.iter}, 'Tipo_Area': values}
                
                print('parametros en nor1: ', self.parametrosnor)
                print('\ncomenzando nor2 con la banda:', banda[-6:-4], '\n')
                #Hemos calculado la regresion con las bandas recortadas con Rois_extent
                #Ahora vamos a pasar las bandas de rad (completas) para aplicar la ecuacion de regresion
                #path_rad = os.path.join(self.rad, self.escena)
                print('Ruta Rad:', self.rad_escena)
                for r in os.listdir(self.rad_escena):
                    print('BANDA', banda[-6:-4])
                    if banda[-6:-4] in r and r.endswith('.tif'):
                        print('banda:', r)
                        raster = os.path.join(self.rad_escena, r)
                        print('La banda que se va a normalizar es:', raster)

                        if r_value > 0.85 and min(values.values()) >= 10:
                            self.nor2l8(raster, slope, intercept)# Aqui hay que cambiar para que llame a las bandas de rad
                            print('\nNormalizacion de ', banda_num, ' realizada.\n')
                     
                            fig = plt.figure(figsize=(15,10))
                            ax1 = fig.add_subplot(121)
                            ax2 = fig.add_subplot(122)
                            ax1.set_ylim((0, 1))
                            ax1.set_xlim((0, 1))
                            ax2.set_ylim((0, 1))
                            ax2.set_xlim((0, 1))
                            
                            sns.regplot(x=BANDA2, y=REF2, color='g', ax=ax1,
                                line_kws={'color': 'grey', 'label': "y={0:.5f}x+{1:.5f}".format(First_slope, First_intercept)}
                            ).set_title('Regresion PIAs')
                            
                            sns.regplot(x=current_PIA_NoData_STD, y=ref_PIA_NoData_STD, color='b', ax=ax2,
                                line_kws={'color': 'grey', 'label': "y={0:.5f}x+{1:.5f}".format(slope, intercept)}
                            ).set_title('Regresion PIAs-STD')
                            
                            #Legend
                            ax1.legend()
                            ax2.legend()
                            
                            title_ = os.path.split(banda)[1][:-4] + '. Iter: ' + str(self.iter)
                            fig.suptitle(title_, fontsize=15, weight='bold')

                            reg_name = os.path.join(self.nor_escena, os.path.split(banda)[1][:-4])+'.png'
                            reg_nname = reg_name.replace('gr2', 'grn2')
                            plt.savefig(reg_nname)
                            plt.show()

                        else:
                            print('no se puede normalizar la banda', banda)
                            pass
                            
            else:
                print('no se puede normalizar la banda', banda)
                pass
                                       
                    
    def nor2l8(self, banda, slope, intercept):
    
        """
        Apply a linear normalization equation to a Landsat band and save the output raster.

        This method applies a linear transformation to a reflectance-corrected Landsat band
        using a given slope and intercept from a regression model. It ensures that pixel 
        values remain within the valid range and that NoData values are preserved.

        The normalized raster is saved with `_grn2_` in the filename and stored in the 
        corresponding `nor_escena` folder.

        Parameters
        ----------
        banda : str
            Path to the input raster file to be normalized.

        slope : float
            Regression slope to apply in the normalization equation.

        intercept : float
            Regression intercept to apply in the normalization equation.

        Notes
        -----
        - Output values below 0 are clipped to 0.
        - Output values equal to or above 1 are clipped to 1.
        - NoData values (-9999) in the input are preserved in the output.
        """
                
        print('estamos en nor2!')
        #path_rad = os.path.join(self.rad, self.escena)
        #path_nor = os.path.join(self.nor, self.escena)
        
        banda_num = banda.split('_')[-2]

        #nombre de salida que reemplace la  _g2_ del nombre original por _gr2_
        #rs = os.path.join(path_geo, i)
        #out = os.path.join(path_rad, i.replace('_g2_', '_gr2_'))
        
        #outFile = os.path.join(path_nor, self.escena + '_grn2_' + banda_num + '.tif')
        print('NOMBRE DE BANDA EN NOR2', os.path.split(banda)[1])
        outFile = os.path.join(self.nor_escena, os.path.split(banda)[1]).replace('_gr2_', '_grn2_')
        print('Outfile', outFile)
        
        #Metemos la referencia para el NoData, vamos a coger la banda 5 en rad (... Y por que no?)
        for i in os.listdir(self.rad_escena):
            
            if 'nir' in i:
                ref = os.path.join(self.rad_escena, i)
        
        with rasterio.open(ref) as src:
            ref_rs = src.read()
        
        with rasterio.open(banda) as src:

            rs = src.read()
            rs = rs*slope+intercept

            nd = (ref_rs == -9999)
            min_msk =  (rs < 0)             
            max_msk = (rs>=1)

            rs[min_msk] = 0
            rs[max_msk] = 1

            #rs = np.around(rs)
            rs[nd] = -9999

            profile = src.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(outFile, 'w', **profile) as dst:
                dst.write(rs.astype(rasterio.float32))


    def run(self):
        """
        Execute the complete Landsat scene processing workflow.
    
        This method orchestrates the full processing pipeline for a USGS Landsat 
        Collection 2 Level-2 scene. It assumes all required input files (bands, metadata, 
        masks, etc.) are present and correctly structured in the scene directory.
    
        The steps performed are:
    
        1. Generate a hillshade image using DTM and solar angles.
        2. Compute cloud coverage within Doñana National Park.
        3. Compute cloud coverage within Doñana Biosphere Reserve.
        4. Delete temporary cloud mask files.
        5. Apply gap-filling (only for Landsat 7 scenes post-SLC failure).
        6. Reproject and clip spectral bands to a standard extent.
        7. Apply surface reflectance and temperature coefficients.
        8. Normalize all bands using pseudo-invariant features (PIFs) and a reference image.
    
        Outputs are stored in the appropriate subdirectories:
        `pro` (products), `geo` (georeferenced), `rad` (radiometrically corrected),
        and `nor` (normalized).
    
        The method also updates MongoDB with relevant metadata and processing results.
    
        Prints
        ------
        Completion message and total execution time.
    
        Raises
        ------
        RuntimeError
            If any critical step in the pipeline fails.
        """
        
        t0 = time.time()
        self.get_hillshade()
        self.get_cloud_pn()
        self.get_cloud_rbios()  # Added for Biosphere Reserve cloud coverage
        self.remove_masks()
    
        # Apply gapfill if necessary
        self.apply_gapfill()
        
        self.projwin()
        self.coef_sr_st()
        self.normalize()
        print('Escena finalizada en', abs(t0-time.time()), 'segundos')
