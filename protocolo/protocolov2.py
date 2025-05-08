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
    Class for handling USGS Landsat Collection 2 Level-2 Surface Reflectance products.

    This class provides methods to preprocess Landsat images, including cloud masking,
    reflectance and temperature calibration, spatial projection, and normalization using
    pseudo-invariant features.

    Attributes:
        ruta_escena (str): Path to the original scene folder.
        escena (str): Scene folder name.
        base (str): Root directory containing subfolders (ori, rad, geo, nor, pro).
        ...
        last_name (str): Unique scene ID used for naming outputs and MongoDB keys.
        newesc (dict): Document to be inserted in the MongoDB collection.
    """
    
    def __init__(self, ruta_escena, inicializar=True):

        """
        Initialize a Landsat object from a given scene path.

        It parses the metadata, prepares output folders, downloads the quicklook,
        and inserts a new document into the MongoDB collection.

        Args:
            ruta_escena (str): Path to the Landsat scene directory.
            inicializar (bool): If False, skip the full initialization (useful for documentation or static inspection).
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
        Generate a hillshade raster from a fixed DTM and solar metadata.

        This method uses the solar azimuth and elevation from the scene's MTL file
        to generate a hillshade image based on a pre-defined DTM file (specific for path/row 202/034).
        The output is stored in the `nor` folder of the scene.

        Raises:
            RuntimeError: If the `gdaldem hillshade` command fails to execute properly.
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

        This method crops the QA_PIXEL cloud mask using a shapefile of the park boundaries 
        and computes the percentage of valid pixels (clear land or clear water) over 
        the total area of the park (predefined in square meters). 
        The result is stored in MongoDB under the `cloud_PN` field.

        Raises:
            RuntimeError: If the `gdalwarp` command fails.
            Exception: If there is an issue updating MongoDB.
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


    def remove_masks(self):

        """
        Delete temporary cloud mask files generated during processing.

        This method removes all files inside the `masks/` folder within the Landsat scene
        directory and then deletes the folder itself. It is typically called after
        cloud coverage has been analyzed and masks are no longer needed.

        Raises:
            OSError: If any file or the `masks/` folder cannot be deleted.
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

        This method checks if the scene comes from Landsat 7 and was acquired after
        the Scan Line Corrector (SLC) failure (June 2003). If so, it fills in the 
        missing data (gaps) using GDAL's `FillNodata` algorithm for each valid band.

        Only applies to reflectance bands: blue, green, red, nir, swir1, swir2.

        Raises:
            RuntimeError: If a band cannot be opened or processed.
        """
         
        # Verificar si es Landsat 7 y la fecha de adquisición es posterior a junio de 2003
        if self.sat == "L7" and datetime.strptime(self.escena_date, "%Y%m%d") > datetime(2003, 6, 1):
            print("Aplicando gapfill a las bandas de Landsat 7 posteriores a junio de 2003.")
    
            for band_path in [self.blue, self.green, self.red, self.nir, self.swir1, self.swir2]:
                if band_path and os.path.exists(band_path):  # Asegurar que la banda existe
                    print(f"Aplicando gapfill a la banda {band_path}")
    
                    # Usar GDAL para llenar los valores NoData en el archivo original
                    src_ds = gdal.Open(band_path, gdalconst.GA_Update)
                    if src_ds is not None:
                        gdal.FillNodata(src_ds.GetRasterBand(1), maskBand=None, maxSearchDist=10, smoothingIterations=1)
                        src_ds = None  # Liberar el dataset después de modificar
                    else:
                        print(f"No se pudo abrir la banda {band_path} para gapfill.")
    
            print("Gapfill aplicado exitosamente a las bandas de Landsat 7.")


    def projwin(self):

        """
        Apply projection, resolution, and geographic extent to all valid bands.

        This method uses `gdalwarp` to reproject the input bands to a common
        spatial reference, apply a standard 30m resolution, and clip them to a
        predefined geographic extent using a WRS-2 shapefile. It handles
        differences between OLI (Landsat 8/9) and TM/ETM+ (Landsat 4/5/7)
        band naming conventions.

        The resulting files are stored in the `geo` directory.

        Raises:
            RuntimeError: If a band cannot be processed.
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

        This method adjusts the radiometrically corrected bands using standard coefficients
        for surface reflectance (SR) and land surface temperature (LST) to make the pixel
        values physically meaningful.

        - Reflectance bands (e.g., blue, green, red, NIR, SWIR1, SWIR2) are scaled to [0, 1].
        - The LST band is converted from digital numbers to degrees Celsius.
        - The fmask band is copied directly without modification.

        Output bands are stored in the `rad` (radiometrically processed) and `pro` (products) folders.
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

        This method iteratively applies normalization to all spectral bands using a reference image
        and pre-defined masks (e.g. Equilibrada / NoEquilibrada). It attempts up to six iterations per band,
        adjusting the residual threshold to improve fit.

        For each band:
        - It calls `nor1()` to compute regression parameters.
        - If the parameters meet quality criteria (R² > 0.85 and ≥10 pixels per invariant area),
        it proceeds to apply the normalization with `nor2l8()`.
        - The normalization parameters are saved in a `.txt` file and inserted into MongoDB.

        Output:
            - Normalized images in `nor_escena` with `_grn2_` suffix
            - Diagnostic plots and logs
            - A `coeficientes.txt` file with regression stats per band
            - MongoDB update with normalization metadata
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

            try:

                db.update_one({'_id': self.last_name}, {'$set':{'Info.Pasos.nor': 
                        {'Normalize': 'True', 'Nor-Values': self.parametrosnor, 'Fecha': datetime.now()}}})

            except Exception as e:
                print("Unexpected error:", type(e), e)
        
        
        
    def nor1(self, banda, mascara, coef = 1):
        
        """
        Perform linear normalization on a single band using pseudo-invariant areas.

        This method compares the target band with its homologous band from a reference
        image (from August 2022), only using pixels that are cloud-free and fall within
        specific regions of interest defined in the mask (e.g., urban, forest, water).

        It applies a first linear regression, calculates residuals, and filters out outliers
        based on the standard deviation multiplied by a given coefficient (`coef`).
        A second regression is then performed on the filtered data.

        If the regression meets quality criteria (R > 0.85 and ≥10 valid pixels per region),
        the slope and intercept are stored and the normalized image is saved via `nor2l8`.

        Args:
            banda (str): Path to the input image band to be normalized.
            mascara (str): Path to the PIFs mask (e.g., Equilibrada.tif or NoEquilibrada.tif).
            coef (int, optional): Multiplier for the residual standard deviation used
                                to exclude outliers in the second regression (default: 1).

        Results:
            - Stores regression coefficients and pixel counts per class in `self.parametrosnor`
            - Calls `nor2l8()` to generate normalized raster if criteria are met
            - Generates and saves diagnostic plots comparing both regressions
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

        This method uses the slope and intercept from a prior linear regression
        to normalize the input band. It replaces out-of-range values and preserves NoData values.

        The output raster is saved with `_grn2_` in the filename to indicate it is a
        normalized version of the original reflectance-corrected image.

        Args:
            banda (str): Path to the input raster file to be normalized.
            slope (float): Regression slope used for normalization.
            intercept (float): Regression intercept used for normalization.

        Effects:
            - Values < 0 are clipped to 0
            - Values ≥ 1 are clipped to 1
            - NoData values (-9999) are preserved
            - The result is saved to the corresponding `nor_escena` folder
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

        This method orchestrates the full processing pipeline for a Landsat Level-2 scene,
        from auxiliary product generation to final normalization. It assumes that the
        required files (bands, metadata, masks) are present and correctly structured.

        Steps:
        1. Generate a hillshade image using DTM and solar angles.
        2. Compute cloud coverage within the boundaries of the National Park.
        3. Remove temporary masks created during cloud analysis.
        4. Apply gap-filling (only if the scene is from Landsat 7 after June 2003).
        5. Reproject and clip bands to the region of interest.
        6. Apply surface reflectance and temperature coefficients.
        7. Perform normalization based on invariant areas and a reference image.

        The process updates MongoDB with key metadata and creates all intermediate and final products
        inside the corresponding `pro`, `geo`, `rad` and `nor` directories.

        Prints a completion message and total execution time.
        """
        
        t0 = time.time()
        self.get_hillshade()
        self.get_cloud_pn()
        self.remove_masks()

        # Aplicar gapfill si es necesario
        self.apply_gapfill()
        
        self.projwin()
        self.coef_sr_st()
        self.normalize()
        print('Escena finalizada en', abs(t0-time.time()), 'segundos')