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
    
    
    """Clase para trabajar con las Landsat de la nueva Collection 2 Level 2 del USGS"""
    
    def __init__(self, ruta_escena):
        
        """Inicializa un objeto Landsat basado en el path de la escena.

        Args:
            ruta_escena (str): Ruta al directorio de la escena Landsat.
        """
        
        # Definimos los paths de entrada
        self.ruta_escena = ruta_escena
        self.escena = os.path.split(self.ruta_escena)[1]
        self.ori = os.path.split(self.ruta_escena)[0]
        self.base = os.path.split(self.ori)[0]
        
        self.data = os.path.join(self.base, 'data')
        self.geo = os.path.join(self.base, 'geo')
        self.rad = os.path.join(self.base, 'rad')
        self.nor = os.path.join(self.base, 'nor')
        self.pro = os.path.join(self.base, 'pro')
        
        # Definimos un grupo de variables de la escena
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

        # Mascara de nuebes. Hay que hacerlo así porque sabe dios por qué ^!·/&"! los valores no son los mismos en OLI que en ETM+ y TM
        if self.sensor == 'OLI':
            self.cloud_mask_values = [21824, 21952]
        else:
            self.cloud_mask_values = [5440, 5504]

        #Definimos nombre last
        self.last_name = (self.escena_date + self.sat + self.sensor + self.path + '_' + self.row[1:]).lower()

        # Definimos paths de salida
        self.pro_escena = os.path.join(self.pro, self.last_name)
        os.makedirs(self.pro_escena, exist_ok=True)

        self.geo_escena = os.path.join(self.geo, self.last_name)
        os.makedirs(self.geo_escena, exist_ok=True)

        self.rad_escena = os.path.join(self.rad, self.last_name)
        os.makedirs(self.rad_escena, exist_ok=True)

        self.nor_escena = os.path.join(self.nor, self.last_name)
        os.makedirs(self.nor_escena, exist_ok=True)

        self.pro_escena = os.path.join(self.pro, self.last_name)
        os.makedirs(self.pro_escena, exist_ok=True)

        # Definimos máscaras a utilizar
        self.equilibrado = os.path.join(self.data, 'Equilibrada.tif')
        self.noequilibrado = os.path.join(self.data, 'NoEquilibrada.tif')
        self.parametrosnor = {}
        self.iter = 1

        #CReamos un diccionario a partir del MTL
        self.mtl = {}
        for i in os.listdir(self.ruta_escena):
            if i.endswith('MTL.txt'):
                mtl = os.path.join(self.ruta_escena, i)
                
                f = open(mtl, 'r')
                
                for line in f.readlines():
                    if "=" in line:
                        l = line.split("=")
                        self.mtl[l[0].strip()] = l[1].strip()
        
        #Creamos las variables con la franja del espectro de cada banda
        if self.sat in ['L8', 'L9']:
            
            for i in os.listdir(self.ruta_escena):
                if i.endswith('.TIF'):
                    banda = os.path.splitext(i)[0].split('_')[-1]
            
                    if banda == 'B2':
                        self.blue = os.path.join(self.ruta_escena, i)
                    elif banda == 'B3':
                        self.green = os.path.join(self.ruta_escena, i)
                    elif banda == 'B4':
                        self.red = os.path.join(self.ruta_escena, i)
                    elif banda == 'B5':
                        self.nir = os.path.join(self.ruta_escena, i)
                    elif banda == 'B6':
                        self.swir1 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B7':
                        self.swir2 = os.path.join(self.ruta_escena, i)
                    elif i.endswith('QA_PIXEL.TIF'):
                        self.qa = os.path.join(self.ruta_escena, i)
                        print(self.qa)
                    else:
                        continue
                
        elif self.sat in ['L7', 'L5']:
            
            for i in os.listdir(self.ruta_escena):
                if i.endswith('.TIF'):
                    banda = os.path.splitext(i)[0].split('_')[-1]
            
                    if banda == 'B1':
                        self.blue = os.path.join(self.ruta_escena, i)
                    elif banda == 'B2':
                        self.green = os.path.join(self.ruta_escena, i)
                    elif banda == 'B3':
                        self.red = os.path.join(self.ruta_escena, i)
                    elif banda == 'B4':
                        self.nir = os.path.join(self.ruta_escena, i)
                    elif banda == 'B5':
                        self.swir1 = os.path.join(self.ruta_escena, i)
                    elif banda == 'B7':
                        self.swir2 = os.path.join(self.ruta_escena, i)
                    elif i.endswith('QA_PIXEL.TIF'):
                        self.qa = os.path.join(self.ruta_escena, i)
                        print(self.qa)
                    else:
                        continue
        
        else:
            print('No encuentro ninguna escena Landsat')
            
        
        # Descargamos el quicklook de la escena 
        url_base = 'https://landsatlook.usgs.gov/gen-browse?size=rrb&type=refl&product_id={}'.format(self.mtl['LANDSAT_PRODUCT_ID'].strip('""'))
        self.qk_name = os.path.join(self.ruta_escena, self.escena + '_Quicklook.jpeg')
        
        if not os.path.exists(self.qk_name):
            qk = open(self.qk_name, 'wb')
            qk_open = urlopen(url_base)
            urlimg = qk_open.read()
            qk.write(urlimg)
            qk.close()

            print('QuicKlook descargado')
            
        else:
            print('El Quicklook ya estaba previamente descargado')
        
        #print(url_base)
        print('Landsat iniciada con éxito') 

        self.pn_cover = None
        #Creamos el json para instarlo en la base de datos MongoDB
        self.newesc = {'_id': self.last_name, 
                       'usgs_id': self.mtl['LANDSAT_SCENE_ID'][1:-1], 
                       'tier_id': self.mtl['LANDSAT_PRODUCT_ID'][1:-1],
                           'lpgs': self.mtl['PROCESSING_SOFTWARE_VERSION'][1:-1],
                       'category': self.mtl['COLLECTION_CATEGORY'][1:-1],
                       'Clouds': {'cloud_scene': float(self.mtl['CLOUD_COVER']),
                                 'land cloud cover': float(self.mtl['CLOUD_COVER_LAND'])},
                           'Info': {'Tecnico': 'LAST-EBD Auto', 
                                    'Iniciada': datetime.now(), 'Pasos': {'rad': '', 'nor': ''}}}

        try:

            db.insert_one(self.newesc)

        except Exception as e:

            db.update_one({'_id': self.last_name}, {'$set':{'Info.Iniciada': datetime.now()}}, upsert=True)
            #print("Unexpected error:", type(e)) #se Podria dar un error por clave unica, por eso en
            #ese caso, lo que hacemos es actualizar la fecha en la que tratamos la imagen

        print('Landsat instanciada y subida a la base de datos')


    def get_hillshade(self):

        """Genera un hillshade de la escena usando parámetros solares (azimuth y elevación solar) 
        de los metadatos del MTL.txt.

        La idea es simular las sombras reales y quitarlas de los píxeles validos en la máscara de agua.
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

        """Calcula la cobertura de nubes dentro del Parque Nacional.

        Recorta la máscara de nubes (fmask) con un shapefile del Parque Nacional 
        para obtener la cobertura nubosa dentro de los límites del parque. 
        Actualiza la base de datos con los porcentajes de nubes (sobre tierra y escena completa MTL.txt) y sobre Doñana.
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

        """Elimina las máscaras temporales generadas durante el procesamiento de nubes.

        Borra los archivos de máscaras creados en el directorio 'masks' de la escena.
        """

        path_masks = os.path.join(self.ruta_escena, 'masks')
        for i in os.listdir(path_masks):

            name = os.path.join(path_masks, i)
            os.chmod(name, stat.S_IWRITE)
            os.remove(name)

        shutil.rmtree(path_masks)


    def projwin(self):

        """Aplica proyección y extensión geográfica a las bandas de la escena.
        """        
        
        olibands = {'B1': 'cblue_b1', 'B2': 'blue_b2', 'B3': 'green_b3', 'B4': 'red_b4', 'B5': 'nir_b5', 'B6': 'swir1_b6',
                   'B7': 'swir2_b7', 'PIXEL': 'fmask', 'B10': 'lst'}
        
        etmbands = {'B1': 'blue_b1', 'B2': 'green_b2', 'B3': 'red_b3', 'B4': 'nir_b4', 'B5': 'swir1_b5',
                   'B7': 'swir2_b7', 'PIXEL': 'fmask', 'B6': 'lst'}
        
        
        #geo = '/media/diego/31F8C0B3792FC3B6/EBD/Protocolo_v2_2024/geo'
        #path_rad = os.path.join(self.geo, self.escena)
        #os.makedirs(path_rad, exist_ok=True)
        
        for i in os.listdir(self.ruta_escena):
            
            if self.sat in ['L8', 'L9']:
                if i.endswith('.TIF'):

                    banda = i.split('_')[-1][:-4]

                    if banda in olibands.keys():
                        ins = os.path.join(self.ruta_escena, i)

                        name = self.escena_date + self.sat + self.sensor + self.path + '_' + self.row[1:] + '_g2_' + olibands[banda] + '.tif'
                        out = os.path.join(self.geo_escena, name.lower())

                        cmd = "gdalwarp -ot Int32 -srcnodata 0 -dstnodata '-9999' -tr 30 30 -te 633570 4053510 851160 4249530 -tap -cutline /media/diego/Datos4/EBD/Protocolo_v2_2024/data/wrs_202034.shp {} {}".format(ins, out)
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

                        cmd = "gdalwarp -ot Int32 -srcnodata 0 -dstnodata '-9999' -tr 30 30 -te 633570 4053510 851160 4249530 -tap -cutline /media/diego/Datos4/EBD/Protocolo_v2_2024/data/wrs_202034.shp {} {}".format(ins, out)
                        print(cmd)
                        os.system(cmd)

                    else:
                        continue
                
                
            else:
                print('Lo siento, pero no encuentro el satélite')
                
                
    def coef_sr_st(self):

        """Aplica los coeficientes de reflectancia superficial y temperatura a las bandas.

        Esta función toma las bandas proyectadas y ajusta los valores para que sean
        interpretables en términos de reflectancia superficial y temperatura.
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
        
        """Controla el proceso de normalización de las bandas.

        Si no se obtienen los coeficientes requeridos (R>0.85 y N_Pixeles >= 10),
        se itera a través de diferentes niveles hasta que se logran los valores 
        requeridos o se alcanza el último paso.
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
        
        """Busca los coeficientes necesarios para la normalización de una banda.

        Args:
            banda (str): Ruta a la banda que se va a normalizar.
            mascara (str): Ruta a la máscara utilizada para la normalización.
            coef (int, optional): Coeficiente utilizado para ajustar el valor de desviación típica. 
                Por defecto es 1.
        
        Busca los coeficientes de regresión y realiza la primera normalización.
        Si los coeficientes cumplen los requisitos, se guarda la normalización.
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
    
        """Aplica la ecuación de regresión a la banda normalizada.

        Args:
            banda (str): Ruta de la banda que se va a ajustar.
            slope (float): Pendiente de la regresión calculada.
            intercept (float): Intercepto de la regresión calculada.

        Genera la banda normalizada aplicando la ecuación de la recta obtenida 
        en la regresión.
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

        """Ejecuta todo el flujo de procesamiento para la escena Landsat.

        Realiza la generación del hillshade, calcula la cobertura nubosa, elimina
        máscaras temporales, proyecta las bandas, aplica coeficientes y realiza
        la normalización completa.
        """
        
        t0 = time.time()
        self.get_hillshade()
        self.get_cloud_pn()
        self.remove_masks()
        self.projwin()
        self.coef_sr_st()
        self.normalize()
        print('Escena finalizada en', abs(t0-time.time()), 'segundos')