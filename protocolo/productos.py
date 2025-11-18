
import os
import shutil
import re
import time
import subprocess
import glob
import pandas
import rasterio
import sys
import urllib
import fiona
import sqlite3
import math
import pymongo
import json
import psycopg2
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from rasterio.features import geometry_mask
from rasterio.mask import mask
from osgeo import gdal, gdalconst
from datetime import datetime, date
from rasterstats import zonal_stats

# Añadimos la ruta con el código a nuestro pythonpath para poder importar la clase Landsat
sys.path.append('/root/git/ProtocoloV2/protocolo')
from config import SSH_USER, SSH_KEY_PATH, SERVER_HOSTS

#from utils import process_composition_rgb, process_flood_mask, generar_metadatos_flood, subir_xml_y_tif_a_geonetwork
from utils import * 
from coast import Coast

from pymongo import MongoClient
client = MongoClient()

database = client.Satelites
db = database.Landsat

class Product(object):
    
    
    '''Esta clase genera los productos de inundacion, turbidez del agua y ndvi de las escenas normalizadas'''
    
        
    def __init__(self, ruta_nor):
        
        """Inicializa un objeto Product con la ruta de la escena normalizada.

        Args:
            ruta_nor (str): Ruta al directorio de la escena normalizada.
        """

        self.escena = os.path.split(ruta_nor)[1]
        self.raiz = os.path.split(os.path.split(ruta_nor)[0])[0]
        print(self.raiz)
        self.ori = os.path.join(self.raiz, 'ori')
        self.pro = os.path.join(self.raiz, 'pro')
        self.nor = ruta_nor
        self.productos = os.path.join(self.raiz, 'pro')
        self.data = os.path.join(self.raiz, 'data')
        self.water_masks = os.path.join(self.data, 'water_mask_pv2')
        self.temp = os.path.join(self.raiz, 'temp')
        os.makedirs(self.temp, exist_ok=True)
        
        self.ori_escena = os.path.join(self.raiz, os.path.join('ori', self.escena))
        self.nor_escena = ruta_nor
        self.pro_escena = os.path.join(self.pro, self.escena)
        os.makedirs(self.pro_escena, exist_ok=True)

        self.ndvi_escena = None
        self.ndwi_escena = None
        self.mndwi_escena = None
        self.flood_escena = None
        self.turbidity_escena = None
        self.depth_escena = None

        # Lista con los productos obtenidos para el envío de mails
        self.productos_generados = []

        # Shape con recintos
        self.recintos = os.path.join(self.data, 'Recintos_Marisma.shp')
        self.lagunas = os.path.join(self.data, 'lagunas_carola_32629.shp')
        self.lagunas_labordette = os.path.join(self.data, 'lagunas_labordette.shp')
        self.resultados_lagunas = {}
        self.resultados_lagunas_labordette = {}
        # Salida con la superficie inundada por recinto
        #self.superficie_inundada = os.path.join(self.pro_escena, 'superficie_inundada.csv')
        
        # Salida de los jpgs para el Observatrio del Cambio Global
        # PRO
        self.rbios = os.path.join(self.data, 'RBIOS.shp')
        self.out_OCG = "/mnt/productos_inundaciones2"
        # VPS
        self.out_OCG_VPS = "/mnt/productos_inundaciones2_VPS"
        
        # Tenemos que definir el sensor para coger los valores adecuados de Fmask
        if 'oli' in self.escena:
            self.sensor = 'OLI'
        elif 'etm' in self.escena:
            self.sensor = 'ETM+'
        elif 'l5tm' in self.escena:
            self.sensor = 'TM'
        else:
            print('what f* kind of satellite are you trying to work with?')

        # Mascara de nuebes. Hay que hacerlo así porque sabe dios por qué ^!·/&"! los valores no son los mismos en OLI que en ETM+ y TM
        if self.sensor == 'OLI':
            self.cloud_mask_values = [21824, 21952]
        else:
            self.cloud_mask_values = [1, 5440, 5504] # 1 es el valor de los gaps

        for i in os.listdir(self.nor_escena):
            if re.search('tif$', i):
                # Verificamos si el archivo es 'fmask' o 'hillshade'
                if 'fmask' in i:
                    self.fmask = os.path.join(self.nor, i)
                elif 'hillshade' in i:
                    self.hillshade = os.path.join(self.nor, i)
                else:
                    # Dividimos el nombre del archivo en partes
                    partes = i.split('_')
                    # Comprobamos si tiene al menos 3 partes (para evitar el error de índice)
                    if len(partes) >= 3:
                        banda = partes[-2]
                        if banda == 'blue':
                            self.blue = os.path.join(self.nor, i)
                        elif banda == 'green':
                            self.green = os.path.join(self.nor, i)
                        elif banda == 'red':
                            self.red = os.path.join(self.nor, i)
                        elif banda == 'nir':
                            self.nir = os.path.join(self.nor, i)
                        elif banda == 'swir1':
                            self.swir1 = os.path.join(self.nor, i)
                        elif banda == 'swir2':
                            self.swir2 = os.path.join(self.nor, i)

        # Debugging print statements
        print('SWIR1:', self.swir1)
        print('FMASK:', self.fmask)
        print('HILLSHADE:', self.hillshade)

       
        try:
            # Verificar si ya existen productos asociados a la escena
            documento = db.find_one({'_id': self.escena}, {'Productos': 1})
            
            if documento and 'Productos' in documento:
                print(f"Productos existentes para la escena {self.escena}: {documento['Productos']}")
            else:
                # Si no hay productos existentes, inicializar la lista de productos
                db.update_one({'_id': self.escena}, {'$set': {'Productos': []}}, upsert=True)
                print(f"No se encontraron productos existentes para la escena {self.escena}. Inicializando...")

        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('escena importada para productos correctamente')

        
    def generate_composition_rgb(self):
        
        """Genera la composición RGB en pro_escena (sobrescribe si existe)."""
        output_path = os.path.join(self.pro_escena, f"{self.escena}_rgb.png")
        process_composition_rgb(
            self.swir1,
            self.nir,
            self.blue,
            self.rbios,
            output_path
        )

    def generate_flood_mask(self):
        
        """Genera la imagen de la máscara de inundación en pro_escena (sobrescribe si existe)."""
        output_path = os.path.join(self.pro_escena, f"{self.escena}_flood.png")
        process_flood_mask(
            self.flood_escena,
            self.rbios,
            output_path
        )
            
        
    def ndvi(self):

        """Calcula el NDVI (Índice de Vegetación de Diferencia Normalizada) para la escena.

        El NDVI se guarda como un archivo GeoTIFF y se actualiza en la base de datos.
        """

        self.ndvi_escena = os.path.join(self.pro_escena, self.escena + '_ndvi_.tif')
        print(self.ndvi_escena)
        
        with rasterio.open(self.nir) as nir:
            NIR = nir.read()
            
        with rasterio.open(self.red) as red:
            RED = red.read()

        num = NIR.astype(float)-RED.astype(float)
        den = NIR+RED
        ndvi = np.true_divide(num, den)
        ndvi[NIR == -9999] = -9999
                
        profile = nir.meta
        profile.update(nodata=-9999)
        profile.update(dtype=rasterio.float32)

        with rasterio.open(self.ndvi_escena, 'w', **profile) as dst:
            dst.write(ndvi.astype(rasterio.float32))
                    
        try:
        
            db.update_one({'_id':self.escena}, {'$addToSet':{'Productos': 'NDVI'}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print(f'Ndvi guardado en: {self.ndvi_escena}')


    def ndwi(self):

        """Calcula el NDWI (Índice de Agua de Diferencia Normalizada) para la escena.

        El NDWI se guarda como un archivo GeoTIFF y se actualiza en la base de datos.
        """

        self.ndwi_escena = os.path.join(self.pro_escena, self.escena + '_ndwi.tif')
        #print outfile
        
        with rasterio.open(self.nir) as nir:
            NIR = nir.read()
            
        with rasterio.open(self.green) as green:
            GREEN = green.read()
            
        num = GREEN-NIR
        den = GREEN+NIR
        ndwi = num/den

        # Aplicamos NoData (-9999) al marco exterior
        ndwi[NIR == -9999] = -9999
            
        profile = nir.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(self.ndwi_escena, 'w', **profile) as dst:
            dst.write(ndwi.astype(rasterio.float32))

        try:
        
            db.update_one({'_id':self.escena}, {'$addToSet':{'Productos': 'NDWI'}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)

        print(f'Ndwi guardado en: {self.ndwi_escena}')


    def mndwi(self):

        """Calcula el MNDWI (Índice Modificado de Agua de Diferencia Normalizada) para la escena.

        El MNDWI se guarda como un archivo GeoTIFF y se actualiza en la base de datos.
        """

        self.mndwi_escena = os.path.join(self.pro_escena, self.escena + '_mndwi.tif')
        #print outfile
        
        with rasterio.open(self.swir1) as swir1:
            SWIR1 = swir1.read()

        with rasterio.open(self.green) as green:
            GREEN = green.read()
        
        num = GREEN-SWIR1
        den = GREEN+SWIR1
        mndwi = num/den

        # Aplicamos NoData (-9999) al marco exterior
        mndwi[SWIR1 == -9999] = -9999
        
        profile = swir1.meta
        profile.update(dtype=rasterio.float32)

        with rasterio.open(self.mndwi_escena, 'w', **profile) as dst:
            dst.write(mndwi.astype(rasterio.float32))

        try:
        
            db.update_one({'_id':self.escena}, {'$addToSet':{'Productos': 'MNDWI'}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)

        print(f'Mndwi guardado en: {self.mndwi_escena}')


    def flood(self):
        
        """Genera la máscara de inundación utilizando diversos criterios (e.g., NDWI, MNDWI, Slope).

        La máscara de inundación se guarda como un archivo GeoTIFF y se actualiza en la base de datos.
        Aplica reglas para determinar qué áreas son consideradas inundadas.
        """
    
        self.flood_escena = os.path.join(self.pro_escena, self.escena + '_flood.tif')
        # print(self.flood_escena)
    
        # Abrimos los rasters
        dtm_path = os.path.join(self.water_masks, 'dtm_202_34.tif')
        slope_path = os.path.join(self.water_masks, 'slope_202_34.tif')
        fmask_path = os.path.join(self.water_masks, 'fmask_202_34.tif')
        ndwi_path = os.path.join(self.water_masks, 'ndwi_p99_202_34.tif')
        mndwi_path = os.path.join(self.water_masks, 'mndwi_p99_202_34.tif')
        cobveg_path = os.path.join(self.water_masks, 'cob_veg_202_34.tif')
        ndvi_p10_path = os.path.join(self.water_masks, 'ndvi_p10_202_34.tif')
        ndvi_mean_path = os.path.join(self.water_masks, 'ndvi_mean_202_34.tif')
    
        with rasterio.open(dtm_path) as dtm, \
             rasterio.open(slope_path) as slope, \
             rasterio.open(fmask_path) as fmaskw, \
             rasterio.open(ndwi_path) as ndwi, \
             rasterio.open(mndwi_path) as mndwi, \
             rasterio.open(cobveg_path) as cobveg, \
             rasterio.open(ndvi_p10_path) as ndvi_p10, \
             rasterio.open(ndvi_mean_path) as ndvi_mean, \
             rasterio.open(self.ndvi_escena) as ndvi_scene, \
             rasterio.open(self.ndwi_escena) as ndwi_scene, \
             rasterio.open(self.mndwi_escena) as mndwi_scene, \
             rasterio.open(self.fmask) as fmask_scene, \
             rasterio.open(self.hillshade) as hillsh, \
             rasterio.open(self.swir1) as swir1:
    
            DTM = dtm.read(1)
            SLOPE = slope.read(1)
            FMASKW = fmaskw.read(1)
            NDWI = ndwi.read(1)
            MNDWI = mndwi.read(1)
            COBVEG = cobveg.read(1)
            NDVIP10 = ndvi_p10.read(1)
            NDVIMEAN = ndvi_mean.read(1)
            NDVISCENE = ndvi_scene.read(1)
            NDWISCENE = ndwi_scene.read(1)
            MNDWISCENE = mndwi_scene.read(1)
            FMASK_SCENE = fmask_scene.read(1)
            HILLSHADE = hillsh.read(1)
            SWIR1 = swir1.read(1)
    
            # Generamos la máscara de agua
            water_mask = (SWIR1 < 0.12)
    
            # Aplicamos la condición de pendiente
            slope_condition = (SLOPE > 8) & ~((NDWI > 0.25) | (MNDWI > 0.8))
            water_mask[slope_condition] = 0
    
            # Excluimos valores nodata del cálculo del percentil
            valid_hillshade = HILLSHADE[HILLSHADE != -9999]
            shadow_threshold = np.percentile(valid_hillshade, 30)
    
            # Aplicamos la condición de sombras (Hillshade)
            shadow_condition = HILLSHADE < shadow_threshold
            water_mask[shadow_condition] = 0
    
            # Aplicamos la condición de NDVI
            ndvi_condition = (NDVIP10 > 0.3) & (NDVIMEAN > 0.5)
            water_mask[ndvi_condition] = 0
    
            # Aplicamos la condición de CobVeg
            cobveg_condition = (COBVEG > 75)
            water_mask[cobveg_condition] = 0
    
            # Aplicamos la condición de NDVI de la escena
            ndvi_scene_condition = ((NDVISCENE > 0.60) & (DTM > 2.5))
            water_mask[ndvi_scene_condition] = 0
    
            # Aplicamos la condición para nubes y sombras de nubes usando np.where
            water_mask = np.where(~np.isin(FMASK_SCENE, self.cloud_mask_values), 2, water_mask)
    
            # Reclasificamos los 2 índices y Fmask
            mndwi_r = np.where(MNDWISCENE > 0, 1, 0)
            ndwi_r = np.where(NDWISCENE > 0, 1, 0)
            fmask_r = np.where(FMASK_SCENE == self.cloud_mask_values[1], 1, 0)
    
            # Suma de los 3
            water_ix_sum = mndwi_r + ndwi_r + fmask_r
    
            # Si dos de ellos dan valor agua, el pixel pasa a ser agua
            water_masks_condition = (water_ix_sum >= 2)
            water_mask[water_masks_condition] = 1
    
            # Aplicamos NoData (-9999) al marco exterior
            water_mask[SWIR1 == -9999] = -9999
    
            # Guardamos el resultado final
            with rasterio.open(
                self.flood_escena,
                'w',
                driver='GTiff',
                height=water_mask.shape[0],
                width=water_mask.shape[1],
                count=1,
                dtype='int16',
                crs=dtm.crs,
                transform=dtm.transform,
                compress='lzw',
                nodata=-9999
            ) as dst:
                dst.write(water_mask, 1)
    
        try:
            db.update_one({'_id': self.escena}, {'$addToSet': {'Productos': 'Flood'}}, upsert=True)
        except Exception as e:
            print("Unexpected error:", type(e), e)
    
        print(f'Máscara de agua guardada en: {self.flood_escena}')

    
        
    def turbidity(self):

        """Calcula la turbidez del agua en la escena usando bandas espectrales.

        El cálculo de la turbidez se guarda como un archivo GeoTIFF y se actualiza en la base de datos.
        Usa diferentes modelos dependiendo del tipo de cuerpo de agua (río o marisma).
        """
        
        waterMask = os.path.join(self.water_masks, 'water_mask_turb.tif')
        self.turbidity_escena = os.path.join(self.pro_escena, self.escena + '_turbidity.tif')
        #print(self.turbidity_escena)
        
        with rasterio.open(self.flood_escena) as flood:
            FLOOD = flood.read()
        
        with rasterio.open(waterMask) as wmask:
            WMASK = wmask.read()
            
        with rasterio.open(self.blue) as blue:
            BLUE = blue.read()
            BLUE = np.where(BLUE == 0, 1, BLUE)
            #BLUE = np.true_divide(BLUE, 10000)
                        
        with rasterio.open(self.green) as green:
            GREEN = green.read()
            GREEN = np.where(GREEN == 0, 1, GREEN)
            #GREEN = np.true_divide(GREEN, 10000)
            GREEN_R = np.where((GREEN<0.1), 0.1, GREEN)
            GREEN_RECLASS = np.where((GREEN_R>=0.4), 0.4, GREEN_R)

        with rasterio.open(self.red) as red:
            RED = red.read()
            RED = np.where(RED == 0, 1, RED)
            #RED = np.true_divide(RED, 10000)
            RED_RECLASS = np.where((RED>=0.2), 0.2, RED)
            
        with rasterio.open(self.nir) as nir:
            NIR = nir.read()
            NIR = np.where(NIR == 0, 1, NIR)
            #NIR = np.true_divide(NIR, 10000)
            NIR_RECLASS = np.where((NIR>0.5), 0.5, NIR)
            
        with rasterio.open(self.swir1) as swir1:
            SWIR1 = swir1.read()
            SWIR1 = np.where(SWIR1 == 0, 1, SWIR1)
            #SWIR1 = np.true_divide(SWIR1, 10000)
            SWIR_RECLASS = np.where((SWIR1>=0.09), 0.9, SWIR1)
        
        
        #Turbidez para la el rio
        rio = (-4.3 + (85.22 * GREEN_RECLASS) - (455.9 * np.power(GREEN_RECLASS,2)) \
            + (594.58 * np.power(GREEN_RECLASS,3)) + (32.3 * RED) - (15.36 * NIR_RECLASS)  \
            + (21 * np.power(NIR_RECLASS,2))) - 0.01        
        #RIO = np.power(math.e, rio)
        
        #Turbidez para la marisma        
        marisma = (4.1263574 + (18.8113118 * RED_RECLASS) - (32.2615219 * SWIR_RECLASS) \
        - 0.0114108989999999 * np.true_divide(BLUE, NIR)) - 0.01
        #MARISMA = np.power(math.e, marisma)
        
        
        TURBIDEZ = np.where(((FLOOD == 1) & (WMASK == 1)), marisma, 
                             np.where(((FLOOD == 1) & (WMASK == 2)), rio, -9999))

        TURBIDEZ[SWIR1 == -9999] = -9999
        
        
        profile = swir1.meta
        profile.update(nodata=-9999)
        profile.update(dtype=rasterio.float32)
                             
        with rasterio.open(self.turbidity_escena, 'w', **profile) as dst:
            dst.write(TURBIDEZ.astype(rasterio.float32))        
        
        try:
        
            db.update_one({'_id':self.escena}, {'$addToSet':{'Productos': 'Turbidity'}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print(f'Máscara de turbidez guardada en: {self.turbidity_escena}')


    def depth(self):

        """Calcula la profundidad del agua en las áreas inundadas de la escena.

        La profundidad se calcula utilizando ratios entre bandas espectrales y modelos empíricos.
        El resultado se guarda como un archivo GeoTIFF y se actualiza en la base de datos.
        """
        
        # Abrimos las bandas necesarias para correr el algoritmo
        septb4 = os.path.join(self.water_masks, '20230930l9oli202_34_grn2_nir_b5.tif')
        septwmask = os.path.join(self.water_masks, '20230930l9oli202_34_flood.tif')
        
        self.depth_escena = os.path.join(self.pro_escena, self.escena + '_depth_.tif')
        #print(self.depth_escena)

        with rasterio.open(self.flood_escena) as flood:
            FLOOD = flood.read()
            
        with rasterio.open(septb4) as septb4:
            
            SEPTB4 = septb4.read()
                        
            #En reflectividades
            #SEPTB4_REF = np.true_divide(SEPTB4, 306)
            SEPTB4_REF = np.where(SEPTB4 >= 0.830065359, 0.830065359, SEPTB4)
        
        with rasterio.open(septwmask) as septwater:
            SEPTWMASK = septwater.read()
            
        #Banda 1
        with rasterio.open(self.blue) as blue:
            BLUE = blue.read()
            BLUE = np.where(BLUE >= 0.2, 0.2, BLUE)

            #Blue en reflectividad
            #BLUE_REF = np.true_divide(BLUE, 398)
            
            
        #Banda 2
        with rasterio.open(self.green) as green:
            GREEN = green.read()
            
            #Green en reflectivdiad
            #GREEN_REF = np.true_divide(GREEN, 401) #
            
        
        #Banda 4
        with rasterio.open(self.nir) as nir:
            NIR = nir.read()
            
            #NIR en reflectividad
            #NIR_REF = np.true_divide(NIR, 422)
            
        
        #Banda 5
        with rasterio.open(self.swir1) as swir1:
            SWIR1 = swir1.read()
            
            #SWIR1 en reflecrtividad
            #SWIR1_REF = np.true_divide(SWIR1, 324)
            
        
        #Ratios
        RATIO_GREEN_NIR = np.true_divide(GREEN, NIR)
        RATIO_GREEN_NIR = np.where(RATIO_GREEN_NIR >= 2.5, 2.5, RATIO_GREEN_NIR)
        RATIO_NIR_SEPTNIR = np.true_divide(NIR, SEPTB4)           
        
        #Profundidad para la marisma        
            
        a = 5.293739862 + (-0.038684824 * BLUE) + (0.02826867 * SWIR1) + (-0.007525455 * SEPTB4) + \
            (1.023724916 * RATIO_GREEN_NIR) + (-1.041844944 * RATIO_NIR_SEPTNIR)
        
        a_safe = np.where(a > 50, 50, a)
        
        DEPTH = np.exp(a_safe) - 0.01
        
        #PASAR A NODATA EL AGUA DE SEPTIEMBRE!!!!
        
        #Se podría pasar directamente a SWIR1 <= 53
        DEPTH_ = np.where((FLOOD == 1) & (SEPTWMASK == 0), DEPTH, -9999)

        profile = swir1.meta
        profile.update(nodata=-9999)
        profile.update(dtype=rasterio.float32)
        #profile.update(driver='GTiff')

        with rasterio.open(self.depth_escena, 'w', **profile) as dst:
            dst.write(DEPTH_.astype(rasterio.float32))

        try:
        
            db.update_one({'_id':self.escena}, {'$addToSet':{'Productos': 'Depth'}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print(f'Imagen de profundida guardada en: {self.depth_escena}')


    def get_flood_surface(self):
        
        """
        Calcula la superficie inundada por zonas de marisma y actualiza MongoDB.
    
        Utiliza un shapefile de recintos de marisma y una máscara de inundación para calcular la superficie 
        inundada en hectáreas para cada zona. Los resultados se guardan en un archivo CSV y en la base de datos.
        """
        try:
            # Leer el shapefile de recintos
            gdf = gpd.read_file(self.recintos).to_crs("EPSG:32629")
            gdf["area_total"] = gdf.geometry.area / 10000  # en hectáreas
    
            inundacion_dict = {}
            lista_csv = []
    
            total_inundado = 0
            total_area = 0
    
            with rasterio.open(self.flood_escena) as src:
                for _, row in gdf.iterrows():
                    nombre = row["Nombre"]
                    try:
                        geom = [row["geometry"]]
                        out_image, out_transform = mask(dataset=src, shapes=geom, crop=True)
                        out_image = out_image[0]
                        pixel_area = abs(src.res[0] * src.res[1])
                        flooded_area = np.sum(out_image == 1) * pixel_area / 10000  # ha
                        area_total = row["area_total"]
                        porcentaje = 100 * flooded_area / area_total if area_total else 0
    
                        # Guardar en diccionario para MongoDB
                        inundacion_dict[nombre] = {
                            "area_inundada": round(flooded_area, 2),
                            "porcentaje_inundacion": round(porcentaje, 2),
                            "area_total": round(area_total, 2)
                        }
    
                        # Guardar en lista para CSV
                        lista_csv.append({
                            "_id": self.escena,
                            "recinto": nombre,
                            "area_inundada": round(flooded_area, 2),
                            "porcentaje_inundacion": round(porcentaje, 2),
                            "area_total": round(area_total, 2)
                        })
    
                        total_inundado += flooded_area
                        total_area += area_total
    
                    except Exception as e:
                        print(f"⚠️ Error en recinto {nombre}:", e)
    
            # Añadir la fila total al CSV
            porcentaje_total = 100 * total_inundado / total_area if total_area else 0
    
            lista_csv.append({
                "_id": self.escena,
                "recinto": "Total",
                "area_inundada": round(total_inundado, 2),
                "porcentaje_inundacion": round(porcentaje_total, 2),
                "area_total": round(total_area, 2)
            })
    
            inundacion_dict["Total"] = {
                "area_inundada": round(total_inundado, 2),
                "porcentaje_inundacion": round(porcentaje_total, 2),
                "area_total": round(total_area, 2)
            }
    
            # Guardar CSV
            df = pd.DataFrame(lista_csv)[["_id", "recinto", "area_inundada", "porcentaje_inundacion", "area_total"]]
            csv_path = os.path.join(self.pro_escena, "superficie_inundada.csv")
            df.to_csv(csv_path, index=False)
            print(f"CSV guardado en: {csv_path}")
    
            # Guardar en MongoDB
            db.update_one(
                {"_id": self.escena},
                {
                    "$set": {"Flood_Data.Marismas": inundacion_dict},
                    "$addToSet": {"Productos": "Flood"}
                }
            )
            print("Datos de inundación actualizados en MongoDB correctamente.")
    
        except Exception as e:
            print("⚠️ Error durante el procesamiento:", e)


    # CSV version
    def calcular_inundacion_lagunas(self):
        """
        Calculate flooded surface area for Carola lagoons layer.
        
        This method computes flooding statistics for all lagoon polygons in the 
        Carola lagoons shapefile by intersecting them with the flood mask. Only 
        pixels with value 1 (water) are counted, excluding clouds (value 2) and 
        NoData (value -9999).
        
        Results are stored in MongoDB and exported as CSV files.
        
        The calculation includes:
        - Number of lagoons with water
        - Total flooded surface area (hectares)
        - Percentage of theoretical maximum flooding
        
        Results are saved to:
        - MongoDB: `Flood_Data.Lagunas`
        - CSV: `resumen_lagunas_carola.csv` (summary)
        - CSV: `lagunas_carola.csv` (per-lagoon detail)
        
        Notes
        -----
        The method uses a custom zonal statistics function to count only water 
        pixels (value == 1), avoiding overestimation from cloud pixels (value == 2).
        
        Raises
        ------
        Exception
            If MongoDB update fails or spatial operations encounter errors.
        
        See Also
        --------
        calcular_inundacion_lagunas_principales : Calculate flooding for main Carola lagoons.
        calcular_inundacion_lagunas_labordette : Calculate flooding for Labordette lagoons.
        """
    
        lagunas = gpd.read_file(self.lagunas)
    
        # Load flood mask
        with rasterio.open(self.flood_escena) as src:
            resolution = src.res[0] * src.res[1]
    
        # Calculate theoretical maximum flood area
        lagunas["area_total"] = lagunas.geometry.area / 10000
        area_maxima_teorica = lagunas["area_total"].sum()
    
        # Custom function to count only water pixels (value == 1)
        def count_water_pixels(x):
            return np.sum(x == 1)
    
        # Calculate zonal statistics
        stats = zonal_stats(
            lagunas,
            self.flood_escena,
            add_stats={'water_pixels': count_water_pixels},
            raster_out=False,
            geojson_out=False,
        )
    
        # Add flooded area column
        lagunas["area_inundada"] = [
            (stat.get('water_pixels', 0) or 0) * resolution / 10000 for stat in stats
        ]
    
        # Calculate metrics
        lagunas_con_agua = lagunas[lagunas["area_inundada"] > 0]
        numero_lagunas_con_agua = len(lagunas_con_agua)
        superficie_total_inundada = lagunas_con_agua["area_inundada"].sum()
        porcentaje_inundado = (superficie_total_inundada / area_maxima_teorica) * 100
    
        # Store results in dictionary
        self.resultados_lagunas = {
            "numero_lagunas_con_agua": numero_lagunas_con_agua,
            "superficie_total_inundada": superficie_total_inundada,
            "porcentaje_inundado": porcentaje_inundado,
        }
    
        # Display results
        print(f"Número de lagunas con agua: {numero_lagunas_con_agua}")
        print(f"Superficie total inundada: {superficie_total_inundada:.2f} ha")
        print(f"Porcentaje de inundación respecto al total teórico: {porcentaje_inundado:.2f}%")
    
        # Save to MongoDB
        try:
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Flood_Data.Lagunas": self.resultados_lagunas}},
                upsert=True
            )
            print("Resultados de lagunas guardados en MongoDB correctamente.")
        except Exception as e:
            print(f"Error al guardar en MongoDB: {e}")
    
        # Save results to CSV
        resumen = pd.DataFrame([{
            "_id": self.escena,
            "numero_lagunas_con_agua": numero_lagunas_con_agua,
            "superficie_total_inundada": superficie_total_inundada,
            "porcentaje_inundado": porcentaje_inundado
        }])
        resumen_path = os.path.join(self.pro_escena, "resumen_lagunas_carola.csv")
        resumen.to_csv(resumen_path, index=False, encoding="utf-8-sig")
    
        lagunas_out = lagunas.drop(columns="geometry")
        lagunas_out["_id"] = self.escena
        lagunas_path = os.path.join(self.pro_escena, "lagunas_carola.csv")
        lagunas_out.to_csv(lagunas_path, index=False, encoding="utf-8-sig")
    
        print(f"Resultados guardados en CSV: {resumen_path} y {lagunas_path}")



    def calcular_inundacion_lagunas_principales(self):
        """
        Calculate flooding for main Carola lagoons with defined toponyms.
        
        This method filters lagoons that have a non-null toponym field and computes 
        flooding statistics for each. Only pixels with value 1 (water) are counted, 
        excluding clouds (value 2) and NoData (value -9999). Results include area, 
        flooded area, and percentage of flooding. Data is stored in MongoDB and 
        returned for CSV export.
        
        Returns
        -------
        list of dict
            List of dictionaries containing flooding data for each main lagoon.
            Each dictionary includes: TOPONIMO, area_total, area_inundada, 
            porcentaje_inundacion. Returns empty list if no lagoons with toponyms 
            are found.
        
        Notes
        -----
        - Assumes the Carola lagoons shapefile has a field named 'TOPONIMO'. 
        - Uses custom zonal statistics to count only water pixels (value == 1).
        - Empty or null results are handled gracefully with default values.
        
        Raises
        ------
        Exception
            If spatial operations fail or MongoDB update encounters errors.
        
        See Also
        --------
        calcular_inundacion_lagunas : Calculate flooding for all Carola lagoons.
        calcular_inundacion_lagunas_principales_labordette : Calculate flooding for main Labordette lagoons.
        """
        
        try:
            lagunas = gpd.read_file(self.lagunas)
    
            # Filter lagoons with non-null toponym
            lagunas_principales = lagunas[lagunas["TOPONIMO"].notnull()].copy()
    
            if lagunas_principales.empty:
                print("No hay lagunas principales con 'toponimo' definido.")
                return []
    
            lagunas_principales["area_total"] = lagunas_principales.geometry.area / 10000
    
            with rasterio.open(self.flood_escena) as src:
                resolution = src.res[0] * src.res[1]
    
            # Custom function to count only water pixels (value == 1)
            def count_water_pixels(x):
                return np.sum(x == 1)
    
            stats = zonal_stats(
                lagunas_principales,
                self.flood_escena,
                add_stats={'water_pixels': count_water_pixels},
                raster_out=False,
                geojson_out=False,
            )
    
            lagunas_principales["area_inundada"] = [
                (stat.get('water_pixels', 0) or 0) * resolution / 10000 for stat in stats
            ]
            lagunas_principales["porcentaje_inundacion"] = (
                lagunas_principales["area_inundada"] / lagunas_principales["area_total"] * 100
            )
    
            lagunas_dict = lagunas_principales[["TOPONIMO", "area_total", "area_inundada", "porcentaje_inundacion"]].to_dict("records")
    
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Flood_Data.LagunasPrincipales": lagunas_dict}},
                upsert=True
            )
            print("Resultados de lagunas principales actualizados en MongoDB.")
    
            return lagunas_dict
    
        except Exception as e:
            print(f"Error calculando inundación para lagunas principales: {e}")
            return []


    def export_MongoDB(self, ruta_destino="/mnt/datos_last/mongo_data", formato="json"):
        
        """
        Exporta la base de datos MongoDB a un archivo JSON o CSV y lo guarda en la ruta especificada.
        
        Args:
            ruta_destino (str): Ruta donde se guardarán los archivos exportados.
            formato (str): Formato de exportación, puede ser 'json' o 'csv'.
        """
        
        try:
            # Exportar todas las colecciones a archivos JSON o CSV
            for coleccion_nombre in database.list_collection_names():
                coleccion = database[coleccion_nombre]
                documentos = list(coleccion.find({}))

                if formato == "json":
                    with open(f"{ruta_destino}/{coleccion_nombre}.json", "w") as archivo_json:
                        json.dump(documentos, archivo_json, default=str, indent=4)
                    print(f"Colección '{coleccion_nombre}' exportada a JSON en {ruta_destino}")

                elif formato == "csv":
                    # Convertir los documentos a un DataFrame de Pandas
                    df = pd.DataFrame(documentos)
                    df.to_csv(f"{ruta_destino}/{coleccion_nombre}.csv", index=False, encoding="utf-8-sig")
                    print(f"Colección '{coleccion_nombre}' exportada a CSV en {ruta_destino}")

                else:
                    print("Formato no válido. Debe ser 'json' o 'csv'.")

        except Exception as e:
            print(f"Error durante la exportación: {e}")


    # CSV version
    def guardar_lagunas_principales_en_csv(self, lagunas_dict):
        
        """
        Guarda los datos de las lagunas principales (con toponimo) en un archivo CSV.
        
        Args:
            lagunas_dict (list): Lista de diccionarios con datos de lagunas principales.
        """
        for laguna in lagunas_dict:
            laguna["_id"] = self.escena
            laguna["usgs_id"] = None  # Puedes adaptarlo si lo tienes
    
        df = pd.DataFrame(lagunas_dict)
        ruta_csv = os.path.join(self.pro_escena, "lagunas_principales.csv")
        df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
        print(f"✅ Lagunas principales guardadas en CSV: {ruta_csv}")


    # CSV Version
    def guardar_resumen_lagunas_en_csv(self):
        
        """
        Guarda el resumen de las lagunas de la escena actual en un archivo CSV.
        """
        
        try:
            print(f"Procesando el resumen de lagunas para la escena: {self.escena}")
    
            numero_total_cuerpos = len(gpd.read_file(self.lagunas))
            numero_cuerpos_con_agua = int(self.resultados_lagunas.get("numero_lagunas_con_agua", 0))
            superficie_total_inundada = float(self.resultados_lagunas.get("superficie_total_inundada", 0))
            porcentaje_inundacion = float(self.resultados_lagunas.get("porcentaje_inundado", 0))
            porcentaje_cuerpos_con_agua = (
                float(numero_cuerpos_con_agua / numero_total_cuerpos * 100)
                if numero_total_cuerpos > 0 else 0.0
            )
    
            # Extraer usgs_id de MongoDB
            doc = db.find_one({"_id": self.escena})
            #usgs_id = doc.get("usgs_id", None) if doc else None
    
            # Crear DataFrame y guardar
            df = pd.DataFrame([{
                "_id": self.escena,
                #"usgs_id": usgs_id,
                "numero_cuerpos_con_agua": numero_cuerpos_con_agua,
                "porcentaje_cuerpos_con_agua": porcentaje_cuerpos_con_agua,
                "superficie_total_inundada": superficie_total_inundada,
                "porcentaje_inundacion": porcentaje_inundacion
            }])
    
            ruta_csv = os.path.join(self.pro_escena, "resumen_lagunas.csv")
            df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
            print(f"✅ Resumen de lagunas guardado en CSV: {ruta_csv}")
    
        except Exception as e:
            print(f"⚠️ Error procesando el resumen de lagunas de la escena {self.escena}: {e}")


    def calcular_inundacion_censo(self):
        """
        Calculate flooded surface area for aerial census Level 3 polygons.
        
        This method computes flooding statistics for all polygons in the aerial 
        census Level 3 shapefile by intersecting them with the flood mask. Only 
        pixels with value 1 (water) are counted, excluding clouds (value 2) and 
        NoData (value -9999).
        
        Results are stored in MongoDB and exported as a CSV file with the 
        following fields: Name, descriptio, superficie_inundada (in hectares).
        
        The calculation ensures accurate flood detection by explicitly counting 
        only water pixels, avoiding overestimation from cloud or NoData pixels.
        
        Results are saved to:
        - MongoDB: `Flood_Data.CensoAereo`
        - CSV: `censo_aereo_l3.csv` (per-polygon detail)
        
        Notes
        -----
        - The method uses a custom zonal statistics function to count only water 
          pixels (value == 1), avoiding overestimation from cloud pixels (value == 2).
        - Surface area is converted from square meters to hectares (÷ 10000).
        - The aerial census shapefile must contain 'Name' and 'descriptio' fields.
        
        Raises
        ------
        Exception
            If the shapefile cannot be read, MongoDB update fails, or spatial 
            operations encounter errors.
        
        See Also
        --------
        get_flood_surface : Calculate flooding for marsh zones.
        calcular_inundacion_lagunas : Calculate flooding for Carola lagoons.
        calcular_inundacion_lagunas_labordette : Calculate flooding for Labordette lagoons.
        """
        
        try:
            # Read aerial census shapefile
            censo = gpd.read_file(os.path.join(self.data, "censo_aereo_l3.shp"))
            
            # Load flood mask and get pixel resolution
            with rasterio.open(self.flood_escena) as src:
                resolution = src.res[0] * src.res[1]  # Pixel area in square meters
            
            # Custom function to count only water pixels (value == 1)
            def count_water_pixels(x):
                return np.sum(x == 1)
            
            # Calculate zonal statistics
            stats = zonal_stats(
                censo,
                self.flood_escena,
                add_stats={'water_pixels': count_water_pixels},
                raster_out=False,
                geojson_out=False,
            )
            
            # Create results DataFrame
            censo["superficie_inundada"] = [
                (stat.get('water_pixels', 0) or 0) * resolution / 10000 for stat in stats  # Convert to hectares
            ]
            
            # Select only the fields of interest
            censo_out = censo[["Name", "descriptio", "superficie_inundada"]]
            
            # Add scene field for traceability
            censo_out["_id"] = self.escena
            
            # Save to CSV with UTF-8 encoding
            censo_out_path = os.path.join(self.pro_escena, "censo_aereo_l3.csv")
            censo_out.to_csv(censo_out_path, index=False, encoding="utf-8-sig")
            
            print(f"✅ Aerial census results saved to: {censo_out_path}")
            
            # Update MongoDB
            censo_dict = censo_out[["Name", "descriptio", "superficie_inundada"]].to_dict(orient="records")
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Flood_Data.CensoAereo": censo_dict}},
                upsert=True
            )
            print(f"✅ Aerial census results updated in MongoDB for scene {self.escena}.")
            
        except Exception as e:
            print(f"❌ Error calculating flooding for aerial census: {e}")


    def calcular_inundacion_lagunas_labordette(self):
        """
        Calculate flooded surface area for Labordette lagoons layer.
        
        This method computes flooding statistics for all lagoon polygons in the 
        Labordette lagoons shapefile by intersecting them with the flood mask. Only 
        pixels with value 1 (water) are counted, excluding clouds (value 2) and 
        NoData (value -9999).
        
        Results are stored in MongoDB and exported as CSV files.
        
        The calculation includes:
        - Number of lagoons with water
        - Total flooded surface area (hectares)
        - Percentage of theoretical maximum flooding
        
        Results are saved to:
        - MongoDB: `Flood_Data.LagunasLabordette`
        - CSV: `resumen_lagunas_labordette.csv` (summary)
        - CSV: `lagunas_labordette.csv` (per-lagoon detail)
        
        Notes
        -----
        - The method uses a custom zonal statistics function to count only water 
          pixels (value == 1), avoiding overestimation from cloud pixels (value == 2).
        - This is a parallel implementation to `calcular_inundacion_lagunas()` for 
          the alternative Labordette lagoons dataset.
        
        Raises
        ------
        Exception
            If MongoDB update fails or spatial operations encounter errors.
        
        See Also
        --------
        calcular_inundacion_lagunas : Calculate flooding for Carola lagoons layer.
        calcular_inundacion_lagunas_principales_labordette : Calculate flooding for main Labordette lagoons.
        """
    
        lagunas = gpd.read_file(self.lagunas_labordette)
    
        # Load flood mask
        with rasterio.open(self.flood_escena) as src:
            resolution = src.res[0] * src.res[1]
    
        # Calculate theoretical maximum flood area
        lagunas["area_total"] = lagunas.geometry.area / 10000
        area_maxima_teorica = lagunas["area_total"].sum()
    
        # Custom function to count only water pixels (value == 1)
        def count_water_pixels(x):
            return np.sum(x == 1)
    
        # Calculate zonal statistics
        stats = zonal_stats(
            lagunas,
            self.flood_escena,
            add_stats={'water_pixels': count_water_pixels},
            raster_out=False,
            geojson_out=False,
        )
    
        # Add flooded area column
        lagunas["area_inundada"] = [
            (stat.get('water_pixels', 0) or 0) * resolution / 10000 for stat in stats
        ]
    
        # Calculate metrics
        lagunas_con_agua = lagunas[lagunas["area_inundada"] > 0]
        numero_lagunas_con_agua = len(lagunas_con_agua)
        superficie_total_inundada = lagunas_con_agua["area_inundada"].sum()

        total_lagunas = len(lagunas)
        porcentaje_cuerpos_con_agua = (numero_lagunas_con_agua / total_lagunas * 100) if total_lagunas > 0 else 0.0

        # Porcentaje del total teórico
        porcentaje_inundado = (superficie_total_inundada / area_maxima_teorica * 100) if area_maxima_teorica > 0 else 0.0

        # ---- Guarda en Mongo con campos CONSISTENTES ----
        self.resultados_lagunas_labordette = {
            "numero_cuerpos_con_agua": int(numero_lagunas_con_agua),
            "porcentaje_cuerpos_con_agua": float(porcentaje_cuerpos_con_agua),
            "superficie_total_inundada": float(superficie_total_inundada),
            "porcentaje_inundacion": float(porcentaje_inundado),
        }

        print(f"Lagunas Labordette - Número con agua: {numero_lagunas_con_agua}")
        print(f"Lagunas Labordette - Superficie total inundada: {superficie_total_inundada:.2f} ha")
        print(f"Lagunas Labordette - % cuerpos con agua: {porcentaje_cuerpos_con_agua:.2f}%")
        print(f"Lagunas Labordette - Porcentaje de inundación: {porcentaje_inundado:.2f}%")

        try:
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Flood_Data.LagunasLabordette": self.resultados_lagunas_labordette}},
                upsert=True
            )
            print("Resultados de lagunas Labordette guardados en MongoDB correctamente.")
        except Exception as e:
            print(f"Error al guardar lagunas Labordette en MongoDB: {e}")

        # ---- CSV EXACTO SEGÚN MODELO ----
        resumen = pd.DataFrame([{
            "_id": self.escena,
            "numero_cuerpos_con_agua": int(numero_lagunas_con_agua),
            "porcentaje_cuerpos_con_agua": round(float(porcentaje_cuerpos_con_agua), 6),
            "superficie_total_inundada": round(float(superficie_total_inundada), 2),
            "porcentaje_inundacion": round(float(porcentaje_inundado), 6),
        }])

        resumen_path = os.path.join(self.pro_escena, "resumen_lagunas_labordette.csv")

        # validación dura del esquema antes de escribir
        columnas_esperadas = ["_id","numero_cuerpos_con_agua","porcentaje_cuerpos_con_agua","superficie_total_inundada","porcentaje_inundacion"]
        assert list(resumen.columns) == columnas_esperadas, f"Columnas incorrectas: {list(resumen.columns)}"

        resumen.to_csv(resumen_path, index=False, encoding="utf-8-sig")
        print(f"Resumen Labordette guardado en CSV (modelo OK): {resumen_path}")

        # ---- CSV DE LAGUNAS DETALLADO (solo campos específicos) ----
        lagunas_out = lagunas[["NOMBRE", "area_total", "area_inundada"]].copy()
        lagunas_out["_id"] = self.escena

        # Reordenar columnas en el orden correcto: _id, NOMBRE, area_total, area_inundada
        lagunas_out = lagunas_out[["_id", "NOMBRE", "area_total", "area_inundada"]]

        lagunas_path = os.path.join(self.pro_escena, "lagunas_labordette.csv")
        lagunas_out.to_csv(lagunas_path, index=False, encoding="utf-8-sig")
        print(f"Lagunas Labordette detalladas guardadas en CSV: {lagunas_path}")
    
    
    def calcular_inundacion_lagunas_principales_labordette(self):
        """
        Calculate flooding for main Labordette lagoons with defined names.
        
        This method filters lagoons that have a non-null name field (NOMBRE) and 
        computes flooding statistics for each. Only pixels with value 1 (water) are 
        counted, excluding clouds (value 2) and NoData (value -9999). Results include 
        area, flooded area, and percentage of flooding. Data is stored in MongoDB and 
        returned for CSV export.
        
        Returns
        -------
        list of dict
            List of dictionaries containing flooding data for each main lagoon.
            Each dictionary includes: NOMBRE, area_total, area_inundada, 
            porcentaje_inundacion. Returns empty list if no lagoons with names 
            are found.
        
        Notes
        -----
        - Assumes the Labordette lagoons shapefile has a field named 'NOMBRE'. 
        - Uses custom zonal statistics to count only water pixels (value == 1).
        - Empty or null results are handled gracefully with default values.
        - This is a parallel implementation to `calcular_inundacion_lagunas_principales()` 
          for the alternative Labordette lagoons dataset.
        
        Raises
        ------
        Exception
            If spatial operations fail or MongoDB update encounters errors.
        
        See Also
        --------
        calcular_inundacion_lagunas_labordette : Calculate flooding for all Labordette lagoons.
        calcular_inundacion_lagunas_principales : Calculate flooding for main Carola lagoons.
        """
        
        try:
            lagunas = gpd.read_file(self.lagunas_labordette)
    
            # Filter lagoons with non-null NOMBRE field
            lagunas_principales = lagunas[lagunas["NOMBRE"].notnull()].copy()
    
            if lagunas_principales.empty:
                print("No hay lagunas principales Labordette con 'NOMBRE' definido.")
                return []
    
            lagunas_principales["area_total"] = lagunas_principales.geometry.area / 10000
    
            with rasterio.open(self.flood_escena) as src:
                resolution = src.res[0] * src.res[1]
    
            # Custom function to count only water pixels (value == 1)
            def count_water_pixels(x):
                return np.sum(x == 1)
    
            stats = zonal_stats(
                lagunas_principales,
                self.flood_escena,
                add_stats={'water_pixels': count_water_pixels},
                raster_out=False,
                geojson_out=False,
            )
    
            lagunas_principales["area_inundada"] = [
                (stat.get('water_pixels', 0) or 0) * resolution / 10000 for stat in stats
            ]
            lagunas_principales["porcentaje_inundacion"] = (
                lagunas_principales["area_inundada"] / lagunas_principales["area_total"] * 100
            )
    
            lagunas_dict = lagunas_principales[["NOMBRE", "area_total", "area_inundada", "porcentaje_inundacion"]].to_dict("records")
    
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Flood_Data.LagunasLabordettePrincipales": lagunas_dict}},
                upsert=True
            )
            print("Resultados de lagunas principales Labordette actualizados en MongoDB.")
    
            return lagunas_dict
    
        except Exception as e:
            print(f"Error calculando inundación para lagunas principales Labordette: {e}")
            return []
    
    
    def guardar_lagunas_principales_labordette_en_csv(self, lagunas_dict):
        """
        Save main Labordette lagoons flooding data to CSV file.
    
        Parameters
        ----------
        lagunas_dict : list of dict
            List of dictionaries containing flooding data for main lagoons,
            as returned by `calcular_inundacion_lagunas_principales_labordette()`.
    
        Notes
        -----
        Adds scene ID field to each record before saving.
        Output file: `lagunas_principales_labordette.csv`
        """
    
        for laguna in lagunas_dict:
            laguna["_id"] = self.escena
            # Si no existe el campo usgs_id, se añade vacío (por compatibilidad con modelo)
            if "usgs_id" not in laguna:
                laguna["usgs_id"] = None

        # Crear DataFrame
        df = pd.DataFrame(lagunas_dict)
    
        # Asegurar tipos y redondeos
        if "porcentaje_inundacion" in df.columns:
            df["porcentaje_inundacion"] = (
                pd.to_numeric(df["porcentaje_inundacion"], errors="coerce").round(2)
            )
    
        # Orden correcto de columnas según modelo de datos
        columnas_requeridas = [
            "_id",
            "usgs_id",
            "NOMBRE",
            "area_total",
            "area_inundada",
            "porcentaje_inundacion",
        ]
    
        # Filtrar solo columnas válidas (por si acaso)
        df = df[[c for c in columnas_requeridas if c in df.columns]]
    
        # Guardar CSV
        ruta_csv = os.path.join(self.pro_escena, "lagunas_principales_labordette.csv")
        df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
    
        print(
            f"✅ Lagunas principales Labordette guardadas en CSV con columnas: "
            f"{', '.join(df.columns)}"
        )


    def movidas_de_servidores(self):
        
        """Mueve los productos finales a una subcarpeta y los copia a los servidores remotos usando scp sin contraseña."""
    
        # Crear carpeta final con el nombre de la escena dentro de self.pro_escena
        carpeta_final = os.path.join(self.pro_escena, self.escena)
        try:
            os.makedirs(carpeta_final, exist_ok=True)
        except Exception as e:
            print(f"[ERROR] No se pudo crear la carpeta '{carpeta_final}': {e}")
            return
    
        # Mover todos los archivos PNG y CSV a la subcarpeta final
        patrones = ["*.png", "*.csv"]
        archivos = []
        for patron in patrones:
            archivos.extend(glob.glob(os.path.join(self.pro_escena, patron)))
    
        for archivo in archivos:
            try:
                nombre_original = os.path.basename(archivo)
                if archivo.endswith(".csv"):
                    nombre_nuevo = f"{self.escena}_{nombre_original}"
                else:
                    nombre_nuevo = nombre_original  # .png u otros no cambian
                destino = os.path.join(carpeta_final, nombre_nuevo)
                shutil.move(archivo, destino)
            except Exception as e:
                print(f"[ERROR] Al mover '{archivo}': {e}")
    
        # Servidores remotos con usuario diego_g explícito
        servidores = SERVER_HOSTS

        for host, ruta_remota in servidores.items():
            try:
                ssh_user = SSH_USER if SSH_USER else "diego_g"
                ssh_key = SSH_KEY_PATH if SSH_KEY_PATH else "/root/.ssh/id_rsa"

                print(f"[INFO] Copiando a {host} como usuario {ssh_user}...")
                comando = [
                    "scp", "-r",
                    "-i", ssh_key,  # Especificar la clave SSH
                    carpeta_final,
                    f"{ssh_user}@{host}:{ruta_remota}/"
                ]
                subprocess.check_call(comando)
                print(f"[OK] Copia completada en {host} como {ssh_user}")
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Falló la copia a {host}: {e}")


    def publicar_en_geonetwork(self, username, password):
        
        """
        Publica el XML y el raster de inundación en GeoNetwork usando la función utilitaria.
    
        Parameters
        ----------
        username : str
            Usuario de GeoNetwork.
        password : str
            Contraseña del usuario.
        """
        xml = os.path.join(self.pro_escena, f"{self.escena}_flood_metadata.xml")
        tif = os.path.join(self.pro_escena, f"{self.escena}_flood.tif")
        resultado = subir_xml_y_tif_a_geonetwork(xml, tif, username, password)
        print("📤 Resultado subida GeoNetwork:", resultado)


    def run(self):
        """
        Execute the complete product generation workflow.
    
        Calculates NDVI, NDWI, MNDWI, flood mask, turbidity, depth, and flooded 
        surface area for marsh zones and lagoons. Updates MongoDB with product 
        metadata and saves results as CSV files.
        
        Processing steps:
        1. Calculate spectral indices (NDVI, NDWI, MNDWI)
        2. Generate flood mask
        3. Calculate turbidity and depth
        4. Compute flooded area for marsh zones
        5. Compute flooding for Carola lagoons (all and main)
        6. Compute flooding for Labordette lagoons (all and main)
        7. Compute flooding for aerial census polygons
        8. Generate RGB composition and flood mask images
        9. Transfer products to remote servers
        10. Extract coastline
        11. Generate and publish metadata to GeoNetwork
        
        Raises
        ------
        Exception
            If any product generation step fails.
        """
        
        try:
            print('Comenzando el procesamiento de productos...')
    
            # Calculate products
            self.ndvi()
            self.ndwi()
            self.mndwi()
            self.flood()
            self.turbidity()
            self.depth()
    
            # Flooded surface in marsh zones
            self.get_flood_surface()
    
            # Flooding in Carola lagoons
            self.calcular_inundacion_lagunas()
            lagunas_dict = self.calcular_inundacion_lagunas_principales()
            self.guardar_resumen_lagunas_en_csv()
            if lagunas_dict:
                self.guardar_lagunas_principales_en_csv(lagunas_dict)
    
            # Flooding in Labordette lagoons
            self.calcular_inundacion_lagunas_labordette()  # Ya guarda lagunas_labordette.csv Y resumen_lagunas_labordette.csv
            lagunas_dict_labordette = self.calcular_inundacion_lagunas_principales_labordette()
            # self.guardar_resumen_lagunas_labordette_en_csv()  # ← ELIMINAR ESTA LÍNEA
            if lagunas_dict_labordette:
                self.guardar_lagunas_principales_labordette_en_csv(lagunas_dict_labordette)
            
            # Aerial census Level 3
            self.calcular_inundacion_censo()
    
            # RGB composition and flood mask (JPGs)
            print('vamos a enviar las imágenes a vps y pro')
            self.generate_composition_rgb()
            self.generate_flood_mask()
            
            # Check cloud coverage before sending to servers
            SKIP_CLOUD_CHECK = False  # Para pruebas
            doc = db.find_one({'_id': self.escena})
            cloud_rbios = doc.get('Clouds', {}).get('cloud_RBIOS', 100) if doc else 100
            
            if SKIP_CLOUD_CHECK or cloud_rbios <= 20:
                print(f'Cobertura de nubes en RBIOS: {cloud_rbios}% - Enviando a servidores...')
                self.movidas_de_servidores()
            else:
                print(f'⚠️ Cobertura de nubes en RBIOS: {cloud_rbios}% (>20%) - NO se envían productos a servidores')
    
            # Coastline extraction
            c = Coast(self.pro_escena)
            c.run()
    
            # Metadata generation and publication
            print('vamos con los metadatos')
            generar_metadatos_flood(self)
            self.publicar_en_geonetwork("diegogarcia", "iV524qI&aefq")
    
            # List of generated products for email notification
            nombres_productos = {
                'ndvi_escena': 'NDVI',
                'ndwi_escena': 'NDWI',
                'mndwi_escena': 'MNDWI',
                'flood_escena': 'Flood',
                'turbidity_escena': 'Turbidez',
                'depth_escena': 'Profundidad'
            }
            
            for attr, nombre in nombres_productos.items():
                if getattr(self, attr, None) is not None:
                    self.productos_generados.append(nombre)
    
        except Exception as e:
            print(f"Error durante el procesamiento: {e}")
