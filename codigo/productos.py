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
from osgeo import gdal, gdalconst
from datetime import datetime, date
from rasterstats import zonal_stats

# Añadimos la ruta con el código a nuestro pythonpath para poder importar la clase Landsat
sys.path.append('/root/git/ProtocoloV2/codigo')
from utils import process_composition_rgb, process_flood_mask
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

        # Shape con recintos
        self.recintos = os.path.join(self.data, 'Recintos_Marisma.shp')
        self.lagunas = os.path.join(self.data, 'lagunas_carola_32629.shp')
        self.resultados_lagunas = {}
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
        print('BLUE:', self.swir1)
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
        #print(self.flood_escena)
    
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
             rasterio.open(self.ndvi_escena) as ndvi_scene, \
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
            water_mask = (SWIR1 < 0.12) #& ((DTM <= 5) | (MNDWI >= 0))

            #water_mask = (SWIR1 < 0.12) & (DTM <= 5)

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
            #if self.sensor == 'OLI':
            cobveg_condition = (COBVEG > 75)
            water_mask[cobveg_condition] = 0

            # Aplicamos la condición de NDVI de la escena
            # ndvi_scene_condition = ((NDVISCENE > 0.60) & ((DTM == 0) | (DTM > 2.5)))
            ndvi_scene_condition = ((NDVISCENE > 0.60) & (DTM > 2.5)) # Vamos a dejar el mar fuera a ver si entra en agua
            water_mask[ndvi_scene_condition] = 0

            # Podríamos corregir así el mar con ndvis muy altos?
            # Aqui la idea sería mandar los pixeles saturados en el mar a agua, pero podría pasar en la orilla?
            # Normalmente a la orilla no le afecta
            # ndvi_scene_condition_2 = ((NDVISCENE > 0.60) & (DTM == 0))
            # water_mask[ndvi_scene_condition] = 1

            # Aplicamos la condición para nubes y sombras de nubes usando np.where
            water_mask = np.where(~np.isin(FMASK_SCENE, self.cloud_mask_values), 2, water_mask)

            # Condición para que sean agua los pixeles que son mayores de 0 en ndwi y mndwi y agua en fmask ?
            # Intento de solucionar los "NoData" de agua en el mar
            # Vamos a sumar MDNWI, NDWI y Fmask cuando es cuando es agua y si al menos dos de ellos dan valor agua el pixel pasará a ser agua

            # Reclasificamos los 2 índices y Fmask
            mndwi_r = np.where(MNDWISCENE > 0, 1, 0)
            ndwi_r = np.where(NDWISCENE > 0, 1, 0)
            fmask_r = np.where(FMASK_SCENE == self.cloud_mask_values[1], 1, 0)
            # Suma de los 3
            water_ix_sum = mndwi_r + ndwi_r + fmask_r
                 
            # water_masks_condition = (NDWISCENE > 0) & (MNDWISCENE > 0) #& (FMASK_SCENE == 21952) | (FMASK_SCENE == 5504)
            # Si dos de ellos dan valor agua el pixel pasa a ser agua. Esto puede solucionar el mar y no afectar al resto
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
                    dtype='int16',  # Aseguramos que el dtype es int16 para manejar el valor NoData correctamente
                    crs=dtm.crs,
                    transform=dtm.transform,
                    compress='lzw',
                    nodata=-9999  # Especificamos el valor NoData
            ) as dst:
                dst.write(water_mask, 1)


        try:
        
            db.update_one({'_id':self.escena}, {'$addToSet':{'Productos': 'Flood'}},  upsert=True)
            
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

        """Calcula la superficie inundada por zonas de marisma y actualiza MongoDB.

        Utiliza un shapefile de recintos de marisma y una máscara de inundación para calcular la superficie 
        inundada en hectáreas para cada zona. Los resultados se guardan en un archivo CSV y en la base de datos.
        """
        
        print('Vamos a calcular la superficie inundada para los recintos de la marisma')
        # Cargar el raster de la máscara de agua
        with rasterio.open(self.flood_escena) as src:
            raster_data = src.read(1)  # Leer la primera banda
            raster_transform = src.transform  # Obtener la transformación del raster
            raster_crs = src.crs  # Obtener el sistema de referencia del raster
    
        # Cargar el shapefile de las zonas de marisma
        zonas_marisma = gpd.read_file(self.recintos)
        zonas_marisma = zonas_marisma.to_crs(raster_crs)  # Reproyectar al CRS del raster si es necesario
    
        #return raster_data, raster_transform, zonas_marisma
        # Crear un DataFrame para almacenar los resultados
        resultados = []
    
        for index, row in zonas_marisma.iterrows():
            nombre = row['Nombre']
            geom = row['geometry']
    
            # Crear una máscara para el polígono actual
            mask = geometry_mask([geom], transform=raster_transform, invert=True, out_shape=raster_data.shape)
    
            # Calcular la superficie inundada
            superficie_inundada = ((raster_data[mask] == 1).sum() * raster_transform[0] ** 2) / 10000  # Asumiendo que el valor 1 representa agua
    
            # Añadir los resultados al DataFrame
            resultados.append({
                'nombre': nombre,
                'superficie_inundada': superficie_inundada
            })
    
        resultados_df = pd.DataFrame(resultados)
    
        # Guardar los resultados en un archivo CSV (opcional)
        resultados_df.to_csv(os.path.join(self.pro_escena, 'superficie_inundada.csv'), index=False, encoding="utf-8-sig")
    
        #return resultados_df
        
        # Convertir el DataFrame en un diccionario

        inundacion_dict = resultados_df.set_index('nombre')['superficie_inundada'].to_dict()
        
        # Supongamos que 'self.escena' es el ID del documento en MongoDB
        #document_id = self.escena
        
        # Encuentra el documento por su _id
        documento = db.find_one({"_id": self.escena})
        
        # Si el documento tiene un campo "Productos"
        if documento and "Productos" in documento:
            productos = documento["Productos"]
            
            # Verifica si "Flood" ya existe en la lista "Productos"
            flood_exists = False
            for index, producto in enumerate(productos):
                if producto == "Flood":
                    # Reemplaza "Flood" con un diccionario
                    productos[index] = {"Flood": inundacion_dict}
                    flood_exists = True
                    break
            
            # Si "Flood" no existe, añádelo como un nuevo diccionario
            if not flood_exists:
                productos.append({"Flood": inundacion_dict})
            
            # Actualiza el documento en la base de datos con la nueva lista "Productos"
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Productos": productos}}
            )
        else:
            # Si el documento no tiene un campo "Productos", lo creas y añades "Flood"
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Productos": [{"Flood": inundacion_dict}]}}
            )
        
        print(f"Superficie inundada para la escena {self.escena} ha sido actualizada en MongoDB.")


    # CSV version
    def calcular_inundacion_lagunas(self):
        
        """
        Calcula la superficie inundada para las lagunas, actualiza MongoDB y guarda resultados en CSV.
        """
    
        lagunas = gpd.read_file(self.lagunas)
    
        # 1. Cargar la máscara de inundación
        with rasterio.open(self.flood_escena) as src:
            resolution = src.res[0] * src.res[1]  # Resolución del píxel en unidades de área
    
        # 2. Calcular el área máxima teórica de inundación
        lagunas["area_total"] = lagunas.geometry.area / 10000
        area_maxima_teorica = lagunas["area_total"].sum()
    
        # 3. Calcular estadísticas zonales
        stats = zonal_stats(
            lagunas,
            self.flood_escena,
            stats=["sum"],
            raster_out=False,
            geojson_out=False,
        )
    
        # 4. Añadir superficie inundada a la capa de lagunas
        lagunas["area_inundada"] = [
            (stat["sum"] or 0) * resolution / 10000 for stat in stats
        ]
    
        # 5. Calcular métricas
        lagunas_con_agua = lagunas[lagunas["area_inundada"] > 0]
        numero_lagunas_con_agua = len(lagunas_con_agua)
        superficie_total_inundada = lagunas_con_agua["area_inundada"].sum()
        porcentaje_inundado = (superficie_total_inundada / area_maxima_teorica) * 100
    
        # 6. Guardar resultados en el diccionario
        self.resultados_lagunas = {
            "numero_lagunas_con_agua": numero_lagunas_con_agua,
            "superficie_total_inundada": superficie_total_inundada,
            "porcentaje_inundado": porcentaje_inundado,
        }
    
        # 7. Mostrar resultados
        print(f"Número de lagunas con agua: {numero_lagunas_con_agua}")
        print(f"Superficie total inundada: {superficie_total_inundada:.2f} ha")
        print(f"Porcentaje de inundación respecto al total teórico: {porcentaje_inundado:.2f}%")
    
        # 8. Guardar resultados en MongoDB dentro de Flood
        try:
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Flood.Lagunas": self.resultados_lagunas}},
                upsert=True
            )
            print("Resultados de lagunas guardados en MongoDB correctamente.")
        except Exception as e:
            print(f"Error al guardar en MongoDB: {e}")
    
        # 9. Guardar resultados en CSV
        resumen = pd.DataFrame([{
            "_id": self.escena,
            "numero_lagunas_con_agua": numero_lagunas_con_agua,
            "superficie_total_inundada": superficie_total_inundada,
            "porcentaje_inundado": porcentaje_inundado
        }])
        resumen_path = os.path.join(self.pro_escena, "resumen_lagunas_carola.csv")
        resumen.to_csv(resumen_path, index=False, encoding="utf-8-sig")
    
        lagunas_out = lagunas.drop(columns="geometry")
        lagunas_out["escena"] = self.escena
        lagunas_path = os.path.join(self.pro_escena, "lagunas_carola.csv")
        lagunas_out.to_csv(lagunas_path, index=False, encoding="utf-8-sig")
    
        print(f"✅ Resultados guardados en CSV: {resumen_path} y {lagunas_path}")



    def calcular_inundacion_lagunas_principales(self):
        
        """
        Calcula la inundación para lagunas principales (con toponimo no nulo), 
        actualiza MongoDB y devuelve el lagunas_dict para guardar en CSV.
        
        Returns:
            list: Lista de diccionarios con información por laguna principal.
        """
        
        try:
            # Leer la capa de lagunas
            lagunas = gpd.read_file(self.lagunas)
    
            # Filtrar lagunas con toponimo no nulo
            lagunas_principales = lagunas[lagunas["TOPONIMO"].notnull()].copy()
    
            if lagunas_principales.empty:
                print("⚠️ No hay lagunas principales con 'toponimo' definido.")
                return []
    
            # Calcular área total
            lagunas_principales["area_total"] = lagunas_principales.geometry.area / 10000
    
            # Cargar la máscara de inundación
            with rasterio.open(self.flood_escena) as src:
                resolution = src.res[0] * src.res[1]
    
            # Calcular estadísticas zonales
            stats = zonal_stats(
                lagunas_principales,
                self.flood_escena,
                stats=["sum"],
                raster_out=False,
                geojson_out=False,
            )
    
            # Añadir estadísticas
            lagunas_principales["area_inundada"] = [
                (stat["sum"] or 0) * resolution / 10000 for stat in stats
            ]
            lagunas_principales["porcentaje_inundacion"] = (
                lagunas_principales["area_inundada"] / lagunas_principales["area_total"] * 100
            )
    
            # Preparar datos para MongoDB y CSV
            lagunas_dict = lagunas_principales[["TOPONIMO", "area_total", "area_inundada", "porcentaje_inundacion"]].to_dict("records")
    
            # Actualizar en MongoDB
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Flood.LagunasPrincipales": lagunas_dict}},
                upsert=True
            )
            print("✅ Resultados de lagunas principales actualizados en MongoDB.")
    
            return lagunas_dict
    
        except Exception as e:
            print(f"❌ Error calculando inundación para lagunas principales: {e}")
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
            usgs_id = doc.get("usgs_id", None) if doc else None
    
            # Crear DataFrame y guardar
            df = pd.DataFrame([{
                "_id": self.escena,
                "usgs_id": usgs_id,
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
        Calcula la superficie inundada para los polígonos del censo aéreo L3,
        guarda los resultados en un CSV y actualiza la base de datos MongoDB.
        """
    
        try:
            # Leer el shapefile del censo aéreo
            censo = gpd.read_file(os.path.join(self.data, "censo_aereo_l3.shp"))
            
            # Cargar la máscara de inundación
            with rasterio.open(self.flood_escena) as src:
                resolution = src.res[0] * src.res[1]  # Área de un píxel
    
            # Calcular estadísticas zonales
            stats = zonal_stats(
                censo,
                self.flood_escena,
                stats=["sum"],
                raster_out=False,
                geojson_out=False,
            )
    
            # Crear DataFrame de resultados
            censo["superficie_inundada"] = [
                (stat["sum"] or 0) * resolution / 10000 for stat in stats  # pasamos a hectáreas
            ]
    
            # Seleccionar solo los campos que nos interesan
            censo_out = censo[["Name", "descriptio", "superficie_inundada"]]
    
            # Añadir campo escena para trazabilidad
            censo_out["escena"] = self.escena
    
            # Guardar en CSV con codificación UTF-8
            censo_out_path = os.path.join(self.pro_escena, "censo_aereo_l3.csv")
            censo_out.to_csv(censo_out_path, index=False, encoding="utf-8-sig")
    
            print(f"✅ Resultados del censo aéreo guardados en: {censo_out_path}")
    
            # ---- Actualizar en MongoDB ----
            censo_dict = censo_out[["Name", "descriptio", "superficie_inundada"]].to_dict(orient="records")
            db.update_one(
                {"_id": self.escena},
                {"$set": {"Flood.CensoAereo": censo_dict}},
                upsert=True
            )
            print(f"✅ Resultados del censo aéreo actualizados en MongoDB para escena {self.escena}.")
    
        except Exception as e:
            print(f"❌ Error calculando inundación para censo aéreo: {e}")



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
    
        # Hosts remotos definidos en ~/.ssh/config
        servidores = {
            "vps84": "/srv/productos_recibidos_last",
            "vps83": "/srv/productos_recibidos_last"
        }
    
        for host, ruta_remota in servidores.items():
            try:
                print(f"[INFO] Copiando a {host}...")
                comando = [
                    "scp", "-r", carpeta_final,
                    f"{host}:{ruta_remota}/"
                ]
                subprocess.check_call(comando)
                print(f"[OK] Copia completada en {host}")
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Falló la copia a {host}: {e}")


    def run(self):
        
        """
        Ejecuta el flujo completo de generación de productos.
    
        Calcula NDVI, NDWI, MNDWI, máscara de inundación, turbidez, profundidad,
        y la superficie inundada, actualizando productos en MongoDB y guardando resultados como CSV.
        """
        
        try:
            print('Comenzando el procesamiento de productos...')
    
            # Cálculo de productos
            self.ndvi()
            self.ndwi()
            self.mndwi()
            self.flood()
            self.turbidity()
            self.depth()
    
            # Superficie inundada en recintos de la marisma
            self.get_flood_surface()
    
            # Inundación en lagunas (capa Carola)
            self.calcular_inundacion_lagunas()
    
            # Inundación en lagunas principales
            lagunas_dict = self.calcular_inundacion_lagunas_principales()
    
            # Guardar todos los CSV
            #self.guardar_inundacion_en_csv()
            self.guardar_resumen_lagunas_en_csv()
            if lagunas_dict:
                self.guardar_lagunas_principales_en_csv(lagunas_dict)
            # Censo Aereo Nivel 3
            self.calcular_inundacion_censo()
    
            # Composición RGB y máscara de agua (JPGs)
            print('vamos a enviar las imágenes a vps y pro')
            self.generate_composition_rgb()
            self.generate_flood_mask()
            self.movidas_de_servidores()

            # Línea de costa
            c = Coast(self.pro_escena)
            c.run()
    
        except Exception as e:
            print(f"⚠️ Error durante el procesamiento: {e}")