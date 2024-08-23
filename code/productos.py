import os, shutil, re, time, subprocess, pandas, rasterio, sys, urllib, fiona, sqlite3, math, pymongo
import numpy as np
import matplotlib.pyplot as plt
from osgeo import gdal, gdalconst
from datetime import datetime, date


from pymongo import MongoClient
client = MongoClient()

database = client.Satelites
db = database.Landsat

class Product(object):
    
    
    '''Esta clase genera los productos de inundacion, turbidez del agua y ndvi de las escenas normalizadas'''
    
        
    def __init__(self, ruta_nor):
        
        
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
        self.flood = None

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
            self.cloud_mask_values = [5440, 5504]

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
        print('BLUE:', self.blue)
        print('FMASK:', self.fmask)
        print('HILLSHADE:', self.hillshade)

       
        try:
        
            db.update_one({'_id':self.escena}, {'$set':{'Productos': []}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('escena importada para productos correctamente')
        
        
        
    def ndvi(self):

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
        
            db.update_one({'_id':self.escena}, {'$set':{'Productos': ['NDVI']}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('NDVI Generado')
        
        
    def flood_(self):
        
        """Calcula la máscara de inundación con base en los criterios especificados"""
    
        self.flood = os.path.join(self.pro_escena, self.escena + '_flood.tif')
        print(self.flood)
    
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
            ndvi_condition = (NDVIP10 > 0.35) & (NDVIMEAN > 0.55)
            water_mask[ndvi_condition] = 0

            # Aplicamos la condición de CobVeg
            if self.sensor == 'OLI':
                cobveg_condition = (COBVEG > 75)
                water_mask[cobveg_condition] = 0

            # Aplicamos la condición de NDVI de la escena
            ndvi_scene_condition = ((NDVISCENE > 0.65) & (DTM > 0))
            water_mask[ndvi_scene_condition] = 0

            # Aplicamos la condición para nubes y sombras de nubes usando np.where
            water_mask = np.where(~np.isin(FMASK_SCENE, self.cloud_mask_values), 2, water_mask)

            # Aplicamos NoData (-9999) al marco exterior
            water_mask[SWIR1 == -9999] = -9999

            # Guardamos el resultado final
            with rasterio.open(
                    self.flood,
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
    
        print(f'Máscara de agua guardada en: {self.flood}')


        try:
        
            db.update_one({'_id':self.escena}, {'$set':{'Productos': ['Flood']}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
        
        
        
    def turbidity(self):
        
        waterMask = os.path.join(self.water_masks, 'water_mask_turb.tif')
        outfile = os.path.join(self.pro_escena, self.escena + '_turbidity.tif')
        print(outfile)
        
        with rasterio.open(self.flood) as flood:
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
                             
        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(TURBIDEZ.astype(rasterio.float32))        
        
        try:
        
            db.update_one({'_id':self.escena}, {'$set':{'Productos': ['Turbidity']}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('Turbidity Mask Generada')


    def depth(self):
        
        # Abrimos las bandas necesarias para correr el algoritmo
        septb4 = os.path.join(self.water_masks, '20230930l9oli202_34_grn2_nir_b5.tif')
        septwmask = os.path.join(self.water_masks, '20230930l9oli202_34_flood.tif')
        
        outfile = os.path.join(self.pro_escena, self.escena + '_depth_.tif')
        print(outfile)

        with rasterio.open(self.flood) as flood:
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

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(DEPTH_.astype(rasterio.float32))


        print('Depth Mask Generada')