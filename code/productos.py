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

        outfile = os.path.join(self.pro_escena, self.escena + '_ndvi_.tif')
        print(outfile)
        
        with rasterio.open(self.nir) as nir:
            NIR = nir.read()
            
        with rasterio.open(self.red) as red:
            RED = red.read()

        num = NIR.astype(float)-RED.astype(float)
        den = NIR+RED
        ndvi = np.true_divide(num, den)
                
        profile = nir.meta
        profile.update(nodata=-9999)
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(ndvi.astype(rasterio.float32))
                    
        try:
        
            db.update_one({'_id':self.escena}, {'$set':{'Productos': ['NDVI']}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('NDVI Generado')
        
        
        
    def flood(self):

        """Aquí está la clave de todo. Tenemos que abrir un montón de rasters para poder ir aplicando condiciones """
        
        waterMask = os.path.join(self.data, 'water_mask_turb.tif')
        outfile = os.path.join(self.productos, self.escena + '_flood.tif')
        print(outfile)

        # Abrimos los rasters
        dtm = os.path.join(self.water_masks, 'dtm_202_34.tif')
        with rasterio.open(dtm) as dtm:
            DTM = dtm.read()

        slope = os.path.join(self.water_masks, 'slope_202_34.tif')
        with rasterio.open(slope) as slope:
            SLOPE = slope.read()
            
        fmask = os.path.join(self.water_masks, 'fmask_202_34.tif')
        with rasterio.open(fmask) as fmask:
            FMASK = fmask.read()

        ndwi = os.path.join(self.water_masks, 'ndwi_p99_202_34.tif')
        with rasterio.open(ndwi) as ndwi:
            NDWI = ndwi.read()

        mndwi = os.path.join(self.water_masks, 'mndwi_p99_202_34.tif')
        with rasterio.open(mndwi) as mndwi:
            MNDWI = mndwi.read()

        cobveg = os.path.join(self.water_masks, 'cob_veg_202_34.tif')
        with rasterio.open(cobveg) as cobveg:
            COBVEG = cobveg.read()

        ndvi_p10 = os.path.join(self.water_masks, 'ndvi_p10_202_34.tif')
        with rasterio.open(ndvi_p10) as ndvi_p10:
            NDVIP10 = ndvi_p10.read()

        ndvi_mean = os.path.join(self.water_masks, 'ndvi_mean_202_34.tif')
        with rasterio.open(ndvi_mean) as ndvi_mean:
            NDVIMEAN = ndvi_mean.read()

        # QUEDAN LOS NDVIS Y LA ALTURA DE LA VEGETACION Y LOS PROPIOS DE CADA ESCENA: FMASK ESCENA Y HILLSHADE
        # MAS LA BANDA DEL SWIR1
        with rasterio.open(self.fmask) as fmask:
            FMASK = fmask.read()

        with rasterio.open(self.hillshade) as hillsh:
            HILLSH = hillsh.read()
            
        with rasterio.open(self.swir1) as swir1:
            SWIR1 = swir1.read()
            

        flood = np.where(((FMASK != 2) & (FMASK != 4)) & ((SWIR1 != 0) & (SWIR1 <= 1200)) & (WMASK > 0), 1, 0)
        
        
        profile = swir1.meta
        profile.update(nodata=0)
        profile.update(dtype=rasterio.ubyte)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(flood.astype(rasterio.ubyte))
            
        #Insertamos la cobertura de nubes en la BD
        # connection = pymongo.MongoClient("mongodb://localhost")
        # db=connection.teledeteccion
        # landsat = db.landsat
        
        
        try:
        
            db.update_one({'_id':self.escena}, {'$set':{'Productos': ['Flood']}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('Flood Mask Generada')
        
        
        
    def turbidity(self, flood):
        
        waterMask = os.path.join(self.data, 'water_mask_turb.tif')
        outfile = os.path.join(self.productos, self.escena + '_turbidity.tif')
        print(outfile)
        
        with rasterio.open(flood) as flood:
            FLOOD = flood.read()
        
        with rasterio.open(waterMask) as wmask:
            WMASK = wmask.read()
            
        with rasterio.open(self.blue) as blue:
            BLUE = blue.read()
            BLUE = np.where(BLUE == 0, 1, BLUE)
            BLUE = np.true_divide(BLUE, 10000)
                        
        with rasterio.open(self.green) as green:
            GREEN = green.read()
            GREEN = np.where(GREEN == 0, 1, GREEN)
            GREEN = np.true_divide(GREEN, 10000)
            GREEN_R = np.where((GREEN<0.1), 0.1, GREEN)
            GREEN_RECLASS = np.where((GREEN_R>=0.4), 0.4, GREEN_R)

        with rasterio.open(self.red) as red:
            RED = red.read()
            RED = np.where(RED == 0, 1, RED)
            RED = np.true_divide(RED, 10000)
            RED_RECLASS = np.where((RED>=0.2), 0.2, RED)
            
        with rasterio.open(self.nir) as nir:
            NIR = nir.read()
            NIR = np.where(NIR == 0, 1, NIR)
            NIR = np.true_divide(NIR, 10000)
            NIR_RECLASS = np.where((NIR>0.5), 0.5, NIR)
            
        with rasterio.open(self.swir1) as swir1:
            SWIR1 = swir1.read()
            SWIR1 = np.where(SWIR1 == 0, 1, SWIR1)
            SWIR1 = np.true_divide(SWIR1, 10000)
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
                             np.where(((FLOOD == 1) & (WMASK == 2)), rio, 0))
        
        profile = swir1.meta
        profile.update(nodata=0)
        profile.update(dtype=rasterio.float32)
                             
        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(TURBIDEZ.astype(rasterio.float32))
            
        #Insertamos la cobertura de nubes en la BD
        connection = pymongo.MongoClient("mongodb://localhost")
        db=connection.teledeteccion
        landsat = db.landsat
        
        
        try:
        
            landsat.update_one({'_id':self.escena}, {'$set':{'Productos': ['Turbidity']}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('Turbidity Mask Generada')


    def depth(self, flood, septb4, septwmask):

        outfile = os.path.join(self.productos, self.escena + '_depth_.tif')
        print(outfile)

        with rasterio.open(flood) as flood:
            FLOOD = flood.read()
            
        with rasterio.open(septb4) as septb4:
            
            SEPTB4 = septb4.read()
                        
            #En reflectividades
            SEPTB4_REF = np.true_divide(SEPTB4, 306)
            SEPTB4_REF = np.where(SEPTB4 >= 0.830065359, 0.830065359, SEPTB4)
        
        with rasterio.open(septwmask) as septwater:
            SEPTWMASK = septwater.read()
            
        #Banda 1
        with rasterio.open(self.blue) as blue:
            BLUE = blue.read()
            BLUE = np.where(BLUE >= 50, 50, BLUE)

            #Blue en reflectividad
            BLUE_REF = np.true_divide(BLUE, 398)
            
            
        #Banda 2
        with rasterio.open(self.green) as green:
            GREEN = green.read()
            
            #Green en reflectivdiad
            GREEN_REF = np.true_divide(GREEN, 401) #
            
        
        #Banda 4
        with rasterio.open(self.nir) as nir:
            NIR = nir.read()
            
            #NIR en reflectividad
            NIR_REF = np.true_divide(NIR, 422)
            
        
        #Banda 5
        with rasterio.open(self.swir1) as swir1:
            SWIR1 = swir1.read()
            
            #SWIR1 en reflecrtividad
            SWIR1_REF = np.true_divide(SWIR1, 324)
            
        
        #Ratios
        RATIO_GREEN_NIR = np.true_divide(GREEN_REF, NIR_REF)
        RATIO_GREEN_NIR = np.where(RATIO_GREEN_NIR >= 2.5, 2.5, RATIO_GREEN_NIR)
        RATIO_NIR_SEPTNIR = np.true_divide(NIR, SEPTB4)           
        
        #Profundidad para la marisma        
            
        a = 5.293739862 + (-0.038684824 * BLUE) + (0.02826867 * SWIR1) + (-0.007525455 * SEPTB4) + \
            (1.023724916 * RATIO_GREEN_NIR) + (-1.041844944 * RATIO_NIR_SEPTNIR)
        
        DEPTH = np.exp(a) - 0.01
        
        #PASAR A NODATA EL AGUA DE SEPTIEMBRE!!!!
        
        #Se podría pasar directamente a SWIR1 <= 53
        DEPTH_ = np.where((FLOOD == 1) & (SEPTWMASK == 0), DEPTH, 0)

        profile = swir1.meta
        profile.update(nodata=0)
        profile.update(dtype=rasterio.float32)
        profile.update(driver='GTiff')

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(DEPTH_.astype(rasterio.float32))


        print('Depth Mask Generada')