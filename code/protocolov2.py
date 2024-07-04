import os
import sys
import rasterio
import subprocess
import stat
import shutil
import re

import numpy as np

from osgeo import gdal, gdalconst
from urllib.request import urlopen
from datetime import datetime

# Mongo Part!
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://digdgeografo:ZUN0GJg1Iz4QeAI5@landsat.adtvxqn.mongodb.net/?retryWrites=true&w=majority&appName=Landsat"

#Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Database Landsat
db = client.Satelites.Landsat

class Landsat:
    
    
    """Clase para trabajar con las Landsat de la nueva Collection 2 Level 2 del USGS"""
    
    def __init__(self, ruta_escena):
        
        """Definimos los atributos que tendrá nuestro objeto a partir del path a su carpeta"""
        
        #Definimos los paths necesarios
        self.ruta_escena = ruta_escena
        self.escena = os.path.split(self.ruta_escena)[1]
        self.ori = os.path.split(self.ruta_escena)[0]
        self.base = os.path.split(self.ori)[0]
        
        self.data = os.path.join(self.base, 'data')
        self.pro = os.path.join(self.base, 'pro')
        if not os.path.exists(self.pro):
            os.makedirs(self.pro)
            
        self.pro_escena = os.path.join(self.pro, self.escena)
        if not os.path.exists(self.pro_escena):
            os.makedirs(self.pro_escena)
        
        #Definimos un grupo de variables de la escena
        data_escena = self.escena.split('_')
        self.sat = "L" + data_escena[0][-1]
        self.sensor = {'L4': 'TM', 'L5': 'TM', 'L7': 'ETM+', 'L8': 'OLI', 'L9': 'OLI'}
        self.nprocesado = data_escena[1]
        self.path = data_escena[2][:3]
        self.row = data_escena[2][-3:]
        self.escena_date = data_escena[3]
        self.escena_procesado_date = data_escena[4]
        self.collection = data_escena[5]
        self.tier = data_escena[6]
        
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
        qk_name = os.path.join(self.ruta_escena, self.escena + '_Quicklook')
        
        if not os.path.exists(qk_name):
            qk = open(qk_name, 'wb')
            qk_open = urlopen(url_base)
            urlimg = qk_open.read()
            qk.write(urlimg)
            qk.close()

            print('QuicKlook descargado')
            
        else:
            print('El Quicklook ya estaba previamente descargado')
        
        #print(url_base)
        print('Landsat iniciada con éxito') 
        
        #Creamos el json para instarlo en la base de datos MongoDB
        self.newesc = {'_id': self.escena, 'usgs_id': self.mtl['LANDSAT_SCENE_ID'], 
                       'tier_id': self.mtl['LANDSAT_PRODUCT_ID'],
                           'lpgs': self.mtl['PROCESSING_SOFTWARE_VERSION'],
                       'category': self.mtl['COLLECTION_CATEGORY'],
                       'Clouds': {'cloud_scene': float(self.mtl['CLOUD_COVER']),
                                 'land cloud cover': float(self.mtl['CLOUD_COVER_LAND'])},
                           'Info': {'Tecnico': 'LAST-EBD Auto', 
                                    'Iniciada': datetime.now(), 'Pasos': {'rad': '', 'nor': ''}}}

        try:

            db.insert_one(self.newesc)

        except Exception as e:

            db.update_one({'_id':self.escena}, {'$set':{'Info.Iniciada': datetime.now()}}, upsert=True)
            #print("Unexpected error:", type(e)) #se Podria dar un error por clave unica, por eso en
            #ese caso, lo que hacemos es actualizar la fecha en la que tratamos la imagen

        print('Landsat instanciada y subida a la base de datos')
        
        
        
        
    def get_cloud_pn(self):

        '''-----\n
        Este metodo recorta la fmask con el shp del Parque Nacional, para obtener la cobertura nubosa en Parque Nacional en el siguiente paso'''

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
        
        mask = (cloud == 21824) | (cloud == 21952)
        
        #################
        ##### DEFINIR LOS VALORES PARA LA MASCARA Y AÑADIR EL DATO A MONGO

        cloud_msk = cloud[mask]
        print(cloud_msk.size)
        clouds = float(cloud_msk.size*900)
        print(clouds)
        PN = 533740500 
        pn_cover = 100 - round((clouds/PN)*100, 2)
        ds = None
        cloud = None
        cloud_msk = None
        clouds = None

        try:

            db.update_one({'_id':self.escena}, {'$set':{'Clouds.cloud_PN': pn_cover}},  upsert=True)

        except Exception as e:
            print("Unexpected error:", type(e), e)

        print("El porcentaje de nubes en el Parque Nacional es de " + str(pn_cover))


    def remove_masks(self):

        '''-----\n
        Este metodo elimina la carpeta en la que hemos ido guardando las mascaras empleadas para obtener los kl y
        la cobertura de nubes en el Parque Nacional'''

        path_masks = os.path.join(self.ruta_escena, 'masks')
        for i in os.listdir(path_masks):

            name = os.path.join(path_masks, i)
            os.chmod(name, stat.S_IWRITE)
            os.remove(name)

        shutil.rmtree(path_masks)


    def projwin(self):

        '''2024. En este metodo vamos a darle el extent a la escena y a hacer el rename. 
        Además de eso vamos a paovechar para aplicar los coeficientes para tener la reflectivdad 
        y la temperatura de superficie en sus valores reales.
        La salida será temporal y sobre ella se realizará la normalización'''
        
        
        olibands = {'B1': 'cblue', 'B2': 'blue', 'B3': 'green', 'B4': 'red', 'B5': 'nir', 'B6': 'swir1',
                   'B7': 'swir2', 'PIXEL': 'fmask', 'B10': 'lst'}
        
        etmbands = {'B1': 'blue', 'B2': 'green', 'B3': 'red', 'B4': 'nir', 'B5': 'swir1',
                   'B7': 'swir2', 'PIXEL': 'fmask', 'B6': 'lst'}
        
        
        geo = '/media/diego/31F8C0B3792FC3B6/EBD/Protocolo_v2_2024/geo'
        path_rad = os.path.join(geo, self.escena)
        os.makedirs(path_rad, exist_ok=True)
        
        for i in os.listdir(self.ruta_escena):
            
            if self.sat in ['L8', 'L9']:
                if i.endswith('.TIF'):

                    banda = i.split('_')[-1][:-4]

                    if banda in olibands.keys():
                        ins = os.path.join(self.ruta_escena, i)

                        name = self.escena_date + self.sat + self.sensor[self.sat] + self.path + '_' + self.row[1:] + '_g2_' + olibands[banda] + '.tif'
                        out = os.path.join(path_rad, name.lower())

                        cmd = "gdal_translate -projwin  623385.0 4266315.0 867615.0 4034685.0 {} {}".format(ins, out)
                        print(cmd)
                        os.system(cmd)

                    else:
                        continue
                        
            elif self.sat in ['L7', 'L5', 'L4']:
                if i.endswith('.TIF'):

                    banda = i.split('_')[-1][:-4]

                    if banda in etmbands.keys():
                        ins = os.path.join(self.ruta_escena, i)

                        name = self.escena_date + self.sat + self.sensor[self.sat] + self.path + '_' + self.row[1:] + '_g2_' + etmbands[banda] + '.tif'
                        out = os.path.join(path_rad, name.lower())

                        cmd = "gdal_translate -projwin  623385.0 4266315.0 867615.0 4034685.0 {} {}".format(ins, out)
                        print(cmd)
                        os.system(cmd)

                    else:
                        continue
                
                
            else:
                print('Lo siento, pero no encuentro el satélite')


    #Aplicamos los coeficientes de reflectancia y temperatura de superficie
    def coef_sr_st(self):

        '''Esta función va a aplicar los coeficientes de reflectancia y temperatura 
        de superficie a las bandas de la escena.'''

        #Recorremos todas las bandas de la escena y las abrimos con rasterio
        #Para aplicar los coeficientes de reflectancia y temperatura de superficie
        #Solo hay que discriminar entre la banda lst y el resto de bandas, dejando fuera a fmask
        path_geo = os.path.join(self.geo, self.escena)
        path_rad = os.path.join(self.rad, self.escena)
        os.makedirs(path_rad, exist_ok=True)
        
        for i in os.listdir(path_geo):

            if i.endswith('.tif'):

                banda = i.split('_')[-1][:-4]

                if banda not in ['fmask', 'lst']:
                    
                    #nombre de salida que reemplace la  _g2_ del nombre original por _gr2_
                    rs = os.path.join(path_geo, i)
                    out = os.path.join(path_rad, i.replace('_g2_', '_gr2_'))

                    with rasterio.open(rs) as src:
                        RS = src.read(1)
                        meta = src.meta

                    sr = RS * 0.0000275 - 0.2
                    srnd = np.where(RS==0, 0, sr)
                    meta.update(dtype=rasterio.float32)

                    with rasterio.open(out, 'w', **meta) as dst:
                        dst.write(srnd.astype(rasterio.float32), 1)

                elif banda == 'lst':

                    #nombre de salida que reemplace la  _g2_ del nombre original por _gr2_
                    rs = os.path.join(path_geo, i)
                    out = os.path.join(path_rad, i.replace('_g2_', '_gr2_'))

                    with rasterio.open(rs) as src:
                        RS = src.read(1)
                        meta = src.meta

                    lst = RS * 0.00341802 + 149.0
                    lst -= 273.15
                    lstnd = np.where(RS==0, 0, lst)

                    meta.update(dtype=rasterio.float32)

                    with rasterio.open(out, 'w', **meta) as dst:
                        dst.write(lstnd.astype(rasterio.float32), 1)

                elif banda == 'fmask':
                                       
                    src = rs = os.path.join(path_geo, i)
                    dst = os.path.join(path_rad, i.replace('_g2_', '_gr2_'))                
                    shutil.copy(src, dst)

                else:                                       
                    continue

        print('Coeficientes aplicados con éxito')

    def normalización(self):

        '''Este método realiza la normalización basada en las áreas pseudo invariantes\n'''
        pass