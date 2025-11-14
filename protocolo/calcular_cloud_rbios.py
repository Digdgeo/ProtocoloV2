
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Calcula cloud_RBIOS para todas las escenas que ya están en MongoDB
sin necesidad de volver a normalizar.
"""

import os
import sys
import numpy as np
from osgeo import gdal
import subprocess
import shutil
from datetime import datetime

sys.path.append('/root/git/ProtocoloV2/protocolo')
from pymongo import MongoClient

client = MongoClient()
db = client.Satelites.Landsat

# Configuración
ORI_PATH = '/mnt/datos_last/ori'
DATA_PATH = '/mnt/datos_last/data'
RBIOS_AREA = 2686250000  # Área de RBIOS en m²

def extraer_info_escena(nombre_escena):
    """Extrae información necesaria del nombre de la escena."""
    try:
        partes = nombre_escena.split('_')
        fecha = datetime.strptime(partes[3], '%Y%m%d')
        sat = "L" + partes[0][-1]
        
        # Determinar sensor
        if sat in ['L8', 'L9']:
            sensor = 'OLI'
        elif sat == 'L7':
            sensor = 'ETM+'
        else:
            sensor = 'TM'
        
        path = partes[2][:3]
        row = partes[2][-3:]
        
        # Construir last_name como lo hace la clase Landsat
        if sensor == 'ETM+':
            last_name = f"{partes[3]}{sat.lower()}{sensor[:-1]}{path}_{row[1:]}".lower()
        else:
            last_name = f"{partes[3]}{sat.lower()}{sensor.lower()}{path}_{row[1:]}".lower()
        
        # Valores de máscara según sensor
        cloud_mask_values = [21824, 21952] if sensor == 'OLI' else [5440, 5504]
        
        return last_name, cloud_mask_values, fecha
    except Exception as e:
        return None, None, None

def calcular_cloud_rbios(ruta_escena, cloud_mask_values):
    """Calcula el porcentaje de cobertura de nubes en RBIOS."""
    
    shape = os.path.join(DATA_PATH, 'RBIOS.shp')
    
    # Buscar QA_PIXEL
    qa_pixel = None
    for archivo in os.listdir(ruta_escena):
        if archivo.endswith('QA_PIXEL.TIF'):
            qa_pixel = os.path.join(ruta_escena, archivo)
            break
    
    if not qa_pixel or not os.path.exists(qa_pixel):
        return None
    
    # Crear directorio temporal
    temp_dir = os.path.join(ruta_escena, 'temp_rbios')
    os.makedirs(temp_dir, exist_ok=True)
    
    salida = os.path.join(temp_dir, 'cloud_RBIOS.TIF')
    
    # Ejecutar gdalwarp
    cmd = [
        "gdalwarp", "-dstnodata", "0", "-cutline", shape,
        "-crop_to_cutline", qa_pixel, salida
    ]
    
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate()
        
        if proc.returncode != 0:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None
        
        # Leer el raster recortado
        ds = gdal.Open(salida)
        if ds is None:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None
            
        cloud = np.array(ds.GetRasterBand(1).ReadAsArray())
        
        # Crear máscara con valores de píxeles claros
        mask = (cloud == cloud_mask_values[0]) | (cloud == cloud_mask_values[1])
        
        # Contar píxeles claros
        cloud_msk = cloud[mask]
        clouds = float(cloud_msk.size * 900)
        
        # Calcular porcentaje
        rbios_cover = round(100 - (clouds / RBIOS_AREA) * 100, 2)
        
        # Limpiar
        ds = None
        shutil.rmtree(temp_dir, ignore_errors=True)
        
        return rbios_cover
        
    except Exception as e:
        print(f"      Error en cálculo: {e}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None

def main():
    """Función principal."""
    
    print(f"\n{'='*80}")
    print("CALCULO DE cloud_RBIOS PARA ESCENAS NORMALIZADAS")
    print(f"{'='*80}\n")
    
    if not os.path.exists(ORI_PATH):
        print(f"Error: {ORI_PATH} no existe")
        return
    
    escenas = sorted([d for d in os.listdir(ORI_PATH) 
                     if os.path.isdir(os.path.join(ORI_PATH, d))])
    
    print(f"Encontradas {len(escenas)} escenas en ori\n")
    
    procesadas = 0
    ya_procesadas = 0
    no_en_mongo = 0
    errores = 0
    
    for escena in escenas:
        last_name, cloud_values, fecha = extraer_info_escena(escena)
        
        if not last_name:
            continue
        
        # Verificar si existe en MongoDB
        doc = db.find_one({'_id': last_name})
        if not doc:
            print(f"Saltando {escena}: No existe en MongoDB")
            no_en_mongo += 1
            continue
        
        # Verificar si ya tiene cloud_RBIOS
        if doc.get('Clouds', {}).get('cloud_RBIOS') is not None:
            ya_procesadas += 1
            continue
        
        print(f"Procesando {escena}...", end=' ')
        
        ruta_escena = os.path.join(ORI_PATH, escena)
        rbios_cover = calcular_cloud_rbios(ruta_escena, cloud_values)
        
        if rbios_cover is not None:
            # Actualizar MongoDB
            db.update_one(
                {'_id': last_name},
                {'$set': {'Clouds.cloud_RBIOS': rbios_cover}}
            )
            print(f"OK (cloud_RBIOS = {rbios_cover}%)")
            procesadas += 1
        else:
            print("ERROR")
            errores += 1
    
    print(f"\n{'='*80}")
    print(f"RESUMEN")
    print(f"{'='*80}")
    print(f"Procesadas correctamente: {procesadas}")
    print(f"Ya tenian cloud_RBIOS: {ya_procesadas}")
    print(f"No en MongoDB: {no_en_mongo}")
    print(f"Errores: {errores}")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
