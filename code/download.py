import os
import sys
import json
import arrow
import tarfile
import argparse

# Añadimos la ruta con el código a nuestro pythonpath para poder importar la clase Landsat
sys.path.append('/home/diego/git/ProtocoloV2/code')

from protocolov2 import Landsat
from productos import Product
from utils import *

from landsatxplore.api import API
from landsatxplore.earthexplorer import EarthExplorer
from pymongo import MongoClient

#Database part
client = MongoClient()
database = client.Satelites
db = database.Landsat

# Download function (at the end this should be done with argparse)
def download_landsat_scenes(username, password, latitude, longitude, days_back=15, end_date=None, process=True, max_cloud_cover=100, output_dir='/media/diego/Datos4/EBD/Protocolo_v2_2024/sr2/rar'):
    
    """Descarga y procesa escenas Landsat de EarthExplorer.

    Esta función busca escenas Landsat dentro de un rango de fechas y coordenadas proporcionadas,
    las descarga y, si se especifica, las procesa utilizando las clases `Landsat` y `Product`.

    Args:
        username (str): Nombre de usuario para autenticación en EarthExplorer.
        password (str): Contraseña para autenticación en EarthExplorer.
        latitude (float): Latitud de la ubicación de interés.
        longitude (float): Longitud de la ubicación de interés.
        days_back (int, optional): Número de días anteriores al `end_date` para buscar escenas. Por defecto es 15.
        end_date (str, optional): Fecha final para la búsqueda de escenas en formato 'YYYY-MM-DD'. Si no se especifica, se toma la fecha actual.
        process (bool, optional): Si es True, las escenas descargadas serán procesadas. Por defecto es True.
        max_cloud_cover (int, optional): Porcentaje máximo de cobertura nubosa permitido. Por defecto es 100.
        output_dir (str, optional): Directorio donde se guardarán las escenas descargadas. Por defecto es '/media/diego/Datos4/EBD/Protocolo_v2_2024/sr2/rar'.

    Returns:
        None
    """
    
    # Initialize EarthExplorer and API instances
    ee = EarthExplorer(username, password)
    api = API(username, password)
    
    # Set default end_date if not provided
    if end_date is None:
        end_date = arrow.now().format('YYYY-MM-DD')
    else:
        end_date = arrow.get(end_date).format('YYYY-MM-DD')
    
    # Calculate start_date based on end_date
    start_date = arrow.get(end_date).shift(days=-days_back).format('YYYY-MM-DD')

    # Search for Landsat scenes
    scenes = api.search(
        dataset='landsat_ot_c2_l2', #!!!!!!!!!!!!!!!!!!!Hay que añadir los otros datasets!!!!!!!!!!!
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
        max_cloud_cover=max_cloud_cover
    )

    print(f"{len(scenes)} scenes found.")

    # Process the result
    for scene in scenes:


        #print(scene)
        
        # Scene Id
        sc = scene['display_id']
        processing = sc.split('_')[1]
        
        print(sc, processing)

        if processing == 'L2SP':
        
        
            try:
    
                
                # Consulta en la base de datos
                result = db.find_one({'tier_id': sc})
                
                # Verificar el resultado de la consulta
                print(f"Resultado de la consulta para '{sc}': {result}")
    
                # Verifica si result es None
                if result:
                    print(f"La escena con ID {sc} ya está en la base de datos.")
                else:
                    print(f"La escena con ID {sc} no está en la base de datos.")
                    print(f"Downloading scene {sc}...")
    
            
                    
                    ee.download(sc, output_dir=output_dir)
    
                    # De aquí salta al error con lexplore :(
    
                    # Now as default we're going to process the new scene
                    if process == True:
                        # Check database to see if it's already done
                        print('Ahora habría que descomprimir y procesar la escena. La descarga fue ok')
    
                        # Sometimes works good?           
    
                        print('Vamos a descomprimir', sc)  
                        
                        sc_tar = os.path.join(output_dir, sc + '.tar')
                        sr2 = os.path.split(output_dir)[0]
                        
                        print('CHECKING!!', sr2, sc)
                        
                        sc_dest = os.path.join(sr2, sc)
        
                        os.makedirs(sc_dest, exist_ok=True)
                        
                        # En la función download_landsat_scenes
                        try:
                            
                            print(f"sc_tar: {sc_tar}")
                            print(f"sc_dest: {sc_dest}")
                            print(f"os.path.exists(sc_dest): {os.path.exists(sc_dest)}")
                                        
                            print('Extrayendo archivos a sr2')
                            with tarfile.open(sc_tar) as tar:
                                tar.extractall(sc_dest)
                                print(f"Archivos extraídos en {sc_dest}")
                            
                            # Once files are extracted in the correct folder we can proceed to run landsat class
                            print('\ncrossed fingers, we are going to start with Protocolo\n')
                            landsat = Landsat(sc_dest)
                            landsat.run()
        
                            # Now we have the scene processed and we are going to run the products (by the moment testing with NDVI)
                            landsatp = Product(landsat.nor_escena)
                            landsatp.run()
        
                            # Let's get water surfaces
                            # Verificar si el documento existe
                            # Consulta para recuperar el documento
                            escena_id = landsat.last_name
                            print(escena_id)
                            documento = db.find_one({"_id": escena_id})
        
                            if documento:
                                # Buscar el diccionario de "Flood" dentro de la lista "Productos"
                                for producto in documento.get("Productos", []):
                                    if isinstance(producto, dict) and "Flood" in producto:
                                        inundacion_data = producto["Flood"]
                                        break
                                else:
                                    inundacion_data = None  # Si no se encuentra "Flood"
                            
                                if inundacion_data:
                                    print(f"Datos de inundación para la escena {escena_id}:")
                                    print(inundacion_data)
                                    # Let's try to send an email
                                    print('Moving to mailing')
                                    info_escena = {'escena': landsat.last_name,
                                                   'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                                                   'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                                                   'nubes_Doñana': landsat.pn_cover,
                                                   'flood_PN': inundacion_data}
                    
                                    archivo_adjunto = landsat.qk_name
                                    proceso_finalizado(info_escena, archivo_adjunto)
                                    
                                else:
                                    print(f"No se encontraron datos de inundación para la escena {escena_id}.")
                            else:
                                print(f"No se encontró ningún documento con ID {escena_id}.")
                        
                                # Let's try to send an email
                                print('Moving to mailing')
                                info_escena = {'escena': landsat.last_name,
                                               'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                                               'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                                               'nubes_Doñana': landsat.pn_cover,
                                               'flood_PN': inundacion_data}
                
                                archivo_adjunto = landsat.qk_name
                                proceso_finalizado(info_escena, archivo_adjunto)
                                
                        except Exception as e:
                            print(f"Error extracting scene {sc}: {e}")     
                
                                # else:
                                #     continue
                
            
            except Exception as e:
                
                print(f"Error downloading scene {sc}: {e}")
    
                print('Estamos en la Excepción (de aquel que no tiene corazón... amos achooo')
                
                # Now as default we're going to process the new scene we have to make it in the exception part because lxplore throws an error no matter how
                if process == True:
    
                    print('Vamos a descomprimir', sc, 'estamos en el except')  
                    
                    sc_tar = os.path.join(output_dir, sc + '.tar')
                    sr2 = os.path.split(output_dir)[0]
                    
                    print('CHECKING!!', sr2, sc)
                    
                    sc_dest = os.path.join(sr2, sc)
    
                    os.makedirs(sc_dest, exist_ok=True)
                    
                    # En la función download_landsat_scenes
                    try:
                        
                        print(f"sc_tar: {sc_tar}")
                        print(f"sc_dest: {sc_dest}")
                        print(f"os.path.exists(sc_dest): {os.path.exists(sc_dest)}")
                                    
                        print('Extrayendo archivos a sr2')
                        with tarfile.open(sc_tar) as tar:
                            tar.extractall(sc_dest)
                            print(f"Archivos extraídos en {sc_dest}")
                        
                        # Once files are extracted in the correct folder we can proceed to run landsat class
                        print('\ncrossed fingers, we are going to start with Protocolo\n')
                        landsat = Landsat(sc_dest)
                        landsat.run()
    
                        # Now we have the scene processed and we are going to run the products (by the moment testing with NDVI)
                        landsatp = Product(landsat.nor_escena)
                        landsatp.run()
    
                        # Let's get water surfaces
                        # Verificar si el documento existe
                        # Consulta para recuperar el documento
                        escena_id = landsat.last_name
                        print(escena_id)
                        documento = db.find_one({"_id": escena_id})
    
                        if documento:
                            # Buscar el diccionario de "Flood" dentro de la lista "Productos"
                            for producto in documento.get("Productos", []):
                                if isinstance(producto, dict) and "Flood" in producto:
                                    inundacion_data = producto["Flood"]
                                    break
                            else:
                                inundacion_data = None  # Si no se encuentra "Flood"
                        
                            if inundacion_data:
                                print(f"Datos de inundación para la escena {escena_id}:")
                                print(inundacion_data)
                                # Let's try to send an email
                                print('Moving to mailing')
                                info_escena = {'escena': landsat.last_name,
                                               'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                                               'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                                               'nubes_Doñana': landsat.pn_cover,
                                               'flood_PN': inundacion_data}
                
                                archivo_adjunto = landsat.qk_name
                                proceso_finalizado(info_escena, archivo_adjunto)
                            else:
                                print(f"No se encontraron datos de inundación para la escena {escena_id}.")
                        else:
                            print(f"No se encontró ningún documento con ID {escena_id}.")
                        
                        # Let's try to send an email
                        print('Moving to mailing')
                        info_escena = {'escena': landsat.last_name,
                                       'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                                       'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                                       'nubes_Doñana': landsat.pn_cover,
                                       'flood_PN': inundacion_data}
    
                        archivo_adjunto = landsat.qk_name
                        proceso_finalizado(info_escena, archivo_adjunto)
                    
                    except Exception as e:
                        print(f"Error extracting scene {sc}: {e}")                
                    
                    # Check database to see if it's already done
                    #print('Ahora habría que procesar la escena desde la excepción (de aquel que no tiene corazón... ayyy compay')
                    #pass
        
    # Logout from EarthExplorer and API
    ee.logout()
    api.logout()

# Example usage
#download_landsat_scenes('user_name', 'user_pass', latitude=37.05, longitude=-6.35, end_date='2022-09-05', days_back=20, output_dir='/out/put/dir/to/save/scenes/')