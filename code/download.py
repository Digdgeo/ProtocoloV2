import os
import sys
import json
import arrow
import tarfile

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
def download_landsat_scenes(username, password, latitude, longitude, days_back=15, end_date=None, process=True, max_cloud_cover=10, output_dir='/media/diego/Datos4/EBD/Protocolo_v2_2024/sr2/rar'):
    
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

        # Scene Id
        sc = scene['display_id']
        print('Escena encontrada:', sc)

        # Here you can add the code to check the database if the scene is already processed

        try:
            
            # Check database to see if the scene is already done
            result = db.find_one({'_id': sc})
        
            # Comprobar si el documento existe
            if result:
                print(f"La escena con ID {sc} ya está en la base de datos.")
                #pass
                
            else:
                print(f"La escena con ID {sc} no está en la base de datos.")
                print(f"Downloading scene {sc}...")
                ee.download(sc, output_dir=output_dir)

                # De aquí salta al error con lexplore :(

                
                # Now as default we're going to process the new scene
                if process == True:
                    # Check database to see if it's already done
                    print('Ahora habría que descomprimir y procesar la escena')
                    #pass

                # else:
                #     continue
            
            
        except Exception as e:
            
            print(f"Error downloading scene {sc}: {e}")
            
            # Now as default we're going to process the new scene we have to make it in the exception part because lxplore throws an error no matter how
            if process == True:

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
                    landsatp.ndvi()

                    # Let's try to send an email
                    print('Moving to mailing')
                    info_escena = {'escena': landsat.last_name,
                                   'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                                   'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                                   'flood_PN': 15214}

                    archivo_adjunto = '/media/diego/Datos4/EBD/Protocolo_v2_2024/sr2/LC08_L2SP_202034_20130622_20200912_02_T1/LC08_L2SP_202034_20130622_20200912_02_T1_Quicklook'
                    proceso_finalizado(info_escena, archivo_adjunto)
                
                except Exception as e:
                    print(f"Error extracting scene {sc}: {e}")                
                
                # Check database to see if it's already done
                print('Ahora habría que procesar la escena desde la excepción (de aquel que no tiene corazón... ayyy compay')
                pass
    
    # Logout from EarthExplorer and API
    ee.logout()
    api.logout()

# Example usage
#download_landsat_scenes('user_name', 'user_pass', latitude=37.05, longitude=-6.35, end_date='2022-09-05', days_back=20, output_dir='/out/put/dir/to/save/scenes/')