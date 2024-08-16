import json
import arrow
import tarfile
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

        try:
            
            # Check database to see if the scene is already done
            result = db.find_one({'_id': sc})
        
            if result:
                print(f"La escena con ID {sc} ya está en la base de datos.")
                
            else:
                print(f"La escena con ID {sc} no está en la base de datos.")
                print(f"Downloading scene {sc}...")
                ee.download(sc, output_dir=output_dir)

                # De aquí salta al error con lexplore :(

                
                # Now as default we're going to process the new scene
                # Not sure if we need process flag...
                if process == True:
                    # Check database to see if it's already done
                    print('Ahora habría que descomprimir y procesar la escena')
            
        
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
                
                try:
                    # print(f"sc_tar: {sc_tar}")
                    # print(f"sc_dest: {sc_dest}")
                    # print(f"os.path.exists(sc_dest): {os.path.exists(sc_dest)}")
                                
                    # print('Extrayendo archivos a sr2')
                    with tarfile.open(sc_tar) as tar:
                        tar.extractall(sc_dest)
                        print(f"Archivos extraídos en {sc_dest}")
                    
                    # En lugar de borrar el tar al fial del proceso, quizás sería mejor dejarlo guardado 
                    # y que el codigo comeince borrando los tar existentes de la ultima vez 
                    # así se podrían comprobar si hubeira algun problema
                    # print('Borrando el .tar')
                    # os.remove(sc_tar)
                
                except Exception as e:
                    print(f"Error extracting scene {sc}: {e}")                
            
                
                # Check database to see if it's already done
                print('Ahora habría que procesar la escena desde la excepción (de aquel que no tiene corazón... ayyy compay')
                pass
    
    # Logout from EarthExplorer and API
    ee.logout()
    api.logout()

# Example usage
#download_landsat_scenes('user_name', 'user_pass', latitude=37.05, longitude=-6.35, end_date='2022-09-05', days_back=20, output_dir='./')