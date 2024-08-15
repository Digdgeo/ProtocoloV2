import json
import arrow
from landsatxplore.api import API
from landsatxplore.earthexplorer import EarthExplorer
from pymongo import MongoClient

#Database part
client = MongoClient()
database = client.Satelites
db = database.Landsat

# Download function (at the end this should be done with argparse)
def download_landsat_scenes(username, password, latitude, longitude, days_back=15, end_date=None, process=True, max_cloud_cover=50, output_dir='./'):

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
                pass
                
            else:
                print(f"La escena con ID {sc} no está en la base de datos.")
                print(f"Downloading scene {sc}...")
                ee.download(sc, output_dir=output_dir)

                # Now as default we're going to process the new scene
                if process == True:
                    # Check database to see if it's already done
                    print('Ahora habría que descomprimir y procesar la escena')
                    pass

                # else:
                #     continue
            
            
        except Exception as e:
            
            print(f"Error downloading scene {sc}: {e}")
            # Now as default we're going to process the new scene
            if process == True:
                # Check database to see if it's already done
                print('Ahora habría que descomprimir y procesar la escena desde la excepción (de aquel que no tiene corazón... ayyy compay')
                pass
    
    # Logout from EarthExplorer and API
    ee.logout()
    api.logout()

# Example usage
download_landsat_scenes('user_name', 'user_pass', latitude=37.05, longitude=-6.35, end_date='2022-07-05', days_back=40, output_dir='/out/put/dir/to/save/scenes/')