import json
import arrow
from landsatxplore.api import API
from landsatxplore.earthexplorer import EarthExplorer

def download_landsat_scenes(username, password, latitude, longitude, days_back=15, end_date=None, process=True, max_cloud_cover=10, output_dir='./'):

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
        dataset='landsat_ot_c2_l2',
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
        max_cloud_cover=max_cloud_cover
    )

    print(f"{len(scenes)} scenes found.")

    # Process the result
    for scene in scenes:
        print(scene)

        # Missing (To Do): Check in MongoDB if the scene is already done

        try:
            sc = scene['display_id']
            print(f"Downloading scene {sc}...")
            ee.download(sc, output_dir=output_dir)
            if process==True:
                print('Ahora habría que descomprimir y procesar la escena')
                pass
        except Exception as e:
            print(f"Error downloading scene {sc}: {e}")
    
    # Logout from EarthExplorer and API
    ee.logout()
    api.logout()

# Call the function
download_landsat_scenes('user_name', 'pass', latitude=37.05, longitude=-6.35, end_date='2022-02-24', days_back=30, output_dir='/media/diego/Datos4/EBD/Protocolo_v2_2024/sr2/rar/')