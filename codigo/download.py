import os
import sys
import json
import arrow
import tarfile
import argparse

# Añadimos la ruta con el código a nuestro pythonpath para poder importar la clase Landsat
sys.path.append('/root/git/ProtocoloV2/codigo')

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
        dataset='landsat_etm_c2_l2', #!!!!!!!!!!!!!!!!!!!Hay que añadir los otros datasets!!!!!!!!!!! ot etm tm
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
        max_cloud_cover=max_cloud_cover
    )

    print(f"{len(scenes)} scenes found.")

    # Comprobar si hay escenas nuevas
    escenas_nuevas = []
    for scene in scenes:
        sc = scene['display_id']
        result = db.find_one({'tier_id': sc})  # Consulta en la base de datos
        if not result:  # Si no está en la base de datos, es nueva
            escenas_nuevas.append(scene)

    # Si no hay escenas nuevas, enviar un correo y salir
    destinatarios = ['digd.geografo@gmail.com', 'diegogarcia@ebd.csic.es', 'jbustamante@ebd.csic.es', 
                     'rdiaz@ebd.csic.es', 'isabelafan@ebd.csic.es', 'daragones@ebd.csic.es', 'gabrielap.romero@ebd.csic.es']
    if not escenas_nuevas:
        asunto = "No hay nuevas escenas disponibles en la USGS"
        mensaje = (
            "Hola Equipo LAST,\n\n"
            "No se han encontrado nuevas escenas disponibles para procesar. "
            "Parece que la USGS está tardando más de lo habitual en actualizar los datos.\n\n"
            "...O quizás Biden, Trump, Putin, Netanyahu (él más probable) o la madre que parió al cordero y a todos juntos a la vez hayan empezado ya la III WW.\n"
            "Otra probabilidad es que Elon Musk haya tomado el mando del programa Landsat y ahora haya que tener una suscripción premiun plus pagando por píxeles...\n\n"
            "En fin, seguiremos (si podemos) informando las próximas semanas.\n\n"
            "Saludos cordiales,\n"
            "El bot del Protocolo del Dieguito 🤖"
        )
        enviar_correo(
            destinatarios=destinatarios,
            asunto=asunto,
            cuerpo=mensaje
            #exito=False
        )
        print("No hay escenas nuevas disponibles. Correo enviado.")
        return  # Salir de la función

    # Process the result    
    for scene in scenes:

        # Let's try to solve the duplicat emails with this dogdy approach
        mail = 0
        
        #print(scene)
        
        # Scene Id
        sc = scene['display_id']
        processing = sc.split('_')[1]
        tier = sc.split('_')[-1]
        
        print(sc, processing, tier)

        if processing == 'L2SP' and tier == 'T1':
        
            try:
                # Consulta en la base de datos usando `tier_id`
                result = db.find_one({'tier_id': sc})
    
                # Verificar si la escena ya está en la base de datos
                if result:
                    print(f"La escena con tier_id {sc} ya está en la base de datos.")
                else:
                    print(f"La escena con tier_id {sc} no está en la base de datos.")
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

                        quicklook = landsat.qk_name
                        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!', quicklook, '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                        # Aqui rellenamos info_escena para obtener los datos que se enviaran en el mail
                        # Actualizar `info_escena` con datos de `landsat.newesc`
                        info_escena = {
                            'escena': landsat.last_name,
                            'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                            'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                            'nubes_Doñana': landsat.pn_cover
                        }

                        print('INFO ESCENAAAAAAAAAAAAAAAAAAA: ', info_escena)
    
                        # Now we have the scene processed and we are going to run the products (by the moment testing with NDVI)
                        landsatp = Product(landsat.nor_escena)
                        landsatp.run()

                        try:
                            # Obtener los productos de 'landsatp' desde MongoDB después de ejecutar 'run'
                            landsatp.productos = db.find_one({'_id': landsat.last_name}, {'Productos': 1}).get("Productos", [])
                        
                            # Iterar a través de los productos para encontrar 'Flood'
                            flood_data = next((prod["Flood"] for prod in landsatp.productos if isinstance(prod, dict) and "Flood" in prod), None)
                        
                            if flood_data:
                                info_escena['flood_PN'] = flood_data
                                print('INFO ESCENAAAAAAAAAAAAAAAAAAA en try: ', info_escena)
                                mail += 1
                            else:
                                print("No hay datos de inundación")
                                #mail -= 1
                        
                        except Exception as e:
                            print('No hay datos de inundación', e)
                            continue
    
                        # Let's get water surfaces
                        # Verificar si el documento existe
                        # Consulta para recuperar el documento
                        
                        #documento = db.find_one({"_id": escena_id})
                            
                    except Exception as e:
                        print(f"Error extracting scene {sc}: {e}")     
            
                        
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

                        quicklook = landsat.qk_name
                        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!', quicklook, '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                        # Aqui rellenamos info_escena para obtener los datos que se enviaran en el mail
                        # Actualizar `info_escena` con datos de `landsat.newesc`
                        info_escena = {
                            'escena': landsat.last_name,
                            'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                            'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                            'nubes_Doñana': landsat.pn_cover
                        }

                        print('INFO ESCENAAAAAAAAAAAAAAAAAAA EN EXCEPT: ', info_escena)
    
                        # Now we have the scene processed and we are going to run the products (by the moment testing with NDVI)
                        landsatp = Product(landsat.nor_escena)
                        landsatp.run()

                        try:
                            # Obtener los productos de 'landsatp' desde MongoDB después de ejecutar 'run'
                            landsatp.productos = db.find_one({'_id': landsat.last_name}, {'Productos': 1}).get("Productos", [])
                        
                            # Iterar a través de los productos para encontrar 'Flood'
                            flood_data = next((prod["Flood"] for prod in landsatp.productos if isinstance(prod, dict) and "Flood" in prod), None)
                        
                            if flood_data:
                                info_escena['flood_PN'] = flood_data
                                print('INFO ESCENAAAAAAAAAAAAAAAAAAA en try: ', info_escena)
                                mail += 1
                            else:
                                print("No hay datos de inundación")
                                #mail -= 1
                        
                        except Exception as e:
                            print('No hay datos de inundación', e)
                            continue
                            
                    except Exception as e:
                        print(f"Error extracting scene {sc}: {e}")  
                                       
    
                        # En lugar de mandar el mail simplemnte sumamos uno al contador mail
                        #archivo_adjunto = landsat.qk_name
                        #proceso_finalizado(info_escena, archivo_adjunto)
                        #mail += 1
                    
                    except Exception as e:
                        print(f"Error extracting scene {sc}: {e}")                


        try:
            archivo_adjunto = quicklook
            print('QUICKLOOK!!!!!!!!!!!!!!!!', quicklook)
            print('Archivo adjuntoOOOOOOOOOOOOOOOOOOOOOOOOOOOO', archivo_adjunto)
            print(type(archivo_adjunto))

            # Enviamos el correo basándonos en el valor de `mail`
            if mail > 0:
                enviar_notificacion_finalizada(info_escena, archivo_adjunto, exito=True)  # Correo de éxito
            else:
                enviar_notificacion_finalizada(info_escena, archivo_adjunto, exito=False)  # Correo de fallo
            

        except Exception as e:
            print(f"Error processing scene {sc}: {e}")
                       
    
            
    #Logout from EarthExplorer and API
    ee.logout()
    api.logout()

# Example usage
#download_landsat_scenes('user_name', 'user_pass', latitude=37.05, longitude=-6.35, end_date='2022-09-05', days_back=20, output_dir='/out/put/dir/to/save/scenes/')