import os
import sys
import tarfile
import datetime
import requests

from pymongo import MongoClient
from usgs import api

# Añadimos la ruta con el código a nuestro pythonpath para poder importar la clase Landsat
sys.path.append('/root/git/ProtocoloV2/codigo')

from protocolov2 import Landsat
from productos import Product
from coast import Coast
from utils import enviar_correo, enviar_notificacion_finalizada

# Database connection
client = MongoClient()
database = client.Satelites
db = database.Landsat

# Parámetros generales
usuario = "user"
token = "uyuyuyuyuyuyuyuyuyuyuyuyuyuyuyuyuyuyuyuyqueseve"  # Sustituye por tu token real
api_key_full = api.login(usuario, token)
api_key = api_key_full["data"]

def download_landsat_scenes(latitude, longitude, days_back=15, end_date=None,
                             process=True, max_cloud_cover=100,
                             output_dir='/path/to/ori/rar'):

    hoy = datetime.date.today() if end_date is None else datetime.date.fromisoformat(end_date)
    inicio = hoy - datetime.timedelta(days=days_back)

    # Buscar escenas
    response = api.scene_search(
        dataset="landsat_ot_c2_l2",
        lat=latitude,
        lng=longitude,
        distance=5000,
        start_date=str(inicio),
        end_date=str(hoy),
        max_results=20,
        api_key=api_key
    )

    escenas = response.get("data", {}).get("results", [])
    print(f"\n🔍 Se encontraron {len(escenas)} escenas")

    # Filtrar por nuevas y tipo L2SP / T1
    escenas_nuevas = []
    for escena in escenas:
        display_id = escena['displayId']
        partes = display_id.split('_')
        if len(partes) >= 3 and partes[1] == 'L2SP' and partes[-1] == 'T1':
            if not db.find_one({'tier_id': display_id}):
                escenas_nuevas.append(escena)

    destinatarios = [
        'digd.geografo@gmail.com', 'diegogarcia@ebd.csic.es', 'jbustamante@ebd.csic.es',
        'rdiaz@ebd.csic.es', 'isabelafan@ebd.csic.es', 'daragones@ebd.csic.es', 'gabrielap.romero@ebd.csic.es'
    ]

    if not escenas_nuevas:
        enviar_correo(
            destinatarios,
            "No hay nuevas escenas disponibles en la USGS",
            "Hola Equipo LAST,\n\nNo se han encontrado nuevas escenas para procesar.\n\nSaludos del bot 🤖"
        )
        return

    for escena in escenas_nuevas:
        display_id = escena["displayId"]
        entity_id = escena["entityId"]
        mail = 0

        print(f"\n🚀 Procesando escena: {display_id}")

        opciones = api.download_options(
            dataset="landsat_ot_c2_l2",
            entity_ids=[entity_id],
            api_key=api_key
        )

        producto = next(
            (p for p in opciones["data"] if p.get("productName") == "Landsat Collection 2 Level-2 Product Bundle" and p.get("available")),
            None
        )

        if not producto:
            print(f"❌ No se puede descargar {display_id}")
            continue

        download_info = api.download_request(
            dataset="landsat_ot_c2_l2",
            entity_id=entity_id,
            product_id=producto["id"],
            api_key=api_key
        )

        available = download_info.get("data", {}).get("availableDownloads", [])
        if not available:
            print(f"⚠️ No hay descargas disponibles para {display_id}")
            continue

        url = available[0].get("url")
        nombre_archivo = f"{display_id}.tar"
        ruta_tar = os.path.join(output_dir, nombre_archivo)

        print(f"⬇️ Descargando {nombre_archivo}...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(ruta_tar, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        print(f"✅ Descargado: {ruta_tar}")

        sr2 = os.path.split(output_dir)[0]
        sc_dest = os.path.join(sr2, display_id)
        os.makedirs(sc_dest, exist_ok=True)

        try:
            print(f"📦 Extrayendo {nombre_archivo} a {sc_dest}")
            with tarfile.open(ruta_tar) as tar:
                tar.extractall(sc_dest)

            landsat = Landsat(sc_dest)
            landsat.run()

            info_escena = {
                'escena': landsat.last_name,
                'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                'nubes_Doñana': landsat.pn_cover
            }

            quicklook = landsat.qk_name
            print(f"🖼️ Quicklook generado: {quicklook}")

            landsatp = Product(landsat.nor_escena)
            landsatp.run()

            landsatc = Coast(landsat.pro_escena)
            landsatc.run()

            try:
                productos = db.find_one({'_id': landsat.last_name}, {'Productos': 1}).get("Productos", [])
                flood_data = next((prod["Flood"] for prod in productos if isinstance(prod, dict) and "Flood" in prod), None)
                if flood_data:
                    info_escena['flood_PN'] = flood_data
                    mail += 1
            except Exception as e:
                print(f"⚠️ No hay datos de inundación: {e}")

            enviar_notificacion_finalizada(info_escena, quicklook, exito=(mail > 0))

        except Exception as e:
            print(f"❌ Error procesando escena {display_id}: {e}")

# Ejemplo de uso
if __name__ == "__main__":
    download_landsat_scenes(
        latitude=37.05,
        longitude=-6.35,
        end_date="2025-04-01",
        days_back=15
    )