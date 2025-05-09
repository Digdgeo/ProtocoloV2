import os
import sys
import tarfile
import datetime
import requests
from pymongo import MongoClient
from usgs import api

# Añadir ruta al código personalizado
sys.path.append('/root/git/ProtocoloV2/codigo')

from .protocolov2 import Landsat
from .productos import Product
from .coast import Coast
from .utils import enviar_correo, enviar_notificacion_finalizada

"""
Download and process Landsat Collection 2 scenes using the USGS API.

This script connects to the USGS EarthExplorer API to search for recent
Landsat Collection 2 Level-2 scenes over a given location, downloads the
product bundle, extracts the data, and runs the full processing chain using
the `Landsat`, `Product`, and `Coast` classes.

After processing, it sends an email notification with a quicklook image and
summarized metadata, including cloud and flood coverage.

Intended to be run periodically via cron or as a standalone script.

Dependencies
------------
- usgs
- pymongo
- requests
- tarfile
- protocolov2, productos, coast, utils (custom modules)
"""

# --- FUNCIÓN PARA LOGIN USGS CON LOGOUT AUTOMÁTICO ---
def get_usgs_api_key(usuario, password):

    """
    Log out any existing USGS session and obtain a new API key.

    Parameters
    ----------
    usuario : str
        USGS EarthExplorer username.

    password : str
        USGS EarthExplorer password.

    Returns
    -------
    str
        A valid API key to be used in subsequent USGS API requests.
    """
     
    try:
        api.logout()
    except Exception:
        pass  # Por si no hay sesión activa

    login_info = api.login(usuario, password)
    return login_info["data"]

# --- PARÁMETROS DE USUARIO ---
usuario = "USERNAME"  # Cambiar por el nombre de usuario real
password = "USERPASS"  # Cambiar por la contraseña real
api_key = get_usgs_api_key(usuario, password)

# --- CONEXIÓN BASE DE DATOS ---
client = MongoClient()
database = client.Satelites
db = database.Landsat

# --- FUNCIÓN PRINCIPAL DE DESCARGA ---
def download_landsat_scenes(latitude, longitude, days_back=15, end_date=None,
                             process=True, max_cloud_cover=100,
                             output_dir='/path/to/folder/landsat/v02/ori/rar'):
    
    """
    Search, download, and process new Landsat scenes from the USGS API.

    This function queries the USGS EarthExplorer API for recent Landsat Collection 2
    Level-2 scenes, downloads the product bundle, extracts the files, and launches
    the full processing workflow using the `Landsat`, `Product`, and `Coast` classes.

    Parameters
    ----------
    latitude : float
        Latitude of the center of the area of interest.

    longitude : float
        Longitude of the center of the area of interest.

    days_back : int, optional
        Number of days to look back from `end_date` (default is 15).

    end_date : str or None, optional
        End date for the search in ISO format (YYYY-MM-DD). Defaults to today.

    process : bool, optional
        Whether to run the processing workflow after download (default is True).

    max_cloud_cover : int, optional
        Maximum acceptable cloud cover (currently not used in filtering).

    output_dir : str, optional
        Path to save the downloaded `.tar` bundles.

    Notes
    -----
    - Only scenes with product type `L2SP` and Tier 1 (`T1`) are considered.
    - Scenes already present in MongoDB are skipped.
    - If no new scenes are found, an email is sent to notify the team.
    - If processing is successful, another email is sent with a quicklook image.

    Returns
    -------
    None
    """

    hoy = datetime.date.today() if end_date is None else datetime.date.fromisoformat(end_date)
    inicio = hoy - datetime.timedelta(days=days_back)

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

    escenas_nuevas = []
    for escena in escenas:
        display_id = escena['displayId']
        partes = display_id.split('_')
        if len(partes) >= 3 and partes[1] == 'L2SP' and partes[-1] == 'T1':
            if not db.find_one({'tier_id': display_id}):
                escenas_nuevas.append(escena)

    destinatarios = [
        'some@gmail.com', 'random@hotmail.es', 'mails@yahoo.es',
        'go@latinmail.es', 'here@outlook.es'
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
        quicklook = None  # <- para poder usarlo también si hay error

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
            quicklook = landsat.qk_name  # quicklook disponible incluso si falla .run()

            landsat.run()

            info_escena = {
                'escena': landsat.last_name,
                'nubes_escena': landsat.newesc['Clouds']['cloud_scene'],
                'nubes_land': landsat.newesc['Clouds']['land cloud cover'],
                'nubes_Doñana': landsat.pn_cover
            }

            print(f"🖼️ Quicklook generado: {quicklook}")

            landsatp = Product(landsat.nor_escena)
            landsatp.run()

            try:
                productos = db.find_one({'_id': landsat.last_name}, {'Productos': 1}).get("Productos", [])
                flood_data = next((prod["Flood"] for prod in productos if isinstance(prod, dict) and "Flood" in prod), None)
                if flood_data:
                    info_escena['flood_PN'] = flood_data
                    mail += 1
            except Exception as e:
                print(f"⚠️ No hay datos de inundación: {e}")

            enviar_notificacion_finalizada(info_escena, archivo_adjunto=quicklook, exito=(mail > 0))

        except Exception as e:
            print(f"❌ Error procesando escena {display_id}: {e}")
            enviar_notificacion_finalizada({"escena": display_id}, archivo_adjunto=quicklook, exito=False)


# --- LLAMADA PRINCIPAL ---
if __name__ == "__main__":
    download_landsat_scenes(
        latitude=37.05,
        longitude=-6.35,
        days_back=2
    )