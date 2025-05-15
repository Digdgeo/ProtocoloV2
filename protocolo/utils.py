import os
import smtplib
import pymongo
import psycopg2
from psycopg2 import sql
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

"""
Utility functions for the Landsat processing protocol.

This module provides a set of helper functions and tools used throughout the processing
pipeline. These include email notifications, hydroperiod preparation, PostgreSQL integration,
scene visualization, and flood mask processing.

The utilities are organized into the following categories:
- Email notifications (e.g., scene status with quicklook).
- Hydroperiod preparation (e.g., copying flood masks, filtering by cloud cover).
- PostgreSQL database creation and update for hydroperiod summaries.
- Plotting average flooded days over time for subregions.
- Visual composition of RGB scenes and flood masks.

Notes
-----
In future versions, some functions in this module will be moved into their corresponding
classes (e.g., `Hydroperiod`, `Mailing`) to improve modularity and maintainability.
"""

#############################################################################################################
####################                   MAILING (LAST-EBD STAFF)                          ####################        
#############################################################################################################

def enviar_correo(destinatarios, asunto, cuerpo, archivo_adjunto=None):

    """
    Sends an email to a list of recipients with optional attachment.

    This function uses the Gmail SMTP server to send plain-text emails to multiple recipients,
    with optional file attachments.

    Parameters
    ----------
    destinatarios : list of str
        List of recipient email addresses.
    asunto : str
        Subject of the email.
    cuerpo : str
        Plain-text body of the email.
    archivo_adjunto : str, optional
        Path to the file to attach (default is None).

    Raises
    ------
    Exception
        If the SMTP connection or sending process fails.
    """

    # Configura los detalles del servidor SMTP de Gmail y las credenciales
    servidor_smtp = 'smtp.gmail.com'
    puerto_smtp = 587
    correo_remitente = 'lastebd.protocolo.vlab@gmail.com'
    contraseña_remitente = '**** COP gia kIWI'

    # Crea el objeto de mensaje
    mensaje = MIMEMultipart()
    mensaje['From'] = correo_remitente
    mensaje['To'] = ", ".join(destinatarios)
    mensaje['Subject'] = asunto

    # Adjunta el cuerpo del mensaje
    mensaje.attach(MIMEText(cuerpo, 'plain'))

    # Adjuntar el archivo si se proporciona
    if archivo_adjunto:
        try:
            with open(archivo_adjunto, "rb") as adjunto:
                parte = MIMEBase('application', 'octet-stream')
                parte.set_payload(adjunto.read())
                encoders.encode_base64(parte)
                parte.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(archivo_adjunto)}")
                mensaje.attach(parte)
        except Exception as e:
            print(f"Error al adjuntar el archivo: {e}")
            return

    # Intenta enviar el correo
    try:
        servidor = smtplib.SMTP(servidor_smtp, puerto_smtp)
        servidor.starttls()  # Protocolo de cifrado
        servidor.login(correo_remitente, contraseña_remitente)
        texto = mensaje.as_string()
        servidor.sendmail(correo_remitente, destinatarios, texto)
        servidor.quit()
        print("Correo enviado exitosamente.")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")


def imprimir_csv_como_texto(path, titulo):
    
    """
    Prints the contents of a CSV file as plain text for inclusion in an email.

    Parameters
    ----------
    path : str
        Path to the CSV file.
    titulo : str
        Title to be displayed in the email section.

    Returns
    -------
    str
        Formatted text with the contents of the CSV or an error message if reading fails.
    """

    if not os.path.exists(path):
        return f"\n⚠️ {titulo} no disponible o no generado.\n"
    try:
        df = pd.read_csv(path)
        if df.empty:
            return f"\n⚠️ {titulo} está vacío.\n"
        texto = f"\n{titulo}:\n"
        texto += df.to_string(index=False)
        return texto
    except Exception as e:
        return f"\n⚠️ Error leyendo {titulo}: {str(e)}\n"


def enviar_notificacion_finalizada(info_escena, archivo_adjunto=None):

    """
    Sends a notification email summarizing the result of processing a Landsat scene.

    This function composes a formatted message based on the scene's metadata,
    including cloud coverage, normalized bands, and generated products.
    If the "Flood" product was created, it includes summaries from the corresponding
    CSV files for flooded marsh areas and lagoon statistics.

    The message is sent to a predefined list of recipients using `enviar_correo()`.
    An optional quicklook image can be attached.

    Parameters
    ----------
    info_escena : dict
        Dictionary containing metadata and processing results for the scene. Keys include:
        - 'escena' : str, scene ID.
        - 'nubes_escena' : float, cloud cover for the full scene (%).
        - 'nubes_land' : float, cloud cover over land (%).
        - 'nubes_Doñana' : float, cloud cover over Doñana (%).
        - 'bandas_normalizadas' : list of str, normalized bands (optional).
        - 'productos_generados' : list of str, generated products (optional).
    archivo_adjunto : str, optional
        Path to a quicklook image to be attached to the email (default is None).

    Notes
    -----
    This function does not raise exceptions but prints error messages if email
    sending or file reading fails.
    """

    destinatarios = [
        'some@gmail.com', 'random@hotmail.es', 'mails@yahoo.es',
        'go@latinmail.es', 'here@outlook.es'
    ]
    escena = info_escena.get("escena", "N/A")
    bandas = info_escena.get("bandas_normalizadas", [])
    productos = info_escena.get("productos_generados", [])

    asunto = f"✅ Escena {escena} procesada"

    cuerpo = f"""
    Hola equipo LAST,
    
    Soy el bot del Protocolo de Dieguito (de todos los bots, el más hasta las webs de la USGS 😩),
    y os escribo desde la máquina virtual cloudlast01 para informaros de una nueva escena procesada.
    
    📦 ESCENA PROCESADA: {escena}
    
    🛰️ Detalles:
    • Nubes escena total:    {info_escena.get('nubes_escena', 'N/A')}%
    • Nubes sobre tierra:    {info_escena.get('nubes_land', 'N/A')}%
    • Nubes en Doñana:       {info_escena.get('nubes_Doñana', 'N/A')}%
    
    ⚙️ Bandas normalizadas:
    {', '.join(bandas) if bandas else '❌ No se ha normalizado ninguna banda'}
    
    🧫 Productos generados:
    {', '.join(productos) if productos else '❌ No se ha generado ningún producto'}
    
    """
    
    if "Flood" in productos:
        ruta_base = os.path.join("/mnt/datos_last/pro", escena, escena)
        csv_inundada = os.path.join(ruta_base, f"{escena}_superficie_inundada.csv".lower())
        csv_lagunas = os.path.join(ruta_base, f"{escena}_resumen_lagunas.csv".lower())

        cuerpo += "\n🌊 Superficies inundadas:\n"
        cuerpo += imprimir_csv_como_texto(csv_inundada, "(CSV marisma)")

        cuerpo += "\n\n🌿 Información de lagunas:\n"
        cuerpo += imprimir_csv_como_texto(csv_lagunas, "(CSV lagunas)")

    cuerpo += f"""
    
    📎 Se adjunta el quicklook de la escena.
    
    Un saludo protocolario,
    — El bot 🤖
    
    Pd. *In loving memory of Isa and Ricardo, who left us for allegedly better jobs. We miss you anyway ❤️*
    """

    enviar_correo(destinatarios, asunto, cuerpo, archivo_adjunto)


#############################################################################################################
####################                   METADATOS (GEONETWORK)                            ####################        
#############################################################################################################

# Metadatos de la escena (Geonetwork)

def generar_metadatos_flood(self):

    """
    Generates the REMegafucking ISO 19139 metadata XML file for the flood mask product using a Jinja2 template.

    The function reads flood summary data for marshes and lagoons from CSV files and fills
    the corresponding fields in the metadata template. The output XML is saved in the product folder.

    Parameters
    ----------
    self : Product
        Instance of the Product class containing scene attributes and file paths.

    Returns
    -------
    None
    """
    
    print('**** Metadatos')
    csv_dir = os.path.join(self.pro_escena, self.escena)
    print('CSVs here:', csv_dir)

    # Read marsh data from SUPERFICIE_INUNDADA.csv
    csv_marismas = os.path.join(csv_dir, f"{self.escena}_superficie_inundada.csv".lower())
    print(csv_marismas)
    df_marismas = pd.read_csv(csv_marismas)
    fila_total = df_marismas[df_marismas["recinto"] == "Total"]

    if not fila_total.empty:
        sup_ha = round(float(fila_total["area_inundada"].values[0]), 1)
        sup_pct = round(float(fila_total["porcentaje_inundacion"].values[0]), 1)
    else:
        sup_ha = 0
        sup_pct = 0

    # Read lagoon data from RESUMEN_LAGUNAS.csv
    csv_lagunas = os.path.join(csv_dir, f"{self.escena}_resumen_lagunas.csv".lower())
    if os.path.exists(csv_lagunas):
        df_lag = pd.read_csv(csv_lagunas)
        n_lagunas = int(df_lag["numero_cuerpos_con_agua"].values[0])
        lagunas_ha = round(float(df_lag["superficie_total_inundada"].values[0]), 1)
        lagunas_pct = round(float(df_lag["porcentaje_inundacion"].values[0]), 1)
    else:
        n_lagunas = 0
        lagunas_ha = 0
        lagunas_pct = 0

    # Load and render XML template with context
    template_path = os.path.join(self.data, "flood_metadata_template_final.xml")
    with open(template_path, "r", encoding="utf-8") as f:
        template = Template(f.read())

    context = {
        "fecha_actual": date.today().isoformat(),
        "escena": self.escena,
        "fecha_escena": datetime.strptime(self.escena[:8], "%Y%m%d").date(),  # asumiendo que tienes self.fecha
        "abstracto": "Máscara de agua derivada de imágenes Landsat Collection 2 Nivel 2 (reflectividad en superficie), normalizadas con áreas pseudo invariantes. Valores: -9999=NoData, 0=Seco, 1=Inundado, 2=No válido (nubes/sombras/errores radiométricos).",
        "sup_marismas_ha": sup_ha,
        "sup_marismas_pct": sup_pct,
        "sup_lagunas_ha": lagunas_ha,
        "sup_lagunas_pct": lagunas_pct,
        "n_lagunas": n_lagunas,
        "nombre_archivo": f"{self.escena}_flood.tif"
    }

    
    xml_rendered = template.render(context)
    output_path = os.path.join(self.pro_escena, f"{self.escena}_flood_metadata.xml")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_rendered)


def extraer_uuid(xml_path):
    
    tree = ET.parse(xml_path)
    root = tree.getroot()
    ns = {'gmd': 'http://www.isotc211.org/2005/gmd', 'gco': 'http://www.isotc211.org/2005/gco'}
    uuid = root.find('.//gmd:fileIdentifier/gco:CharacterString', ns)
    return uuid.text if uuid is not None else None

def subir_xml_y_tif_a_geonetwork(xml_path, tif_path, username, password, server="https://goyas.csic.es/geonetwork"):
    
    """
    Uploads a metadata XML file and a GeoTIFF file as an attachment to the same record in GeoNetwork.

    Parameters
    ----------
    xml_path : str
        Path to the metadata XML file.
    tif_path : str
        Path to the GeoTIFF file (flood mask).
    username : str
        GeoNetwork username.
    password : str
        Password for the GeoNetwork user.
    server : str, optional
        Base URL of the GeoNetwork server (default is 'https://goyas.csic.es/geonetwork').

    Returns
    -------
    dict
        Dictionary with the keys 'status', 'uuid', and 'mensaje', where:
        - 'status' is either 'ok' or 'error'
        - 'uuid' is the UUID of the record (if successful)
        - 'mensaje' contains a status message or error details
    """
    
    session = requests.Session()
    login_url = f"{server}/srv/spa/info?type=me"
    login_response = session.post(login_url, auth=(username, password))
    xsrf_token = login_response.cookies.get("XSRF-TOKEN")

    if not xsrf_token:
        return {"status": "error", "uuid": None, "mensaje": "❌ No se pudo obtener el token XSRF."}

    headers = {
        "Accept": "application/json",
        "X-XSRF-TOKEN": xsrf_token
    }

    # UUID basado en el nombre del archivo (sin extensión)
    uuid = extraer_uuid(xml_path)
    if not uuid:
        return {"status": "error", "uuid": None, "mensaje": "❌ No se pudo extraer el UUID del XML."}


    # Comprobar si el UUID ya existe en GeoNetwork
    check_url = f"{server}/srv/api/records/{uuid}"
    check_response = session.get(check_url, headers=headers, auth=(username, password))

    if check_response.status_code == 200:
        print(f"ℹ️ El UUID {uuid} ya existe en GeoNetwork. No se subirá de nuevo el XML.")
    else:
        # Subir el XML si no existe
        upload_url = f"{server}/srv/api/records"
        with open(xml_path, "rb") as file:
            files = {"file": (os.path.basename(xml_path), file, "application/xml")}
            params = {"uuidProcessing": "OVERWRITE"}
            response = session.post(upload_url, headers=headers, files=files, auth=(username, password), params=params)

        if response.status_code != 201:
            return {
                "status": "error",
                "uuid": uuid,
                "mensaje": f"❌ Error al subir el XML: {response.status_code} - {response.text}"
            }

        print(f"✅ XML subido correctamente con UUID: {uuid}")

    # Subir el TIF como adjunto
    print(f"📎 Subiendo TIF al UUID: {uuid}")
    attach_url = f"{server}/srv/api/records/{uuid}/attachments"
    with open(tif_path, "rb") as file:
        files = {"file": (os.path.basename(tif_path), file)}
        attach_response = session.post(attach_url, headers=headers, files=files, auth=(username, password))

    print(f"📤 Respuesta del servidor al adjuntar TIF: {attach_response.status_code}")
    print(f"🧾 Contenido: {attach_response.text}")

    if attach_response.status_code in [200, 201]:
        return {
            "status": "ok",
            "uuid": uuid,
            "mensaje": "✅ XML (si era necesario) y TIFF subidos correctamente."
        }
    else:
        return {
            "status": "error",
            "uuid": uuid,
            "mensaje": f"⚠️ XML subido pero error al adjuntar TIF: {attach_response.status_code} - {attach_response.text}"
        }
 


# Hydroperiods 
#############################################################################################################
####################                   Hidroperiodo                                      ####################        
#############################################################################################################import os

import shutil
from datetime import datetime
from pymongo import MongoClient

# Conexión a la base de datos MongoDB
client = MongoClient()
database = client.Satelites
db = database.Landsat

# Nueva colección para hidroperiodo
db_hidroperiodo = database.Hidroperiodo

def prepare_hydrop(productos_dir, output_dir, ciclo_hidrologico, umbral_nubes):

    """
    Prepares flood masks for a hydrological cycle by filtering valid Landsat scenes.

    This function selects and copies flood mask files (`*_flood.tif`) from processed Landsat
    scenes that fall within a specified hydrological cycle and meet a maximum cloud cover
    threshold over the marshes. It also stores a summary of selected scenes in MongoDB
    under the collection `Hidroperiodo`.

    Parameters
    ----------
    productos_dir : str
        Path to the directory containing folders of processed Landsat scenes.
    output_dir : str
        Path where selected flood mask files will be copied.
    ciclo_hidrologico : str
        Hydrological cycle to process, in the format 'YYYY-YYYY'.
    umbral_nubes : float
        Maximum cloud cover percentage allowed over the marsh area.

    Notes
    -----
    - Assumes that scene folders are named starting with a date in the format YYYYMMDD.
    - Relies on the MongoDB collection `Satelites.Landsat` to retrieve cloud cover and flood data.
    - The flood masks are stored in a subdirectory named `hidroperiodo_YYYY-YYYY_XX` within `output_dir`.

    Raises
    ------
    Exception
        If any file operation or database query fails.
    """

    
    # Obtener el año inicial y final del ciclo hidrológico
    year_start = int(ciclo_hidrologico.split('-')[0])
    year_end = year_start + 1

    # Definir las fechas de inicio y fin del ciclo hidrológico
    start_date = datetime(year_start, 10, 1)
    end_date = datetime(year_end, 9, 30)

    # Crear el directorio de salida para el ciclo hidrológico si no existe
    ciclo_output_dir = os.path.join(output_dir, f"hidroperiodo_{ciclo_hidrologico}_{umbral_nubes}")
    os.makedirs(ciclo_output_dir, exist_ok=True)

    # Lista para almacenar los IDs de las escenas que cumplen con el umbral de nubes
    escenas_validas = []

    # Recorrer todas las carpetas en el directorio de productos
    for escena in os.listdir(productos_dir):
        escena_dir = os.path.join(productos_dir, escena)
        if os.path.isdir(escena_dir):
            # Extraer la fecha de la escena desde el nombre del archivo (asumiendo que sigue un formato específico)
            try:
                # Asumiendo que la fecha de la escena está en el formato YYYYMMDD en alguna parte del nombre del archivo
                date_str = escena.split('l')[0]
                escena_date = datetime.strptime(date_str, "%Y%m%d")
            except (IndexError, ValueError):
                print(f"El nombre de la escena '{escena}' no tiene una fecha válida.")
                continue

            # Verificar si la fecha de la escena está dentro del rango del ciclo hidrológico
            if start_date <= escena_date <= end_date:
                # Buscar la información de la escena en la base de datos
                escena_info = db.find_one({"_id": escena})
                if escena_info:
                    nubes_marismas = escena_info.get("Clouds", {}).get("cloud_PN", 0)

                    # Verificar si la cobertura de nubes está dentro del umbral permitido
                    if nubes_marismas <= umbral_nubes:
                        # Obtener la suma de hectáreas de inundación de los recintos de marisma
                        flood_data = escena_info.get("Productos", [])
                        suma_ha_inundacion = 0

                        for producto in flood_data:
                            if isinstance(producto, dict) and "Flood" in producto:
                                suma_ha_inundacion = sum(producto["Flood"].values())
                                break

                        # Buscar el archivo '_flood.tif' dentro de la carpeta de la escena
                        for archivo in os.listdir(escena_dir):
                            if archivo.endswith('_flood.tif'):
                                # Copiar el archivo al directorio del ciclo hidrológico
                                archivo_src = os.path.join(escena_dir, archivo)
                                archivo_dst = os.path.join(ciclo_output_dir, archivo)
                                shutil.copy2(archivo_src, archivo_dst)
                                print(f"Archivo '{archivo}' copiado a '{ciclo_output_dir}'")

                                # Añadir la escena con sus datos a la lista de escenas válidas
                                escenas_validas.append({
                                    "escena_id": escena,
                                    "nubes_marismas": nubes_marismas,
                                    "ha_inundacion": suma_ha_inundacion
                                })

    # Almacenar el ciclo hidrológico en la colección Hidroperiodo
    hidroperiodo_id = f"hidroperiodo_{ciclo_hidrologico}_{umbral_nubes}"
    db_hidroperiodo.update_one(
        {"_id": hidroperiodo_id},
        {"$set": {"escenas": escenas_validas}},
        upsert=True
    )

    print(f"Máscaras de inundación para el ciclo hidrológico {ciclo_hidrologico} han sido copiadas a '{ciclo_output_dir}'.")
    print(f"El ciclo hidrológico {ciclo_hidrologico} ha sido registrado en la colección 'Hidroperiodo' con {len(escenas_validas)} escenas.")


###########################################################################################################
# Calcular el número de días de inundación media para cada sub recinto de la marisma y mandarlo a PostgreSQL
###########################################################################################################

import os
import glob
import geopandas as gpd
import rasterio
import numpy as np
import psycopg2
from rasterio.mask import mask
from shapely.geometry import mapping

# Parámetros de conexión a PostgreSQL
db_params = {
    'host': 'x',
    'dbname': 'x',
    'user': 'x',
    'password': 'x'
}

# Cargar el shapefile con los subrecintos (nuevo shapefile)
zona_interes_recintos = gpd.read_file('/mnt/datos_last/v02/data/Recintos_Marisma.shp')

# Definir el directorio donde están los archivos GeoTIFF
directorio_rasters = '/mnt/datos_last/v02/hyd'

# Buscar los archivos TIFF que empiezan con "hydroperiod_nor" y terminan con ".tif"
archivos_tiff = glob.glob(os.path.join(directorio_rasters, '**', 'hydroperiod_nor*.tif'), recursive=True)

# Función para obtener el valor medio de días inundados dentro de un polígono
def obtener_media_raster(archivo_tiff, shapefile):

    """
    Calculates the average number of flooded days within a given shapefile area.

    This function clips a hydroperiod raster using the geometries of a shapefile
    and computes the mean of non-null values (ignoring NoData). It is typically
    used to extract the average flood duration for a subregion or marsh zone.

    Parameters
    ----------
    archivo_tiff : str
        Path to the hydroperiod raster file (e.g., `hydroperiod_nor_XXXX_XXXX.tif`).
    shapefile : geopandas.GeoDataFrame
        GeoDataFrame containing the polygon geometry used to clip the raster.

    Returns
    -------
    float
        Average number of flooded days within the polygon geometry.

    Raises
    ------
    RasterioIOError
        If the raster file cannot be opened or clipped.
    """


    with rasterio.open(archivo_tiff) as src:
        # Recortar el raster con la máscara del shapefile
        out_image, out_transform = mask(src, shapefile.geometry, crop=True)
        out_image = out_image[0]  # Tomar la primera banda del raster

        # Filtrar valores nulos (NoData)
        out_image = out_image[out_image != src.nodata]

        # Calcular el valor medio de inundación (días medios inundados)
        return np.mean(out_image)

# Crear la nueva tabla en PostgreSQL si no existe
def crear_tabla_postgresql():

    """
    Creates a PostgreSQL table to store average flooded days per subarea and cycle.

    This function connects to the configured PostgreSQL database and ensures that the
    table `hidroperiodo_medias_recintos` exists. If the table does not exist, it will be created
    with appropriate columns and a composite primary key.

    Table schema:
    - subrecinto (VARCHAR): Name of the subarea or marsh unit.
    - ciclo (VARCHAR): Hydrological cycle (e.g., '2022-2023').
    - valor_medio (DOUBLE PRECISION): Average number of flooded days.

    Raises
    ------
    Exception
        If the connection or table creation fails.
    """


    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hidroperiodo_medias_recintos (
                subrecinto VARCHAR,
                ciclo VARCHAR,
                valor_medio DOUBLE PRECISION,
                PRIMARY KEY (subrecinto, ciclo)
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error al crear la tabla en PostgreSQL: {e}")

# Insertar los datos en la tabla de PostgreSQL
def insertar_datos_postgresql(medias_recintos):

    """
    Inserts or updates average flooded days into PostgreSQL for each subarea and cycle.

    This function takes a nested dictionary with flood data and writes the values
    to the `hidroperiodo_medias_recintos` table. If an entry already exists for a given
    subarea and cycle, the value is updated.

    Parameters
    ----------
    medias_recintos : dict
        Dictionary of the form:
        {
            'Subrecinto1': {'2022-2023': 45.2, '2023-2024': 51.7},
            'Subrecinto2': {'2022-2023': 38.1, ...},
            ...
        }

    Raises
    ------
    Exception
        If the database connection or insertion fails.
    """


    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        for subrecinto, ciclos in medias_recintos.items():
            for ciclo, media in ciclos.items():
                # Asegúrate de convertir el valor de 'media' a float, si es de tipo numpy.float32
                media = float(media)
                cursor.execute("""
                    INSERT INTO hidroperiodo_medias_recintos (subrecinto, ciclo, valor_medio)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (subrecinto, ciclo) DO UPDATE
                    SET valor_medio = EXCLUDED.valor_medio
                """, (subrecinto, ciclo, media))
        conn.commit()
        cursor.close()
        conn.close()
        print("Datos guardados en la tabla 'hidroperiodo_medias_recintos'")
    except Exception as e:
        print(f"Error al insertar datos en PostgreSQL: {e}")

# Obtener los valores medios para cada subrecinto y ciclo
def obtener_valores_medios_recintos():

    """
    Computes average flooded days per subarea and hydrological cycle from hydroperiod rasters.

    This function iterates through all subareas defined in a shapefile and all hydroperiod raster files,
    calculating the average number of flooded days for each combination of subarea and cycle.

    Returns
    -------
    dict
        A nested dictionary structured as:
        {
            'Subrecinto1': {'2022-2023': 42.5, '2023-2024': 51.0},
            'Subrecinto2': {'2022-2023': 37.3, ...},
            ...
        }

    Raises
    ------
    Exception
        If a raster cannot be read or the shapefile geometry is invalid.
    """


    medias_recintos = {}

    for _, subrecinto in zona_interes_recintos.iterrows():
        # Recortar el shapefile para cada subrecinto
        shapefile_subrecinto = zona_interes_recintos[ zona_interes_recintos['Nombre'] == subrecinto['Nombre'] ]
        
        # Crear un diccionario para almacenar los valores por ciclo
        medias_recintos[subrecinto['Nombre']] = {}

        for archivo in archivos_tiff:
            # Extraer los dos años del nombre del archivo
            anio_inicio = archivo.split('_')[-2]  # Primer año
            anio_fin = archivo.split('_')[-1].split('.')[0]  # Segundo año
            ciclo = f"{anio_inicio}-{anio_fin}"  # Formato '1984-1985'

            # Obtener el valor medio para el subrecinto y ciclo
            media = obtener_media_raster(archivo, shapefile_subrecinto)
            
            # Guardar los resultados para ese subrecinto y ciclo
            medias_recintos[subrecinto['Nombre']][ciclo] = media

    return medias_recintos

# Ejecutar todo el proceso
def ejecutar_script():

    """
    Executes the full workflow to compute and store flood duration statistics in PostgreSQL.

    This function performs the following steps:
    1. Ensures the target PostgreSQL table exists.
    2. Calculates average flooded days for each subarea and hydrological cycle
    using hydroperiod rasters.
    3. Inserts or updates the values in the database.

    Returns
    -------
    None

    Raises
    ------
    Exception
        If any of the subprocesses (table creation, computation, or insertion) fail.
    """


    # Crear la tabla en PostgreSQL
    crear_tabla_postgresql()
    
    # Obtener los valores medios para cada subrecinto y ciclo
    medias_recintos = obtener_valores_medios_recintos()

    # Insertar los valores en PostgreSQL
    insertar_datos_postgresql(medias_recintos)

# Ejecutar el script
if __name__ == "__main__":
    ejecutar_script()


############################################################################################
# Plotear los datos de cada sub recinto
############################################################################################

import psycopg2
import matplotlib.pyplot as plt

# Parámetros de conexión a PostgreSQL
db_params = {
    'host': 'x',
    'dbname': 'x',
    'user': 'x',
    'password': 'x'
}

# Función para obtener los datos de días medios inundados desde PostgreSQL
def obtener_datos_dias_inundados():

    """
    Retrieves average flooded days per subarea and hydrological cycle from PostgreSQL.

    This function queries the table `hidroperiodo_medias_recintos` to retrieve time series
    data for each subarea (e.g., marsh unit), organizing it into a structured dictionary
    suitable for plotting or further analysis.

    Returns
    -------
    dict
        Dictionary with subarea names as keys. Each value is a dictionary with:
        {
            'ciclos': [list of hydrological cycles],
            'valores': [list of corresponding average flooded days]
        }

    Raises
    ------
    Exception
        If the database query fails or the connection cannot be established.
    """


    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Consultar los valores de días medios inundados por subrecinto y ciclo
        cursor.execute("""
            SELECT subrecinto, ciclo, valor_medio
            FROM hidroperiodo_medias_recintos
            ORDER BY subrecinto, ciclo
        """)

        # Obtener los resultados
        resultados = cursor.fetchall()
        
        # Organizar los datos por subrecinto
        datos_recintos = {}
        for subrecinto, ciclo, valor_medio in resultados:
            if subrecinto not in datos_recintos:
                datos_recintos[subrecinto] = {'ciclos': [], 'valores': []}
            datos_recintos[subrecinto]['ciclos'].append(ciclo)
            datos_recintos[subrecinto]['valores'].append(valor_medio)

        cursor.close()
        conn.close()
        return datos_recintos
    except Exception as e:
        print(f"Error al recuperar datos de PostgreSQL: {e}")
        return {}

# Función para graficar los días inundados por subrecinto
def graficar_dias_inundados(datos_recintos):

    """
    Plots the average number of flooded days per hydrological cycle for each subarea.

    This function creates a line plot for each subarea using the data retrieved
    from PostgreSQL. Each line shows the evolution of average flooded days over
    the different hydrological cycles.

    Parameters
    ----------
    datos_recintos : dict
        Dictionary structured as:
        {
            'Subrecinto1': {
                'ciclos': ['2020-2021', '2021-2022', ...],
                'valores': [45.2, 38.1, ...]
            },
            ...
        }

    Returns
    -------
    None

    Notes
    -----
    The function uses Matplotlib to display the graph. Axes are labeled, cycles
    are shown on the x-axis, and subareas are color-coded in the legend.
    """


    plt.figure(figsize=(10, 6))

    # Graficar los valores de días inundados para cada subrecinto
    for subrecinto, data in datos_recintos.items():
        plt.plot(data['ciclos'], data['valores'], marker='o', label=subrecinto)

    # Añadir título y etiquetas
    plt.title('Días medios inundados por ciclo para cada subrecinto')
    plt.xlabel('Ciclo (Años)')
    plt.ylabel('Días medios inundados')

    # Mostrar leyenda
    plt.legend(title='Subrecintos')

    # Mostrar la gráfica
    plt.grid(True)
    plt.xticks(rotation=45)  # Rotar las etiquetas del eje X para mejor visibilidad
    plt.tight_layout()  # Ajustar para evitar solapamientos
    plt.show()

# Ejecutar todo el proceso
def ejecutar_script():

    """
    Runs the complete plotting workflow for flooded days per subarea.

    This function:
    1. Connects to PostgreSQL and retrieves average flooded day data per subarea and cycle.
    2. If data is available, calls `graficar_dias_inundados` to plot the time series.

    Returns
    -------
    None

    Raises
    ------
    Exception
        If the database connection or data retrieval fails.
    """


    # Obtener los datos de días medios inundados desde PostgreSQL
    datos_recintos = obtener_datos_dias_inundados()
    
    if datos_recintos:
        # Graficar los días medios inundados
        graficar_dias_inundados(datos_recintos)

# Ejecutar el script
if __name__ == "__main__":
    ejecutar_script()


#################################################################################################
# Exportar los jpgs de la escena y la máscara de agua para Penelope
#################################################################################################

# utils.py

import geopandas as gpd
import rasterio
from rasterio.mask import mask
from matplotlib.patches import Patch
import numpy as np
import matplotlib.pyplot as plt


def add_north_arrow(ax, position=(0.1, 0.1), size=15, label="N"):

    """
    Adds a north arrow to a Matplotlib axis.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        The axis on which to draw the arrow.

    position : tuple of float, optional
        Coordinates in axes fraction where the arrow should be placed (default is (0.1, 0.1)).

    size : int, optional
        Font size of the "N" label (default is 15).

    label : str, optional
        Label to display as the direction arrow (default is "N").

    Returns
    -------
    None
    """


    ax.annotate(
        label,
        xy=(position[0], position[1] + 0.1),
        xytext=position,
        xycoords='axes fraction',
        textcoords='axes fraction',
        arrowprops=dict(facecolor='black', width=2, headwidth=8, headlength=10),
        fontsize=size,
        ha='center'
    )


def add_scale_bar(ax, transform, length=5000, location=(0.1, 0.05), height=0.005):

    """
    Adds a scale bar to a Matplotlib axis based on raster resolution.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        The axis on which to draw the scale bar.

    transform : affine.Affine
        Affine transformation of the raster, used to calculate pixel size.

    length : int, optional
        Length of the scale bar in meters (default is 5000).

    location : tuple of float, optional
        Position of the scale bar in axes fraction coordinates (default is (0.1, 0.05)).

    height : float, optional
        Height of the scale bar as a fraction of the axes (default is 0.005).

    Returns
    -------
    None
    """


    resolution_x = transform[0]
    relative_bar_length = length / (resolution_x * 1000)
    bar_x_start = location[0]
    bar_x_end = bar_x_start + relative_bar_length

    segment_width = relative_bar_length / 3
    bar_x_mid = bar_x_start + segment_width

    ax.add_patch(plt.Rectangle((bar_x_start, location[1] - height * 0.1), relative_bar_length, height * 1.2,
                                edgecolor="black", facecolor="none", linewidth=1.5, transform=ax.transAxes))
    ax.add_patch(plt.Rectangle((bar_x_start, location[1]), segment_width, height, color="black", transform=ax.transAxes))
    ax.add_patch(plt.Rectangle((bar_x_mid, location[1]), segment_width, height, color="white", transform=ax.transAxes))
    ax.add_patch(plt.Rectangle((bar_x_mid + segment_width, location[1]), segment_width, height, color="black", transform=ax.transAxes))

    ax.text(bar_x_start, location[1] - 0.02, "0", fontsize=10, ha="center", va="top", transform=ax.transAxes)
    ax.text(bar_x_end, location[1] - 0.02, f"{length} m", fontsize=10, ha="center", va="top", transform=ax.transAxes)


def add_legend(ax, legend_type="rgb", line_width=2):

    """
    Adds a custom legend to a Matplotlib plot depending on the visualization type.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        The axis to which the legend will be added.

    legend_type : str, optional
        Type of legend to display. Options:
        - "rgb": for RGB composites (default).
        - "flood": for flood masks (dry, flooded, no data).

    line_width : int, optional
        Width of the boundary lines used in the legend (default is 2).

    Returns
    -------
    None
    """


    legend_elements = []

    # Leyenda para la composición RGB
    if legend_type == "rgb":
        legend_elements.append(Patch(facecolor='none', edgecolor='green', label='Reserva de la Biosfera', linewidth=line_width))

    # Leyenda para la máscara de inundación
    elif legend_type == "flood":
        legend_elements.extend([
            Patch(facecolor='white', edgecolor='black', label='Seco', linewidth=line_width),
            Patch(facecolor='blue', edgecolor='black', label='Inundado', linewidth=line_width),
            Patch(facecolor='gray', edgecolor='black', label='No Data', linewidth=line_width),
            Patch(facecolor='none', edgecolor='green', label='Reserva de la Biosfera', linewidth=line_width)
        ])

    # Añadir la leyenda al gráfico
    ax.legend(handles=legend_elements, loc='lower left', fontsize=10, frameon=False, bbox_to_anchor=(0.1, 0.1))


def process_composition_rgb(swir1, nir, blue, shape, output_path):

    """
    Generates an RGB composite image using SWIR1, NIR, and Blue bands clipped to a shape.

    This function produces a color composite image from reflectance bands, clipped to the
    provided shapefile geometry. It adds a scale bar, north arrow, and legend, then saves
    the output to a file.

    Parameters
    ----------
    swir1 : str
        Path to the SWIR1 band raster file.

    nir : str
        Path to the NIR band raster file.

    blue : str
        Path to the Blue band raster file.

    shape : str
        Path to the shapefile used to crop the rasters.

    output_path : str
        Path where the output image (JPEG or PNG) will be saved.

    Returns
    -------
    None
    """


    with rasterio.open(swir1) as src_swir1, rasterio.open(nir) as src_nir, rasterio.open(blue) as src_blue:
        geometry = gpd.read_file(shape).geometry.values
        swir1, _ = mask(src_swir1, geometry, crop=True)
        nir, _ = mask(src_nir, geometry, crop=True)
        blue, transform = mask(src_blue, geometry, crop=True)

        # Escalar y combinar bandas
        swir1_scaled = np.clip((swir1[0] - 0) / (0.45 - 0), 0, 1)
        nir_scaled = np.clip((nir[0] - 0) / (0.45 - 0), 0, 1)
        blue_scaled = np.clip((blue[0] - 0) / (0.2 - 0), 0, 1)
        rgb_scaled = np.dstack((swir1_scaled, nir_scaled, blue_scaled))

        # Fondo blanco para NoData
        nodata_val = src_swir1.nodata or -9999
        rgb_scaled[np.all(swir1 == nodata_val, axis=0)] = [1, 1, 1]

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(rgb_scaled, extent=(transform[2],
                                  transform[2] + transform[0] * rgb_scaled.shape[1],
                                  transform[5] + transform[4] * rgb_scaled.shape[0],
                                  transform[5]),
              origin="upper")

    # Plot del shape con línea visible
    shapes = gpd.read_file(shape)
    shapes.boundary.plot(ax=ax, color='green', linewidth=3)  

    add_north_arrow(ax)
    add_scale_bar(ax, transform)
    add_legend(ax, legend_type="rgb")  # Leyenda específica para RGB
    ax.axis("off")
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    plt.savefig(output_path, dpi=300, bbox_inches="tight", pad_inches=0)
    plt.show()


def process_flood_mask(flood, shape, output_path):

    """
    Generates a visual flood mask representation clipped to a given shape.

    This function creates a PNG or JPEG image showing flood status using a
    symbolized color scheme. It includes cartographic elements such as a scale bar,
    north arrow, and a legend.

    Parameters
    ----------
    flood : str
        Path to the flood mask raster file. Expected values:
        - 0 = dry
        - 1 = flooded
        - 2 = no data

    shape : str
        Path to the shapefile used to crop the raster and overlay boundaries.

    output_path : str
        Path where the output image will be saved.

    Returns
    -------
    None
    """
    
    with rasterio.open(flood) as src:
        geometry = gpd.read_file(shape).geometry.values
        mask_data, transform = mask(src, geometry, crop=True)
        simbolizada = np.zeros((mask_data.shape[1], mask_data.shape[2], 4), dtype=np.uint8)
        simbolizada[mask_data[0] == 0] = [255, 255, 255, 255]
        simbolizada[mask_data[0] == 1] = [0, 0, 255, 255]
        simbolizada[mask_data[0] == 2] = [64, 64, 64, 255]

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(simbolizada, extent=(transform[2],
                                   transform[2] + transform[0] * simbolizada.shape[1],
                                   transform[5] + transform[4] * simbolizada.shape[0],
                                   transform[5]),
              origin="upper")

    # Plot del shape con línea visible
    shapes = gpd.read_file(shape)
    shapes.boundary.plot(ax=ax, color='green', linewidth=3)  

    add_north_arrow(ax)
    add_scale_bar(ax, transform)
    add_legend(ax, legend_type="flood")  # Leyenda específica para la máscara de agua
    ax.axis("off")
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    plt.savefig(output_path, dpi=300, bbox_inches="tight", pad_inches=0)
    plt.show()