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

# Mails
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
    contraseña_remitente = 'axyr tkwi fvkv kkfd'

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

# Mails
def enviar_notificacion_finalizada(info_escena, archivo_adjunto=None, exito=True):

    """
    Sends a summary notification email after processing a Landsat scene.

    This function constructs a formatted message based on the scene's metadata,
    including cloud coverage and flood statistics over marsh zones and lagoons.
    It then sends the message (with optional image attachment) to a predefined list
    of recipients using `enviar_correo()`.

    Parameters
    ----------
    info_escena : dict
        Dictionary containing metadata and analysis results for the scene. Keys include:
        - 'escena' : str, scene ID.
        - 'nubes_escena' : float, cloud cover over the full scene (%).
        - 'nubes_land' : float, cloud cover over land (%).
        - 'nubes_Doñana' : float, cloud cover over Doñana National Park (%).
        - 'flood_PN' : dict, flooded area per marsh zone in hectares (optional).
        - 'lagunas' : dict, summary stats on water presence in lagoons (optional).
    archivo_adjunto : str, optional
        Path to a quicklook image (PNG or JPG) to attach to the message (default is None).
    exito : bool, optional
        Whether the processing was successful and normalization was completed (default is True).

    Raises
    ------
    Exception
        If the email could not be sent.
    """

    destinatarios = [
        'digd.geografo@gmail.com', 'diegogarcia@ebd.csic.es', 
        'jbustamante@ebd.csic.es', 'rdiaz@ebd.csic.es', 
        'isabelafan@ebd.csic.es', 'daragones@ebd.csic.es', 
        'gabrielap.romero@ebd.csic.es'
    ]
    asunto = 'Nueva escena Landsat procesada' if exito else 'Nueva Escena Landsat procesada sin normalizar'
    estado = "procesada exitosamente" if exito else "procesada, pero sin poder normalizarse"

    # Datos del área total de cada recinto en hectáreas
    area_total = {
        'El Rincon del Pescador': 3499.3,
        'Marismillas': 3861.1,
        'Caracoles': 2718.9,
        'FAO': 64.8,
        'Marisma Occidental': 11668.9,
        'Marisma Oriental': 9575.1,
        'Entremuros': 2617.4
    }
    
    # Construye el cuerpo del correo
    cuerpo = f"""
    Hola equipo LAST,

    Soy el bot del Protocolo de Dieguito, de todos los bots el que más está hasta los... ¿circuitos internos? de la USGS
    y sus cambios en las políticas de acceso y tipos de procesado.

    Este es un mail automático enviado desde la máquina virtual cloudlast01
    para informaros de que la escena {info_escena.get('escena', 'N/A')} ha sido {estado}. 

    Detalles de la escena:

    - Nubes escena: {info_escena.get('nubes_escena', 'N/A')}
    - Nubes escena tierra: {info_escena.get('nubes_land', 'N/A')}
    - Nubes Parque nacional: {info_escena.get('nubes_Doñana', 'N/A')}

    Superficies inundadas:
    """
    
    # Agregar los valores de 'flood_PN' y calcular el porcentaje de inundación
    flood_pn = info_escena.get('flood_PN', {})
    if flood_pn and exito:
        for area, superficie_inundada in flood_pn.items():
            if area in area_total:
                porcentaje_inundado = (superficie_inundada / area_total[area]) * 100
                cuerpo += f"    - {area}: {superficie_inundada:.2f} ha ({porcentaje_inundado:.2f}% inundado)\n"
            else:
                cuerpo += f"    - {area}: {superficie_inundada:.2f} ha (Área total no disponible para cálculo de porcentaje)\n"
    else:
        cuerpo += "    - No se encontraron datos de inundación.\n"

    # Información de las lagunas
    lagunas = info_escena.get("lagunas", {})
    if exito and lagunas:
        numero_cuerpos = lagunas.get("numero_lagunas_con_agua", "N/A")
        porcentaje_cuerpos = lagunas.get("porcentaje_cuerpos_con_agua", "N/A")
        superficie_inundada = lagunas.get("superficie_total_inundada", "N/A")
        porcentaje_inundado = lagunas.get("porcentaje_inundado", "N/A")
        cuerpo += f"""
    Información de las lagunas:
    - Número de lagunas con agua: {numero_cuerpos}
    - Porcentaje de lagunas con agua respecto al total: {porcentaje_cuerpos if porcentaje_cuerpos != "N/A" else "N/A"}%
    - Superficie total inundada: {superficie_inundada if superficie_inundada != "N/A" else "N/A"} ha
    - Porcentaje de inundación respecto al total teórico: {porcentaje_inundado if porcentaje_inundado != "N/A" else "N/A"}%
        """
    else:
        cuerpo += "\n    Información de las lagunas no disponible o no procesada.\n"
                
    # Mensaje de cierre
    cuerpo += "\nSaludos\n\nPd. Se adjunta quicklook de la escena si está disponible."

    # Enviar el correo
    try:
        enviar_correo(destinatarios, asunto, cuerpo, archivo_adjunto)
        print("Correo enviado exitosamente.")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")



 
# Hydroperiods 
import os
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