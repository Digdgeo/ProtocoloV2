import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os


# Mails
def enviar_correo(destinatarios, asunto, cuerpo, archivo_adjunto=None):

    """Envía un correo electrónico con o sin archivo adjunto a los destinatarios especificados.

    Args:
        destinatarios (list): Lista de correos electrónicos de los destinatarios.
        asunto (str): Asunto del correo electrónico.
        cuerpo (str): Cuerpo del mensaje del correo electrónico.
        archivo_adjunto (str, optional): Ruta al archivo adjunto. Por defecto es None.
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
def proceso_finalizado(info_escena, archivo_adjunto=None):

    """Envía un correo electrónico notificando la finalización del procesamiento de una escena Landsat.

    Args:
        info_escena (dict): Diccionario con información relevante de la escena procesada (nubes, inundación, etc.).
        archivo_adjunto (str, optional): Ruta al archivo adjunto (ej. quicklook de la escena). Por defecto es None.
    """

    destinatarios = ['digd.geografo@gmail.com', 
    'jbustamante@ebd.csic.es', 
    'rdiaz@ebd.csic.es', 
    'daragones@ebd.csic.es',
    'isabelafan@ebd.csic.es',
    'gabrielap.romero@ebd.csic.es']
    asunto = 'Nueva escena Landsat procesada'
    cuerpo = f"""
    Hola equipo LAST,

    Soy el bot del Protocolo de Dieguito (de todos los bots, sin duda el más bonito). 
    
    Este es un mail automático 
    para informaros de que la escena {info_escena['escena']} ha sido procesada exitosamente. 
    
    Detalles de la escena:

    - Nubes escena: {info_escena['nubes_escena']}
    - Nubes escena tierra: {info_escena['nubes_land']}
    - Nubes Parque nacional: {info_escena['nubes_Doñana']}
    - Inundación Marisma (ha): {info_escena['flood_PN']}
    
    Seguiremos informando,
    Saludos

    Pd. Se adjunta quicklook de la escena.
    """

    # Enviar el correo
    enviar_correo(destinatarios, asunto, cuerpo, archivo_adjunto)


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

    """Prepara los datos del hidroperiodo copiando las máscaras de inundación válidas para un ciclo hidrológico.

    La función filtra las escenas de acuerdo a su cobertura de nubes y dentro del rango del ciclo hidrológico,
    y luego copia las máscaras de inundación (_flood.tif) al directorio de salida. Además, se almacena el ciclo
    hidrológico y sus escenas en la base de datos MongoDB.

    Args:
        productos_dir (str): Ruta al directorio que contiene las escenas procesadas.
        output_dir (str): Ruta al directorio donde se copiarán las máscaras de inundación.
        ciclo_hidrologico (str): Ciclo hidrológico en formato 'YYYY-YYYY'.
        umbral_nubes (float): Porcentaje máximo de nubes permitido en las escenas de marismas.
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
