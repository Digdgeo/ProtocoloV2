import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

def enviar_correo(destinatarios, asunto, cuerpo, archivo_adjunto=None):
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

# Supongamos que el proceso de normalización ha terminado y tienes la información de la escena
def proceso_finalizado(info_escena, archivo_adjunto=None):
    destinatarios = ['digd.geografo@gmail.com']
    asunto = 'Nueva escena Landsat procesada'
    cuerpo = f"""
    Hola equipo,

    Soy el bot del Protocolo de Dieguito (de todos los bots, sin duda el más bonito). Este es un mail automático 
    para informaros de que la escena {info_escena['escena']} ha sido procesada exitosamente. 
    
    Detalles de la escena:

    - Nubes escena: {info_escena['nubes_escena']}
    - Nubes escena tierra: {info_escena['nubes_land']}
    - Inundación Marisma: {info_escena['flood_PN']}
    
    Seguiremos informando,
    Saludos

    Pd. Se adjunta quicklook de la escena.
    """

    # Enviar el correo
    enviar_correo(destinatarios, asunto, cuerpo, archivo_adjunto)