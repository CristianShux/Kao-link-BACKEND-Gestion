import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
from crud.crudAdmintrador import AdminCRUD
import random
import string


load_dotenv()

EMAIL_ORIGEN="linkkaosor2@gmail.com"
EMAIL_PASSWORD="mwqu vspk fctl suwn"

def enviar_correo_generico(tipo, id_empleado, mensaje):
    empleado = AdminCRUD.obtener_empleado_por_id(id_empleado)
    if not empleado:
        raise ValueError("Empleado no encontrado")

    asunto = f'Notificación - {tipo.capitalize()}'

    msg = MIMEText(mensaje)
    msg['Subject'] = asunto
    msg['From'] = EMAIL_ORIGEN
    msg['To'] = empleado.correo

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(EMAIL_ORIGEN, EMAIL_PASSWORD)
        server.send_message(msg)


def generar_codigo_verificacion(longitud=6):
    """Genera un código de verificación alfanumérico."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=longitud))


def enviar_codigo_verificacion(nombre, correo, codigo):
    cuerpo = f"""
    Hola {nombre},

    Tu código de verificación es: {codigo}

    Por favor, ingresalo en el sistema para completar el registro.

    Saludos,  
    Equipo de Kao-Link
    """

    msg = MIMEText(cuerpo)
    msg['Subject'] = 'Código de Verificación - Kao-Link'
    msg['From'] = EMAIL_ORIGEN
    msg['To'] = correo

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(EMAIL_ORIGEN, EMAIL_PASSWORD)
        server.send_message(msg)


def enviar_correo_manual(correo, asunto, mensaje):
    msg = MIMEText(mensaje)
    msg['Subject'] = asunto
    msg['From'] = EMAIL_ORIGEN
    msg['To'] = correo

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(EMAIL_ORIGEN, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"Correo enviado exitosamente a {correo}")
    except Exception as e:
        print(f"Error al enviar correo: {e}")
