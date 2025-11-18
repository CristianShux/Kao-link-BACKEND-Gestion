import random
import string
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from crud.crudAdmintrador import AdminCRUD 

SENDGRID_API_KEY = "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" 

EMAIL_ORIGEN = "linkkaosor2@gmail.com" 
# --------------------------------------------------------

def _enviar_email_api(destinatario, asunto, contenido_html):
    """Función interna auxiliar para enviar mediante API"""
    message = Mail(
        from_email=EMAIL_ORIGEN,
        to_emails=destinatario,
        subject=asunto,
        html_content=contenido_html
    )
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"Correo enviado a {destinatario}. Status: {response.status_code}")
        return True
    except Exception as e:
        print(f"Error crítico enviando correo: {e}")
        return False

def enviar_correo_generico(tipo, id_empleado, mensaje):
    empleado = AdminCRUD.obtener_empleado_por_id(id_empleado)
    if not empleado:
        raise ValueError("Empleado no encontrado")

    asunto = f'Notificación - {tipo.capitalize()}'
    html_content = f"<p>{mensaje}</p>"
    
    _enviar_email_api(empleado.correo, asunto, html_content)

def generar_codigo_verificacion(longitud=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=longitud))

def enviar_codigo_verificacion(nombre, correo, codigo):
    cuerpo_html = f"""
    <div style="font-family: Arial, sans-serif;">
        <h2>Hola {nombre},</h2>
        <p>Tu código de verificación es:</p>
        <h1 style="color: #2c3e50;">{codigo}</h1>
        <p>Por favor, ingrésalo en el sistema para completar el registro.</p>
        <br>
        <p>Saludos,<br>Equipo de Kao-Link</p>
    </div>
    """
    
    asunto = 'Código de Verificación - Kao-Link'
    _enviar_email_api(correo, asunto, cuerpo_html)

def enviar_correo_manual(correo, asunto, mensaje):
    html_content = f"<p>{mensaje}</p>"
    _enviar_email_api(correo, asunto, html_content)
