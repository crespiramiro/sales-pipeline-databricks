import requests
from dotenv import load_dotenv
import os
from utils.logger import setup_logger

logger = setup_logger()
load_dotenv()

# Configuración de Resend (servicio gratuito de emails)
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM")

def send_alert(email_to, subject, body, email_from=EMAIL_FROM):
    """
    Envía alertas por email usando Resend (servicio gratuito)
    """
    try:
        if not RESEND_API_KEY:
            logger.error("❌ RESEND_API_KEY no configurada en .env")
            return False
            
        # URL de la API de Resend
        url = "https://api.resend.com/emails"
        
        # Headers para la API
        headers = {
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Payload del email
        payload = {
            "from": email_from,
            "to": [email_to],
            "subject": subject,
            "html": f"""
            <html>
            <body>
                <h2>{subject}</h2>
                <p>{body.replace(chr(10), '<br>')}</p>
                <hr>
                <p><small>Enviado automáticamente por el sistema ETL</small></p>
            </body>
            </html>
            """
        }
        
        # Enviar email
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            logger.info(f"✅ Alerta enviada exitosamente a {email_to}")
            return True
        else:
            logger.error(f"❌ Error enviando email: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ No se pudo enviar la alerta por mail: {e}")
        return False

def send_alert_simple(email_to, subject, body):
    """
    Función simple para compatibilidad con código existente
    """
    return send_alert(email_to, subject, body)