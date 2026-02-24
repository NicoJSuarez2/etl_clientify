#%%
import requests
import os
import dotenv
import sys

dotenv.load_dotenv()


def enviar_alerta(mensaje: str, works: bool = True):
    """Función para enviar alertas a Telegram."""
    etl = "ETL Clientify"
    if works:
        mensaje = f"✅ {etl}: {mensaje}, todo good todo nice  😮‍💨🤖"
        imagen = "assets/images/200.png"
    else:
        mensaje = f"❌ {etl}: {mensaje}, algo salió mal 😢🤖"
        imagen = "assets/images/400.png"

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    
    payload = {
        "chat_id": CHAT_ID,
        "caption": mensaje
    }
    
    with open(imagen, 'rb') as photo:
        files = {'photo': photo}
        requests.post(url, data=payload, files=files)
    


def test_api(logger):
    """Test the API endpoint to ensure it is responding correctly."""
    url = "https://api.clientify.net/v1/companies/sectors/"
    API_KEY = os.getenv("TOKEN_CLIENTIFY")

    headers = {
        "Authorization": f"Token {API_KEY}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logger.info("✅ API is responding correctly.")
        return True
    else:
        logger.info(f"❌ API error. Status code: {response.status_code}")
        logger.info(response.text)
        return False


