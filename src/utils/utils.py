#%%
import os
import requests
import dotenv
import traceback

dotenv.load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def enviar_alerta(mensaje: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    payload = {
        "chat_id": CHAT_ID,
        "text": mensaje
    }

    response = requests.post(url, data=payload)

    if response.status_code != 200:
        print("❌ Error enviando mensaje a Telegram:", response.text)


def main():
    # Simulamos error
    x = 1 / 0


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_msg = f"🚨 ERROR EN ETL\n\n{str(e)}\n\n{traceback.format_exc()}"
        enviar_alerta(error_msg)