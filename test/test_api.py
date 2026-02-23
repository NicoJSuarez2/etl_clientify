#%%
import requests
import os
import dotenv
import sys

dotenv.load_dotenv()

def test_api():
    """Test the API endpoint to ensure it is responding correctly."""
    url = "https://api.clientify.net/v1/companies/sectors/"
    API_KEY = os.getenv("TOKEN_CLIENTIFY")

    headers = {
        "Authorization": f"Token {API_KEY}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("✅ API is responding correctly.")
        return True
    else:
        print(f"❌ API error. Status code: {response.status_code}")
        print(response.text)
        return False


""" # 👇 BLOQUE PRINCIPAL
if __name__ == "__main__":
    if test_api():
        print("🚀 Continuing execution...")
        # Aquí va tu código principal
        # ejemplo:
        # run_etl()
    else:
        print("⛔ Execution stopped due to API failure.")
        sys.exit(1) """


