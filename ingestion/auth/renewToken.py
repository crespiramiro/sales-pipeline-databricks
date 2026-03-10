import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

APP_ID        = os.getenv("APP_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Path absoluto basado en la ubicación de este archivo
# Funciona sin importar desde qué carpeta se corra el script
_AUTH_DIR  = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(_AUTH_DIR, "tokens", "meli_tokens.json")

def guardar_tokens(tokens):
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f)

def cargar_tokens():
    if os.path.exists(TOKEN_FILE):
        print("Cargando tokens desde", TOKEN_FILE)
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return None

def renewToken():
    tokens = cargar_tokens()
    if not tokens:
        raise Exception("No hay tokens guardados. Primero ejecutá getTokenOnce.py")

    refresh_token = tokens.get("refresh_token")
    url_token = "https://api.mercadolibre.com/oauth/token"
    payload = {
        "grant_type":    "refresh_token",
        "client_id":     APP_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token
    }
    headers  = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url_token, data=payload, headers=headers)

    if response.status_code == 200:
        data = response.json()
        guardar_tokens(data)
        print("Access token renovado y guardado en", TOKEN_FILE)
        return data["access_token"]
    else:
        raise Exception(f"Error renovando token: {response.status_code} - {response.text}")

if __name__ == "__main__":
    token = renewToken()
    print("Access Token actual:", token)