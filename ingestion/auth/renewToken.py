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
    tokens_env = os.getenv("MELI_TOKENS_JSON") or os.getenv("MELI_TOKENS")
    if tokens_env:
        try:
            print("Cargando tokens desde variable de entorno (MELI_TOKENS_JSON / MELI_TOKENS)")
            return json.loads(tokens_env)
        except Exception as e:
            print("Error parseando variable de entorno de tokens:", e)
    return None

def renewToken():
    tokens = cargar_tokens()
    if not tokens:
        raise Exception("No hay tokens guardados. Primero ejecutá getTokenOnce.py")

    app_id = (os.getenv("APP_ID") or APP_ID or "").strip()
    client_secret = (os.getenv("CLIENT_SECRET") or CLIENT_SECRET or "").strip()
    refresh_token = (tokens.get("refresh_token") or "").strip()

    if not app_id or not client_secret or not refresh_token:
        raise Exception("Faltan credenciales (APP_ID, CLIENT_SECRET o refresh_token) para renovar el token.")

    url_token = "https://api.mercadolibre.com/oauth/token"
    payload = {
        "grant_type":    "refresh_token",
        "client_id":     app_id,
        "client_secret": client_secret,
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
        err_msg = f"Error renovando token: {response.status_code} - {response.text}"
        if "invalid_grant" in response.text:
            err_msg += "\n💡 Sugerencia: El refresh token expiró o ya fue utilizado previamente. " \
                       "Ejecutá getTokenOnce.py localmente y actualizá el secret MELI_TOKENS en GitHub Secrets."
        raise Exception(err_msg)

if __name__ == "__main__":
    token = renewToken()
    print("Access Token actual:", token)
