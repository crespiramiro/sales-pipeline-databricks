"""
getTokenOnce.py — Script para obtener el token inicial de MercadoLibre OAuth2.

Flujo:
  1. Genera la URL de autorización de MercadoLibre.
  2. Solicita el código de autorización (code) retornado en la URL de callback.
  3. Canjea el código por access_token + refresh_token.
  4. Guarda la respuesta en auth/tokens/meli_tokens.json.
"""

import os
import requests
import json
import webbrowser
from dotenv import load_dotenv

load_dotenv()

APP_ID        = os.getenv("APP_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI  = os.getenv("REDIRECT_URI")

_AUTH_DIR  = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(_AUTH_DIR, "tokens", "meli_tokens.json")


def guardar_tokens(tokens):
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f, indent=2)
    print(f"\n✅ Tokens guardados exitosamente en: {TOKEN_FILE}")


def main():
    app_id = (APP_ID or "").strip()
    client_secret = (CLIENT_SECRET or "").strip()
    redirect_uri = (REDIRECT_URI or "").strip()

    if not app_id or not client_secret or not redirect_uri:
        print("❌ Error: Faltan configurar APP_ID, CLIENT_SECRET o REDIRECT_URI en el archivo .env")
        return

    auth_url = f"https://auth.mercadolibre.com.ar/authorization?response_type=code&client_id={app_id}&redirect_uri={redirect_uri}"

    print("=" * 70)
    print("🔑 OBTENCIÓN DE TOKEN INICIAL MERCADOLIBRE")
    print("=" * 70)
    print("\n🌐 Abriendo navegador para autorizar la app...\n")
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass
    print("Si el navegador no se abrió automáticamente, entra a este enlace:")
    print(auth_url)
    print("\n" + "=" * 70)
    print("2️⃣ Tras autorizar, serás redirigido a una URL que luce así:")
    print(f"   {redirect_uri}?code=TG-XXXXXXXXXX-XXXXXX...")
    print("=" * 70 + "\n")

    code_input = input("👉 Ingresa el parámetro 'code' (o la URL completa a la que fuiste redirigido): ").strip()

    # Si pegaron la URL completa, extraer el parámetro `code=`
    if "code=" in code_input:
        code = code_input.split("code=")[1].split("&")[0]
    else:
        code = code_input

    if not code:
        print("❌ Código no válido.")
        return

    print("\n🔄 Canjeando código por tokens...")
    token_url = "https://api.mercadolibre.com/oauth/token"
    payload = {
        "grant_type": "authorization_code",
        "client_id": app_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    res = requests.post(token_url, data=payload, headers=headers)
    if res.status_code == 200:
        data = res.json()
        guardar_tokens(data)
        print("\n🎉 ¡Token obtenido con éxito!")
    else:
        print(f"\n❌ Error ({res.status_code}): {res.text}")


if __name__ == "__main__":
    main()
