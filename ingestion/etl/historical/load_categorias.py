"""
load_categorias.py — Carga one-time de categorías MercadoLibre → Bronze → Silver

Flujo:
  1. Lee los 180 categoria_id únicos desde bronze.ventas_raw
  2. Llama GET /categories/{id} por cada uno
  3. Inserta raw JSON en bronze.categorias_raw
  4. Inserta limpio en silver.categorias

Uso:
  python load_categorias.py

Solo correr una vez (o si Meli agrega nuevas categorías al catálogo).
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from databricks import sql
import certifi

# ── Path setup ────────────────────────────────────────────────
# El .env está en sales-pipeline-databricks/ (3 niveles arriba)
# El código (auth/, utils/) está en ingestion/ (2 niveles arriba)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
CODE_ROOT    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, CODE_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
print("PROJECT_ROOT:", PROJECT_ROOT)
print("CODE_ROOT:", CODE_ROOT)
print("sys.path[0]:", sys.path[0])

from auth.renewToken import renewToken
from utils.logger import setup_logger

logger = setup_logger("CATEGORIAS")

BRONZE_TABLE = "iniciacion_deportiva.bronze.categorias_raw"
SILVER_TABLE = "iniciacion_deportiva.silver.categorias"
MELI_API     = "https://api.mercadolibre.com/categories"


# ── DDL ───────────────────────────────────────────────────────
# Correr esto UNA VEZ en Databricks antes de ejecutar el script:
#
# -- Bronze: todo STRING, raw de la API
# CREATE TABLE IF NOT EXISTS iniciacion_deportiva.bronze.categorias_raw (
#     categoria_id     STRING,   -- PK — ej: MLA413423
#     nombre           STRING,   -- nombre de la categoría
#     path_from_root   STRING,   -- JSON string con la jerarquía completa
#     _source          STRING,   -- siempre 'mercadolibre_api'
#     ingested_at      TIMESTAMP
# ) USING DELTA;
#
# -- Silver: tipos correctos, jerarquía expandida en columnas
# CREATE TABLE IF NOT EXISTS iniciacion_deportiva.silver.categorias (
#     categoria_id     STRING,   -- PK
#     nombre           STRING,   -- nombre de la hoja (categoría específica)
#     nivel_1          STRING,   -- raíz del árbol  (ej: "Deportes y Fitness")
#     nivel_2          STRING,   -- segundo nivel   (ej: "Natación")
#     nivel_3          STRING,   -- tercer nivel    (ej: "Gorras de Natación") — puede ser NULL
#     path_completo    STRING,   -- "Deportes y Fitness > Natación > Gorras de Natación"
#     ingested_at      TIMESTAMP
# ) USING DELTA;


# ── Conexión Databricks ───────────────────────────────────────

def get_connection():
    env_path = os.path.join(PROJECT_ROOT, ".env")
    env_vars = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                env_vars[key.strip()] = val.strip()

    hostname  = env_vars.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = env_vars.get("DATABRICKS_HTTP_PATH")
    token     = env_vars.get("DATABRICKS_TOKEN")

    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token,
        _socket_timeout=30,
        _tls_trusted_ca_file=certifi.where()
    )


# ── Step 1: Obtener categoria_ids desde Bronze ───────────────

def obtener_categoria_ids(cursor):
    cursor.execute("""
        SELECT DISTINCT categoria_id
        FROM iniciacion_deportiva.bronze.ventas_raw
        WHERE categoria_id IS NOT NULL AND categoria_id != ''
        ORDER BY categoria_id
    """)
    rows = cursor.fetchall()
    ids = [r[0] for r in rows]
    logger.info(f"📦 {len(ids)} categorías únicas encontradas en Bronze")
    return ids


# ── Step 2: Llamar API Meli por cada categoría ───────────────

def fetch_categoria(access_token, categoria_id):
    url = f"{MELI_API}/{categoria_id}"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10
    )
    if resp.status_code == 200:
        return resp.json()
    else:
        logger.warning(f"  ⚠️  {categoria_id}: HTTP {resp.status_code}")
        return None


# ── Step 3: Insertar en Bronze (raw) ─────────────────────────

def insertar_bronze(cursor, categoria_id, data):
    ahora = datetime.now(timezone.utc).isoformat()

    def esc(val):
        if val is None:
            return "NULL"
        return "'" + str(val).replace("'", "''") + "'"

    nombre        = esc(data.get("name"))
    path_raw      = esc(json.dumps(data.get("path_from_root", []), ensure_ascii=False))
    ingested_at   = esc(ahora)

    cursor.execute(f"""
        INSERT INTO {BRONZE_TABLE}
            (categoria_id, nombre, path_from_root, _source, ingested_at)
        VALUES
            ({esc(categoria_id)}, {nombre}, {path_raw}, 'mercadolibre_api', {ingested_at})
    """)

# ── Main ──────────────────────────────────────────────────────

def main():
    logger.info("🚀 Carga de categorías MercadoLibre → Bronze + Silver")

    access_token = renewToken()
    conn         = get_connection()
    cursor       = conn.cursor()

    try:
        categoria_ids = obtener_categoria_ids(cursor)
        errores       = []
        procesadas    = 0

        for i, cat_id in enumerate(categoria_ids):
            logger.info(f"[{i+1}/{len(categoria_ids)}] {cat_id}...")

            data = fetch_categoria(access_token, cat_id)
            if not data:
                errores.append(cat_id)
                continue

            try:
                insertar_bronze(cursor, cat_id, data)
                #insertar_silver(cursor, cat_id, data)
                procesadas += 1
                logger.info(f"  ✅ {data.get('name')} — {' > '.join(p['name'] for p in data.get('path_from_root', []))}")
            except Exception as e:
                logger.error(f"  ❌ Error insertando {cat_id}: {e}")
                errores.append(cat_id)

            time.sleep(0.3)  # rate limit: 1500 req/min → tranquilo con 3 req/seg

        logger.info(f"\n{'='*50}")
        logger.info(f"✅ Procesadas: {procesadas}/{len(categoria_ids)}")
        if errores:
            logger.warning(f"❌ Fallidas: {errores}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()