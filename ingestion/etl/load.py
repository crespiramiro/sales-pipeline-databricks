"""
load.py — Carga Bronze a Unity Catalog Volumes (Databricks)

Flujo:
  DataFrame (pandas) → Parquet en memoria → Upload a Volume via Files API

¿Por qué Volumes y no DBFS root?
  DBFS root (/FileStore) tiene restricciones de permisos en workspaces modernos.
  Unity Catalog Volumes es la forma recomendada por Databricks para almacenar
  archivos, y es compatible con Auto Loader y Delta Lake.
"""

import io
import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils.logger import setup_logger

load_dotenv()
logger = setup_logger()

# ─────────────────────────────────────────────
# Config — estas variables van en tu .env y en GitHub Secrets
# ─────────────────────────────────────────────

DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST")   # ej: https://community.cloud.databricks.com
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")  # personal access token

# Path del Volume donde van los Parquet de Bronze
# Formato: /Volumes/<catalog>/<schema>/<volume>
VOLUME_BRONZE_PATH = "/Volumes/iniciacion_deportiva/bronze/staging_zone"


def _parquet_to_bytes(df: pd.DataFrame) -> bytes:
    """
    Convierte DataFrame a bytes Parquet en memoria.
    No escribe nada al disco local — correcto para GitHub Actions.
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    return buffer.read()


def _volume_upload(parquet_bytes: bytes, volume_path: str):
    """
    Sube bytes a un Unity Catalog Volume usando la Files API de Databricks.

    Diferencia clave con DBFS:
    - DBFS usaba POST /api/2.0/dbfs/put con contenido en base64 dentro de JSON
    - Volumes usa PUT /api/2.0/fs/files/<path> con contenido binario directo

    Docs: https://docs.databricks.com/api/workspace/files/upload
    """
    # /Volumes/iniciacion_deportiva/bronze/staging_zone/ventas_xxx.parquet
    # → {host}/api/2.0/fs/files/Volumes/iniciacion_deportiva/bronze/staging_zone/ventas_xxx.parquet
    path_limpio = volume_path.lstrip("/")
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files/{path_limpio}"

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream",  # binario directo, no JSON
    }

    response = requests.put(url, headers=headers, data=parquet_bytes, timeout=60)

    # Files API retorna 204 No Content en éxito (distinto al 200 de DBFS)
    if response.status_code in (200, 204):
        logger.info(f"✅ Archivo subido a Volume: {volume_path}")
    else:
        raise Exception(
            f"❌ Error subiendo a Volume: {response.status_code} — {response.text}"
        )


def cargar_a_bronze(df: pd.DataFrame) -> str:
    """
    Entry point principal.

    Convierte el DataFrame a Parquet y lo sube al Volume de Bronze.

    Naming convention: ventas_YYYYMMDD_HHMMSS_UTC.parquet
    Cada corrida genera un archivo nuevo — Auto Loader detecta los nuevos.
    Si el DataFrame está vacío, subimos igual (heartbeat/auditoría).

    Retorna el path completo del archivo creado en el Volume.
    """
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        raise ValueError(
            "DATABRICKS_HOST y DATABRICKS_TOKEN deben estar configurados en .env"
        )

    timestamp   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename    = f"ventas_{timestamp}_UTC.parquet"
    volume_path = f"{VOLUME_BRONZE_PATH}/{filename}"

    logger.info(f"📦 Preparando archivo Bronze: {filename}")
    logger.info(f"   Filas en el DataFrame: {len(df)}")
    logger.info(f"   Columnas: {list(df.columns)}")

    parquet_bytes = _parquet_to_bytes(df)
    size_kb = len(parquet_bytes) / 1024
    logger.info(f"   Tamaño Parquet: {size_kb:.1f} KB")

    _volume_upload(parquet_bytes, volume_path)

    return volume_path