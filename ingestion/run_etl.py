"""
run_etl.py — Orquestador principal del ETL
Corre cada 15 min via GitHub Actions (horario 8am-1am)

Flujo:
  1. Renova token de MercadoLibre
  2. Extrae ventas (últimas 2 horas)
  3. Sube Parquet a DBFS Bronze
  4. Notifica por email (solo errores — éxito es silencioso para no spamear)
"""

import sys
import time
import traceback
from datetime import datetime

from auth.renewToken import renewToken
from etl.getSales import main as extract_sales
from etl.load import cargar_a_bronze
from utils.logger import setup_logger
from utils.alerts import send_alert

logger = setup_logger()


def run_etl(max_retries=3, delay_seconds=30):
    """
    Ejecuta el ETL con reintentos.
    
    Cambios vs versión anterior:
    - No hay crear_tablas() ni verificar_datos() — eso es trabajo de Silver en Databricks
    - No hay engine.dispose() — no hay base de datos
    - delay_seconds bajó de 60 a 30 — corremos cada 15 min, no podemos esperar mucho
    - Email de éxito eliminado — a 15 min intervals mandarías 96 mails por día
    """
    for intento in range(max_retries):
        try:
            logger.info(f"🔄 ETL iniciando (intento {intento + 1}/{max_retries})...")

            # 1. Token
            logger.info("🔑 Renovando token MercadoLibre...")
            access_token = renewToken()
            logger.info("✅ Token renovado.")

            # 2. Extract
            logger.info("📥 Extrayendo ventas (ventana 2 horas)...")
            df = extract_sales(access_token)
            logger.info(f"✅ {len(df)} ventas extraídas.")

            # 3. Load → DBFS Bronze
            logger.info("📤 Subiendo Parquet a Uniry Catalog Volumes...")
            dbfs_path = cargar_a_bronze(df)
            logger.info(f"✅ Bronze actualizado: {dbfs_path}")

            logger.info("🎉 ETL completado exitosamente.")
            return True

        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"❌ Error en intento {intento + 1}/{max_retries}: {e}")

            if intento < max_retries - 1:
                logger.info(f"⏳ Reintentando en {delay_seconds}s...")
                time.sleep(delay_seconds)
            else:
                # Solo mandamos email cuando TODOS los reintentos fallaron
                # No spameamos por errores transitorios
                logger.error("💥 ETL falló definitivamente.")
                send_alert(
                    "crespiramiro@outlook.com",
                    f"🚨 ETL Falló — {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC",
                    f"El ETL falló después de {max_retries} intentos.\n\n"
                    f"Error: {e}\n\n"
                    f"Traceback:\n{tb}"
                )
                raise


def main():
    start = datetime.utcnow()
    try:
        run_etl()
        elapsed = (datetime.utcnow() - start).total_seconds()
        logger.info(f"⏱️  Tiempo total: {elapsed:.1f}s")
        sys.exit(0)
    except Exception:
        elapsed = (datetime.utcnow() - start).total_seconds()
        logger.error(f"⏱️  Falló después de {elapsed:.1f}s")
        sys.exit(1)


if __name__ == "__main__":
    main()