"""
run_products_web_sync.py — Orquestador de sincronización de catálogo MeLi → NeonDB (Web)
Corre cada 6 horas vía GitHub Actions (ó manual via workflow_dispatch)

Flujo:
  1. Renueva token de MercadoLibre
  2. Extrae catálogo completo de productos del vendedor (detalles, imágenes, precio con descuento, SKU, categoría)
  3. Ejecuta UPSERT en la base de datos Neon (tabla productos_web)
  4. Envía notificación por email únicamente si falla de manera definitiva
"""

import sys
import time
import traceback
from datetime import datetime, timezone

from etl.products_fetch import run_products_web_sync
from utils.logger import setup_logger
from utils.alerts import send_alert

logger = setup_logger("RunProductsSync")


def main_with_retries(max_retries=3, delay_seconds=30):
    for intento in range(max_retries):
        try:
            logger.info(f"🔄 Sincronización Catálogo Web iniciando (intento {intento + 1}/{max_retries})...")
            run_products_web_sync()
            logger.info("🎉 Sincronización de Catálogo Web completada exitosamente.")
            return True

        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"❌ Error en intento {intento + 1}/{max_retries}: {e}")

            if intento < max_retries - 1:
                logger.info(f"⏳ Reintentando en {delay_seconds}s...")
                time.sleep(delay_seconds)
            else:
                logger.error("💥 Sincronización de Catálogo Web falló definitivamente.")
                now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
                send_alert(
                    "crespiramiro@outlook.com",
                    f"🚨 Sincronización Catálogo Web Falló — {now_str} UTC",
                    f"La sincronización de productos a NeonDB falló después de {max_retries} intentos.\n\n"
                    f"Error: {e}\n\n"
                    f"Traceback:\n{tb}"
                )
                raise


def main():
    start = datetime.now(timezone.utc)
    try:
        main_with_retries()
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"⏱️ Tiempo total: {elapsed:.1f}s")
        sys.exit(0)
    except Exception:
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.error(f"⏱️ Falló después de {elapsed:.1f}s")
        sys.exit(1)


if __name__ == "__main__":
    main()
