"""
historical_load.py — Carga histórica directa a Bronze Delta Table

Usa el Databricks SQL Connector for Python — más simple y robusto
que la REST API. Maneja conexión, tipos y errores automáticamente.

Flujo:
  Extrae mes a mes desde Meli API → INSERT directo en ventas_raw (Bronze)

¿Por qué mes a mes?
  La API de Meli pagina de a 50 órdenes. Por mes podés reintentar
  solo el mes que falló sin perder el progreso anterior.

Uso:
  python historical_load.py                         # 2025-01-01 hasta hoy
  python historical_load.py --desde 2025-06-01      # desde fecha específica
  python historical_load.py --mes 2025-03            # solo un mes
  python historical_load.py --desde 2025-01 --hasta 2025-06-30
"""

import os
from databricks import sql
import sys
import argparse
import time
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import certifi

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT_DIR)

from auth.renewToken import renewToken
from etl.getSales import obtener_seller_id, obtener_ventas_por_periodo
from utils.logger import setup_logger

load_dotenv(os.path.join(ROOT_DIR, ".env"), override=True)
logger = setup_logger("HISTORICAL")

BRONZE_TABLE = "iniciacion_deportiva.bronze.ventas_raw"
BATCH_SIZE   = 500  # filas por batch — balance entre velocidad y estabilidad


# ─────────────────────────────────────────────
# Conexión a Databricks
# ─────────────────────────────────────────────

def get_connection():
    # Leer el .env directamente — no depender de load_dotenv
    # que puede fallar si las variables ya están cargadas sin valor
    env_path = os.path.join(ROOT_DIR, ".env")
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    env_vars[key.strip()] = val.strip()

    hostname  = env_vars.get("DATABRICKS_SERVER_HOSTNAME") or os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = env_vars.get("DATABRICKS_HTTP_PATH")       or os.getenv("DATABRICKS_HTTP_PATH")
    token     = env_vars.get("DATABRICKS_TOKEN")           or os.getenv("DATABRICKS_TOKEN")

    if not all([hostname, http_path, token]):
        raise ValueError(
            "Faltan variables en .env:\n"
            "  DATABRICKS_SERVER_HOSTNAME=dbc-xxxx.cloud.databricks.com\n"
            "  DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx\n"
            "  DATABRICKS_TOKEN=dapixxxx"
        )
    
    print(f"Intenando conectar a Databricks SQL en {hostname}...")

    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token,
        _socket_timeout=30, 
        _tls_verify_hostname=True,
        _tls_trusted_ca_file=certifi.where() # timeout de socket para evitar colgarse en queries lentas
    )

# ─────────────────────────────────────────────
# Inserción en Bronze
# ─────────────────────────────────────────────

def insertar_en_bronze(cursor, df: pd.DataFrame, mes_label: str) -> int:
    """
    INSERT INTO simple — para carga histórica donde la tabla ya fue truncada.
    Sin MERGE, sin chequeo de duplicados: máxima velocidad.
    Procesa en batches de BATCH_SIZE filas para no saturar el warehouse.
    """
    if df.empty:
        logger.info(f"  ℹ️  {mes_label}: sin ventas")
        return 0

    total = len(df)
    insertadas = 0

    for i in range(0, total, BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]

        rows = []
        for _, row in batch.iterrows():
            def esc(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return "NULL"
                return "'" + str(val).replace("'", "''") + "'"

            rows.append(
                f"({esc(row.get('id_venta'))}, {esc(row.get('fecha'))}, "
                f"{esc(row.get('id_producto'))}, {esc(row.get('titulo_producto'))}, "
                f"{esc(row.get('cantidad'))}, {esc(row.get('categoria_id'))}, "
                f"{esc(row.get('precio_unitario'))}, {esc(row.get('comision'))}, "
                f"{esc(row.get('id_cliente'))}, {esc(row.get('cliente_nickname'))}, "
                f"{esc(row.get('estado'))}, 'mercadolibre_api', current_timestamp())"
            )

        values_sql = ",\n            ".join(rows)

        insert_sql = f"""
        INSERT INTO {BRONZE_TABLE}
            (id_venta, fecha, id_producto, titulo_producto, cantidad,
             categoria_id, precio_unitario, comision, id_cliente,
             cliente_nickname, estado, _source, ingested_at)
        VALUES {values_sql}
        """

        cursor.execute(insert_sql)
        insertadas += len(batch)
        logger.info(f"  ↳ {mes_label}: {insertadas}/{total} filas")

    return insertadas


# ─────────────────────────────────────────────
# Lógica mes a mes
# ─────────────────────────────────────────────

def generar_meses(desde: datetime, hasta: datetime):
    """Genera lista de (inicio_mes, fin_mes) entre dos fechas."""
    meses = []
    cursor = desde.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor <= hasta:
        fin = cursor + relativedelta(months=1) - relativedelta(seconds=1)
        meses.append((cursor, min(fin, hasta)))
        cursor += relativedelta(months=1)
    return meses


def cargar_historico(desde: datetime, hasta: datetime):
    logger.info(f"🚀 Carga histórica: {desde.strftime('%Y-%m-%d')} → {hasta.strftime('%Y-%m-%d')}")

    access_token = renewToken()
    seller_id    = obtener_seller_id(access_token)
    logger.info(f"   Seller ID: {seller_id}")

    meses = generar_meses(desde, hasta)
    logger.info(f"   Meses a procesar: {len(meses)}")

    total_ventas = 0
    errores      = []

    # Una sola conexión para toda la carga — más eficiente
    conn = get_connection()
    cursor = conn.cursor()

    try:
        for idx, (inicio, fin) in enumerate(meses):
            mes_label = inicio.strftime("%Y-%m")
            logger.info(f"\n📅 [{idx+1}/{len(meses)}] {mes_label}...")

            try:
                # Renovar token de Meli cada 5 meses (expira en 6 horas)
                if idx > 0 and idx % 5 == 0:
                    logger.info("  🔑 Renovando token Meli...")
                    access_token = renewToken()

                fecha_desde = inicio.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
                fecha_hasta = fin.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")

                ventas = obtener_ventas_por_periodo(
                    access_token, seller_id, fecha_desde, fecha_hasta
                )

                df = pd.DataFrame(ventas) if ventas else pd.DataFrame()
                n  = insertar_en_bronze(cursor, df, mes_label)
                total_ventas += n
                logger.info(f"  ✅ {mes_label}: {n} ventas cargadas")

                time.sleep(1)  # respetar rate limit de Meli entre meses

            except Exception as e:
                logger.error(f"  ❌ {mes_label} falló: {e}")
                errores.append({"mes": mes_label, "error": str(e)})
                continue  # seguimos con el siguiente mes

    finally:
        cursor.close()
        conn.close()

    # Resumen
    logger.info(f"\n{'='*50}")
    logger.info(f"✅ Carga histórica completada")
    logger.info(f"   Total ventas insertadas: {total_ventas}")
    logger.info(f"   Meses OK: {len(meses) - len(errores)}/{len(meses)}")
    if errores:
        logger.warning(f"   Meses con error:")
        for e in errores:
            logger.warning(f"     - {e['mes']}: {e['error']}")
        logger.warning("   Reejecutá con --mes YYYY-MM para reintentar cada uno")

    return total_ventas


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Carga histórica MercadoLibre → Bronze")
    parser.add_argument("--desde", default="2025-01-01", help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--hasta", default=None,         help="Fecha fin YYYY-MM-DD (default: hoy)")
    parser.add_argument("--mes",   default=None,         help="Solo un mes: YYYY-MM")
    args = parser.parse_args()

    if args.mes:
        desde = datetime.strptime(args.mes + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)
        hasta = desde + relativedelta(months=1) - relativedelta(seconds=1)
    else:
        desde = datetime.strptime(args.desde, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        hasta = (
            datetime.strptime(args.hasta, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if args.hasta else datetime.now(timezone.utc)
        )

    cargar_historico(desde, hasta)


if __name__ == "__main__":
    main()