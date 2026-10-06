import os
import json
import time
import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv

from auth.renewToken import renewToken
from utils.logger import setup_logger
from utils.alerts import send_alert

load_dotenv()

logger = setup_logger("ProductsSync")

# Diccionario global para evitar repetir llamadas a la API de categorías
CACHE_CATEGORIAS = {}


def obtener_nombre_categoria(token: str, cat_id: str) -> str:
    """Consulta el nombre legible de la categoría usando caché para optimizar."""
    if not cat_id:
        return "General"

    if cat_id in CACHE_CATEGORIAS:
        return CACHE_CATEGORIAS[cat_id]

    url = f"https://api.mercadolibre.com/categories/{cat_id}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            nombre = resp.json().get("name", "General")
            CACHE_CATEGORIAS[cat_id] = nombre
            return nombre
    except Exception as e:
        logger.warning(f"⚠️ Error obteniendo categoría {cat_id}: {e}")

    return "General"


def fetch_all_items(token: str) -> list:
    """Trae todos los IDs de productos del vendedor usando scroll/scan."""
    headers = {"Authorization": f"Bearer {token}"}
    me_resp = requests.get("https://api.mercadolibre.com/users/me", headers=headers, timeout=10)
    me_resp.raise_for_status()
    me = me_resp.json()
    seller_id = me["id"]

    logger.info(f"👤 Vendedor: {me.get('nickname')} (ID: {seller_id})")

    ids = []
    scroll_id = None
    while True:
        url = f"https://api.mercadolibre.com/users/{seller_id}/items/search"
        params = {"search_type": "scan", "limit": 100}
        if scroll_id:
            params["scroll_id"] = scroll_id

        resp = requests.get(url, headers=headers, params=params, timeout=15).json()
        res = resp.get("results", [])
        if not res:
            break
        ids.extend(res)
        scroll_id = resp.get("scroll_id")
        if not scroll_id:
            break

    logger.info(f"📦 Total de productos encontrados: {len(ids)}")
    return ids


def fetch_details_and_photos(token: str, item_ids: list) -> list:
    """Trae detalles, fotos y nombres de categorías en lotes de 20."""
    logger.info(f"⚡ Extrayendo detalles para {len(item_ids)} productos...")
    results = []
    batch_size = 20
    headers = {"Authorization": f"Bearer {token}"}
    campos = "id,title,price,available_quantity,status,category_id,pictures,attributes"

    for i in range(0, len(item_ids), batch_size):
        batch = item_ids[i:i + batch_size]
        url = f"https://api.mercadolibre.com/items?ids={','.join(batch)}&attributes={campos}"

        resp = requests.get(url, headers=headers, timeout=15).json()
        for item in resp:
            if item.get("code") == 200:
                body = item["body"]

                # Extraer SKU
                sku = next(
                    (a["value_name"] for a in body.get("attributes", []) if a["id"] == "SELLER_SKU"),
                    "N/A"
                )

                # Foto principal
                foto = body["pictures"][0]["secure_url"] if body.get("pictures") else None

                # Obtener nombre de categoría
                cat_id = body.get("category_id")
                nombre_cat = obtener_nombre_categoria(token, cat_id)

                results.append({
                    "id": body["id"],
                    "titulo": body["title"],
                    "precio": round(float(body["price"]) * 0.95, 2),
                    "stock": body["available_quantity"],
                    "estado": body["status"],
                    "categoria_nombre": nombre_cat,
                    "sku": sku,
                    "foto_url": foto or "https://via.placeholder.com/300"
                })

        if (i + batch_size) % 100 == 0:
            logger.info(f"   ... procesados {i + batch_size} / {len(item_ids)}")

        time.sleep(0.1)

    logger.info(f"✅ Detalles extraídos: {len(results)} productos.")
    return results


def sync_to_neon(data: list, database_url: str):
    """Carga la data en NeonDB usando UPSERT."""
    if not data:
        logger.warning("⚠️ No hay datos para cargar en NeonDB.")
        return

    logger.info(f"💾 Cargando {len(data)} productos a NeonDB...")

    df = pd.DataFrame(data)
    db_url = database_url.replace("postgres://", "postgresql://")
    engine = sqlalchemy.create_engine(db_url)

    with engine.connect() as conn:
        # 1. Crear tabla principal si no existe
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS productos_web (
                id VARCHAR(50) PRIMARY KEY,
                titulo TEXT,
                precio FLOAT,
                stock INT,
                estado VARCHAR(20),
                categoria_nombre TEXT,
                sku VARCHAR(100),
                foto_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.commit()

        # 2. Carga a tabla temporal
        df.to_sql("temp_sync", engine, if_exists="replace", index=False)

        # 3. UPSERT
        conn.execute(text("""
            INSERT INTO productos_web (id, titulo, precio, stock, estado, categoria_nombre, sku, foto_url, updated_at)
            SELECT id, titulo, precio, stock, estado, categoria_nombre, sku, foto_url, CURRENT_TIMESTAMP
            FROM temp_sync
            ON CONFLICT (id) DO UPDATE SET
                titulo           = EXCLUDED.titulo,
                precio           = EXCLUDED.precio,
                stock            = EXCLUDED.stock,
                estado           = EXCLUDED.estado,
                categoria_nombre = EXCLUDED.categoria_nombre,
                sku              = EXCLUDED.sku,
                foto_url         = EXCLUDED.foto_url,
                updated_at       = CURRENT_TIMESTAMP;
        """))

        # 4. Limpiar
        conn.execute(text("DROP TABLE IF EXISTS temp_sync;"))
        conn.commit()

    logger.info("✅ Sincronización con NeonDB terminada con éxito.")


def run_products_web_sync():
    """Ejecuta el pipeline completo de sincronización de catálogo."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("❌ La variable de entorno DATABASE_URL (NeonDB) no está configurada.")

    logger.info("🔑 Renovando access token de MercadoLibre...")
    token = renewToken()

    item_ids = fetch_all_items(token)
    if item_ids:
        datos_finales = fetch_details_and_photos(token, item_ids)
        sync_to_neon(datos_finales, database_url)
    else:
        logger.warning("⚠️ No se encontraron productos activos/registrados en el vendedor.")


if __name__ == "__main__":
    try:
        run_products_web_sync()
    except Exception as e:
        logger.error(f"❌ ERROR en sincronización de productos a NeonDB: {e}")
        exit(1)
