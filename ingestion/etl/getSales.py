"""
getSales.py — Extracción de ventas desde MercadoLibre API
Ventana: últimas 2 horas (overlap intencional para no perder ventas)
Los duplicados se manejan en Silver layer con Delta MERGE
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from utils.logger import setup_logger
from utils.alerts import send_alert

logger = setup_logger()


# ─────────────────────────────────────────────
# Helpers con reintentos (backoff exponencial)
# ─────────────────────────────────────────────

def _get_with_retry(url, headers, max_retries=3, timeout=15):
    """GET genérico con reintentos. Separo esto para no repetir lógica."""
    for intento in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"⚠️  Intento {intento + 1}/{max_retries} fallido — {url}: {e}")
            if intento < max_retries - 1:
                time.sleep(2 ** intento)  # 1s, 2s, 4s
            else:
                raise Exception(f"Error persistente después de {max_retries} intentos: {url}")


def obtener_seller_id(access_token):
    data = _get_with_retry(
        "https://api.mercadolibre.com/users/me",
        {"Authorization": f"Bearer {access_token}"}
    )
    return data["id"]


def obtener_ids_ordenes(access_token, seller_id, fecha_desde, fecha_hasta, limit=50, offset=0):
    url = (
        f"https://api.mercadolibre.com/orders/search?seller={seller_id}"
        f"&limit={limit}&offset={offset}"
        f"&order.date_created.from={fecha_desde}"
        f"&order.date_created.to={fecha_hasta}"
    )
    data = _get_with_retry(url, {"Authorization": f"Bearer {access_token}"})
    ids = [order["id"] for order in data.get("results", [])]
    total = data.get("paging", {}).get("total", 0)
    return ids, total


def obtener_detalles_orden(access_token, order_id):
    return _get_with_retry(
        f"https://api.mercadolibre.com/orders/{order_id}",
        {"Authorization": f"Bearer {access_token}"}
    )


# ─────────────────────────────────────────────
# Extracción principal
# ─────────────────────────────────────────────

def obtener_ventas_por_periodo(access_token, seller_id, fecha_desde, fecha_hasta, limit=50):
    """
    Extrae todas las ventas en el período dado.
    Retorna lista de dicts con los campos raw (tipos nativos de la API).
    """
    ventas = []
    offset = 0
    ids_vistos = set()
    errores = []

    # Primera llamada para saber el total
    ids, total = obtener_ids_ordenes(access_token, seller_id, fecha_desde, fecha_hasta, limit, offset)
    logger.info(f"📊 Total órdenes a procesar: {total}")

    while offset < total:
        logger.info(f"🔄 Procesando offset {offset}/{total}...")

        for order_id in ids:
            if order_id in ids_vistos:
                continue
            ids_vistos.add(order_id)

            try:
                data = obtener_detalles_orden(access_token, order_id)


                for item in data.get("order_items", []):
                    ventas.append({
                        # ── Todo STRING para coincidir con schema Bronze ─
                        # Silver se encarga de castear a los tipos correctos
                        # NO filtramos canceladas: en Silver hacemos MERGE por estado
                        # así registramos el ciclo completo paid → cancelled
                        "id_venta":          str(data.get("id", "")),
                        "fecha":             str(data.get("date_created", "")),
                        "id_producto":       str(item["item"].get("id", "")),
                        "titulo_producto":   str(item["item"].get("title", "")),
                        "cantidad":          str(item.get("quantity", "")),
                        "categoria_id":      str(item["item"].get("category_id", "")),
                        "precio_unitario":   str(item.get("unit_price", "")),
                        "comision":          str(item.get("sale_fee", "")),
                        "id_cliente":        str(data.get("buyer", {}).get("id", "")),
                        "cliente_nickname":  str(data.get("buyer", {}).get("nickname", "")),
                        "estado":            str(data.get("status", "")),
                    })

                logger.info(f"✔️  Orden {order_id} procesada")

            except Exception as e:
                logger.warning(f"⚠️  Error con orden {order_id}: {e}")
                errores.append({"order_id": order_id, "error": str(e)})

        offset += limit
        if offset < total:
            try:
                ids, _ = obtener_ids_ordenes(
                    access_token, seller_id, fecha_desde, fecha_hasta, limit, offset
                )
                time.sleep(1)  # respetamos rate limit de Meli
            except Exception as e:
                logger.error(f"❌ Error obteniendo siguiente lote: {e}")
                break

    logger.info(f"✅ Ventas extraídas: {len(ventas)} | Errores: {len(errores)}")
    return ventas


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main(access_token):
    """
    Extrae ventas de las últimas 2 horas y retorna DataFrame.
    
    La ventana de 2hs es intencional: corremos cada 15 min,
    entonces cada venta aparece ~8 veces. Silver deduplica con MERGE.
    Esto garantiza que NUNCA perdemos una venta.
    """
    seller_id = obtener_seller_id(access_token)
    logger.info(f"🧾 Seller ID: {seller_id}")

    ahora = datetime.now(timezone.utc)
    hace_2_horas = ahora - timedelta(hours=2)

    fecha_desde = hace_2_horas.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    fecha_hasta = ahora.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")

    logger.info(f"📅 Ventana: {fecha_desde} → {fecha_hasta}")

    ventas = obtener_ventas_por_periodo(access_token, seller_id, fecha_desde, fecha_hasta)

    if not ventas:
        logger.info("ℹ️  Sin ventas en la ventana. Se generará archivo vacío igual (para auditoría).")

    df = pd.DataFrame(ventas)
    return df