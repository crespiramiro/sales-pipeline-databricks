# Sales Pipeline Databricks — Iniciación Deportiva

Pipeline de datos end-to-end para análisis de ventas de MercadoLibre, implementado sobre arquitectura Medallion en Databricks. Proyecto final del Bootcamp SQL & Databricks.

## Descripción

El negocio familiar (artículos deportivos) vende exclusivamente a través de MercadoLibre. Este pipeline extrae las ventas automáticamente cada hora, las procesa en tres capas (Bronze → Silver → Gold) y las expone en un dashboard operativo para decisiones de reposición de stock.

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│  EXTRACCIÓN (cada ~1 hora)                                      │
│  GitHub Actions → Python ETL → Parquet → Unity Catalog Volume  │
└─────────────────────┬───────────────────────────────────────────┘
                      │ File Arrival Trigger
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE — Raw data                                              │
│  Auto Loader → ventas_raw (Delta Table)                        │
│  Todo STRING, sin transformaciones, duplicados esperados        │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER — Datos limpios y tipados                               │
│  ventas (MERGE dedup) + categorias (CREATE OR REPLACE)         │
│  TRY_CAST, TRIM, validaciones, deduplicación por ingested_at   │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD — Star Schema para analytics                              │
│  dim_producto (SCD Type 2) + dim_tiempo + fact_ventas          │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  DASHBOARD — Databricks AI/BI                                   │
│  5 KPIs operativos con filtros interactivos                    │
└─────────────────────────────────────────────────────────────────┘
```

## Estructura del repositorio

```
sales-pipeline-databricks/
├── ingestion/                        # ETL Python (corre en GitHub Actions)
│   ├── auth/
│   │   └── renewToken.py            # Renovación automática token MercadoLibre
│   ├── etl/
│   │   ├── getSales.py              # Extracción ventas API Meli (ventana 2hs)
│   │   ├── load.py                  # Upload Parquet → Unity Catalog Volume
│   │   └── historical/
│   │       ├── full_load.py         # Carga histórica mes a mes
│   │       └── load_categorias.py   # Carga one-time de categorías
│   ├── utils/
│   │   ├── logger.py                # Logger con rotación diaria
│   │   └── alerts.py                # Alertas email via Resend
│   ├── run_etl.py                   # Orquestador principal
│   └── requirements.txt
├── notebooks/                        # Notebooks Databricks
│   ├── 00_EDA/                      # Análisis exploratorio Bronze
│   ├── 01_DDL/                      # Creación de tablas y schemas
│   ├── 02_bronze/                   # Auto Loader ETL
│   ├── 03_silver/                   # ETL Silver (ventas + categorias)
│   ├── 04_gold/                     # ETL Gold (dims + fact)
│   └── 05_maintenance/              # OPTIMIZE + VACUUM semanal (Bronze, Silver, Gold)
├── docs/
│   ├── diagrama_arquitectura.png    # Diagrama Medallion del proyecto
│   └── decisiones_tecnicas.md      # Log de decisiones de arquitectura y diseño
└── .github/
    └── workflows/
        └── etl.yml                  # Schedule GitHub Actions
```

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Orquestación ETL | GitHub Actions (cron schedule) |
| Lenguaje extracción | Python 3.12 |
| API fuente | MercadoLibre Orders API |
| Formato intermedio | Parquet (pyarrow) |
| Storage | Databricks Unity Catalog Volumes |
| Ingesta streaming | Databricks Auto Loader |
| Almacenamiento | Delta Lake |
| Transformaciones | Databricks SQL (notebooks) |
| Orquestación Databricks | Databricks Jobs (File Arrival trigger + schedule semanal) |
| Dashboard | Databricks AI/BI |
| Alertas | Resend (email) |

## Databricks Jobs

| Job | Trigger | Descripción |
|---|---|---|
| ETL Pipeline E2E | File Arrival (`/Volumes/.../staging_zone/`) | Bronze → Silver → Gold en cascada |
| Maintenance Job | Semanal (domingos 3am ART) | OPTIMIZE + VACUUM en todas las capas |

## Tablas del Data Warehouse

### Bronze
| Tabla | Descripción | Filas aprox. |
|---|---|---|
| `bronze.ventas_raw` | Ventas raw de MercadoLibre, todo STRING | ~10k+ |
| `bronze.categorias_raw` | Categorías raw de la API | 180 |

### Silver
| Tabla | Descripción |
|---|---|
| `silver.ventas` | Ventas limpias, tipadas, deduplicadas (MERGE) |
| `silver.categorias` | Jerarquía de categorías expandida en niveles |

### Gold — Star Schema
| Tabla | Tipo | Descripción |
|---|---|---|
| `gold.dim_producto` | Dimensión SCD Type 2 | Productos con historial de cambios de título |
| `gold.dim_tiempo` | Dimensión estática | Calendario con atributos de fecha |
| `gold.fact_ventas` | Tabla de hechos | 1 fila = 1 línea de venta |

## Dashboard operativo

5 KPIs orientados a decisiones de reposición mensual:

- **KPI 1** — Facturación mensual (ARS) con evolución por mes
- **KPI 2** — Revenue por categoría (barras horizontales)
- **KPI 3** — Top 10 productos por unidades vendidas
- **KPI 4** — Ticket promedio real por pedido (reconstruye pedidos multi-item)
- **KPI 5** — Resumen últimos 30/60/90 días por categoría

Filtros interactivos: rango de fechas y categoría.

## Cómo correr el ETL localmente

```bash
# 1. Instalar dependencias
cd ingestion
pip install -r requirements.txt

# 2. Configurar variables de entorno (.env en raíz del proyecto)
# Ver .env.example para las variables requeridas

# 3. Obtener token inicial de MercadoLibre (solo la primera vez)
python auth/getTokenOnce.py

# 4. Correr el ETL
python run_etl.py

# 5. Carga histórica (si es necesario)
python -m etl.historical.full_load --desde 2025-01-01 --hasta 2025-12-31
```

## Variables de entorno requeridas

```bash
# MercadoLibre
APP_ID=
CLIENT_SECRET=
REDIRECT_URI=

# Databricks
DATABRICKS_HOST=
DATABRICKS_TOKEN=
DATABRICKS_SERVER_HOSTNAME=
DATABRICKS_HTTP_PATH=

# Email (Resend)
RESEND_API_KEY=
EMAIL_FROM=
```

## Decisiones técnicas

Ver [docs/decisiones_tecnicas.md](docs/decisiones_tecnicas.md) para el log completo de decisiones de arquitectura y diseño.