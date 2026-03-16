# Decisiones Técnicas — Sales Pipeline Databricks

Log de decisiones arquitectónicas y de diseño tomadas durante el desarrollo del pipeline.

---

## Bronze Layer

### Todo STRING en Bronze
**Decisión:** Todos los campos de `ventas_raw` son STRING excepto `ingested_at` (TIMESTAMP).

**Razón:** La API de MercadoLibre puede cambiar el formato de sus campos sin previo aviso. Al guardar todo como STRING en Bronze, el schema nunca rompe. Silver es la capa responsable de castear a los tipos correctos con `TRY_CAST` — si un valor no castea, retorna NULL en lugar de fallar el pipeline.

---

### Ventana de extracción de 2 horas con overlap intencional
**Decisión:** El ETL extrae las ventas de las últimas 2 horas aunque corre cada ~1 hora.

**Razón:** Garantiza que ninguna venta se pierda por latencia de la API o delay del scheduler. Los duplicados resultantes (cada venta aparece ~2 veces en Bronze) se eliminan en Silver con un MERGE idempotente sobre `id_venta`.

---

### No filtrar canceladas en Bronze
**Decisión:** Las órdenes con `estado = 'cancelled'` se guardan igual en Bronze.

**Razón:** Una venta puede llegar como `paid` en una corrida y como `cancelled` en la siguiente. Si filtramos en Bronze perdemos el ciclo completo. El MERGE en Silver actualiza el estado cuando cambia, registrando la transición `paid → cancelled`.

---

### Duplicados esperados por diseño
**Decisión:** `bronze.ventas_raw` puede tener múltiples filas para el mismo `id_venta`.

**Razón:** Consecuencia directa de la ventana de 2 horas con schedule de 1 hora. No es un error — es la estrategia de "mejor duplicado que perdido". La deduplicación es responsabilidad de Silver.

---

## Silver Layer

### MERGE en Silver con deduplicación por `ingested_at DESC`
**Decisión:** El ETL Silver usa MERGE ON `id_venta` para insertar o actualizar.

**Razón:** Garantiza idempotencia — se puede correr N veces sin generar duplicados. Cuando hay múltiples registros del mismo `id_venta` en Bronze (por el overlap), se toma el más reciente (`ROW_NUMBER() OVER (PARTITION BY id_venta ORDER BY ingested_at DESC)`).

---

### `titulo_producto` no se normaliza en Silver
**Decisión:** Silver guarda `titulo_producto` tal como viene de Bronze, sin lookup a una tabla de productos.

**Razón:** Los títulos de productos en MercadoLibre cambian frecuentemente (ediciones menores del listing). Normalizar en Silver requeriría un JOIN costoso y aún así no capturaría el historial de cambios. Gold maneja esto con SCD Type 2 en `dim_producto`.

---

### `silver.categorias` usa CREATE OR REPLACE
**Decisión:** En lugar de MERGE, `ETL_silver_categorias` recrea la tabla completa.

**Razón:** Son solo 180 filas de datos de referencia que raramente cambian. CREATE OR REPLACE es más simple y garantiza consistencia sin necesidad de lógica de MERGE. El costo de recrear 180 filas es despreciable.

---

### Validaciones en Silver descartan en lugar de corregir
**Decisión:** Filas con `precio_unitario <= 0` o `cantidad <= 0` se descartan silenciosamente.

**Razón:** Estos valores no tienen sentido de negocio y probablemente son errores de la API. Corregirlos requeriría asumir valores que no tenemos. Es preferible perder esas filas (son < 0.1% del total) que contaminar Gold con datos incorrectos.

---

## Gold Layer — Star Schema

### Granularidad de `fact_ventas`: 1 fila = 1 línea de venta
**Decisión:** La fact table tiene una fila por cada item vendido, no por pedido.

**Razón:** Un pedido puede contener múltiples productos distintos. Agregar a nivel pedido perdería el detalle por producto que es el dato más valioso para decisiones de reposición. Los pedidos multi-item se reconstruyen en las queries del dashboard agrupando por `pedido_key`.

---

### `pedido_key` como dimensión degenerada
**Decisión:** `pedido_key = CONCAT(id_cliente, '_', DATE_FORMAT(fecha, 'yyyyMMddHHmm'))` se guarda directamente en la fact table.

**Razón:** MercadoLibre no expone un `order_id` explícito en el endpoint de ventas. Esta clave reconstruida permite agrupar líneas del mismo pedido para calcular el ticket promedio real. Como no tiene atributos propios, no justifica una dimensión separada.

---

### `dim_producto` con SCD Type 2 solo en `titulo_producto`
**Decisión:** El historial de cambios se trackea únicamente para el título del producto, no para el precio.

**Razón:** El precio cambia constantemente y pertenece a la fact table (es una métrica de la transacción). El título del producto es un atributo descriptivo que cambia raramente pero cuando cambia es significativo (renombrado del producto). El precio en `dim_producto` sería redundante e incorrecto.

---

### SCD Type 2 en 2 pasos separados (MERGE + INSERT)
**Decisión:** El ETL de `dim_producto` primero hace un MERGE para cerrar filas viejas (`fecha_fin`, `es_actual = false`) y después un INSERT para agregar las nuevas versiones.

**Razón:** Databricks no permite actualizar e insertar la misma fila en un solo MERGE statement. El patrón de 2 pasos es la solución estándar para SCD Type 2 en Delta Lake.

---

### `dim_cliente` descartada
**Decisión:** No se implementó una dimensión de clientes.

**Razón:** La API de MercadoLibre solo expone `id_cliente` y `nickname` en el endpoint de órdenes. Sin datos de ubicación, segmento o historial de compras, una `dim_cliente` no aportaría valor analítico. Se guardó `id_cliente` en `fact_ventas` para futura expansión cuando se integre el endpoint `/users/{id}`.

---

### `dim_categoria` descartada
**Decisión:** La jerarquía de categorías se desnormalizó directamente en `dim_producto` (`nivel_1`, `nivel_2`, `nombre_categoria`).

**Razón:** Las queries del dashboard siempre filtran por categoría junto con producto. Tener una dimensión separada requeriría un JOIN extra sin beneficio real dado el tamaño del dataset (~10k filas). La desnormalización simplifica las queries y mejora la performance en Databricks Serverless.

---

## Ingesta

### Auto Loader para Bronze
**Decisión:** Databricks Auto Loader detecta nuevos Parquets en el Volume y los ingesta en `ventas_raw`.

**Razón:** Auto Loader maneja automáticamente la detección de archivos nuevos, el tracking de qué archivos ya se procesaron (via checkpoints) y el schema evolution. Alternativas como un notebook que liste el Volume manualmente son más frágiles y requieren más código.

---

### File Arrival trigger en el Databricks Job
**Decisión:** El Job E2E se dispara cuando llega un archivo nuevo al Volume, no por schedule fijo.

**Razón:** Acopla el pipeline de transformación directamente al evento de ingesta. Si GitHub Actions falla y no llega un archivo, el pipeline de Databricks no corre innecesariamente. Es más eficiente y más correcto semánticamente que un schedule independiente.

---

## Mantenimiento Delta — OPTIMIZE, ZORDER y VACUUM

### OPTIMIZE + ZORDER en Gold y Silver
**Decisión:** Job semanal (domingos 3am ART) con `OPTIMIZE ... ZORDER BY (tiempo_key)` en `fact_ventas` y `ZORDER BY (fecha)` en `silver.ventas`.

**Razón:** El pipeline corre ~1 vez por hora generando un archivo Parquet nuevo por corrida. Sin OPTIMIZE, en una semana `fact_ventas` acumula ~168 archivos de 8 filas cada uno — las queries del dashboard tienen que abrirlos todos. OPTIMIZE los compacta en archivos grandes.

ZORDER reorganiza físicamente las filas dentro de los archivos para que datos con valores similares de `tiempo_key` queden juntos. Esto activa el data skipping de Delta: cuando el dashboard filtra por fecha, Databricks lee las estadísticas min/max de cada archivo del `_delta_log` y salta los archivos que no pertenecen al rango pedido sin abrirlos.

**Por qué ZORDER y no PARTITION BY:** Con ~10k filas y ~1.400 nuevas por mes, particionar por fecha generaría carpetas de unos pocos registros cada una — el small file problem empeoraría. La regla general en Databricks es no particionar tablas menores a 1TB. ZORDER logra el mismo data skipping sin fragmentar el storage.

**Por qué no Liquid Clustering:** Liquid Clustering es la evolución moderna del particionado — reorganiza datos incrementalmente en cada OPTIMIZE. Brilla en tablas de cientos de millones de filas. Para el volumen actual ZORDER hace exactamente lo mismo con menos complejidad. Si el negocio escala, la migración es un `ALTER TABLE fact_ventas CLUSTER BY (tiempo_key)` sin downtime.

---

### VACUUM en Bronze y Gold
**Decisión:** `VACUUM ... RETAIN 168 HOURS` en Bronze y Gold. Sin VACUUM en Silver.

**Razón:** OPTIMIZE crea archivos nuevos pero no borra los viejos — quedan como versiones históricas para time travel. VACUUM los elimina físicamente del storage después del período de retención.

168 horas (7 días) es el mínimo recomendado por Databricks. Bajar ese valor puede causar errores en lecturas concurrentes con Auto Loader o en queries largas que todavía referencian versiones anteriores.

Silver no tiene VACUUM intencional: puede regenerarse completamente desde Bronze si hay un bug en el ETL Gold. Mantener el historial de Silver da flexibilidad para hacer time travel y recuperar versiones anteriores sin necesidad de re-procesar Bronze.

---

## Próximos pasos (mejoras futuras)

- **ETL `/items/{id}`** — Enriquecer `dim_producto` con stock disponible, precio actual y estado del listing para detectar "productos clavo" (sin stock pero con demanda).
- **ETL `/users/{id}`** — Construir `dim_cliente` con ubicación geográfica para análisis por región.
- **ML forecasting** — Alimentar un modelo de series temporales con la fact table para predecir demanda por producto y automatizar sugerencias de reposición.