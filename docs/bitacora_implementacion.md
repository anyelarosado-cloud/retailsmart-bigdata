# RetailSmart Global: guía de implementación

Versión 1.0. Pasos para recrear la plataforma de datos de principio a fin en Google Cloud, con los comandos, la configuración y los resultados que debe producir cada paso. El diseño y su justificación están en `docs/RetailSmart_Arquitectura_Big_Data.docx`.

## 1. Alcance y arquitectura

La plataforma integra tres fuentes (thelook, Olist y UCI Online Retail) en un Lakehouse con tres zonas, procesa en batch con Spark y en streaming con Beam, y expone los resultados con una API y un panel web.

| Capa | Servicio | Equivalente abierto |
| --- | --- | --- |
| Ingesta streaming | Productor Python y Pub/Sub | Apache Kafka |
| Ingesta batch | Cloud Storage | Airflow y MinIO |
| Cómputo batch | Apache Spark en Dataproc | Apache Spark |
| Cómputo streaming | Apache Beam en Dataflow | Apache Flink |
| Almacenamiento | Cloud Storage con Apache Iceberg y BigQuery | MinIO con Iceberg, Trino o ClickHouse |
| Exposición | FastAPI en Cloud Run y panel web | FastAPI en Kubernetes |

| Zona | Equivalente | Contenido |
| --- | --- | --- |
| Landing | Bronze | Datos crudos de cada fuente, sin transformar |
| Universal | Silver | Datos limpios, tipados y sin duplicados, con una tabla de cuarentena por entidad |
| Smart | Gold | Esquema en estrella: tabla de hechos de ventas y cinco dimensiones conformadas |

Requisitos previos: una cuenta de Google con facturación y créditos, una cuenta de Kaggle (para descargar Olist) y un navegador. Los comandos se ejecutan en Cloud Shell. Los notebooks y scripts usan el ID de proyecto `retailsmart-bd`; si se usa otro, hay que reemplazarlo en ellos.

## 2. Entorno y convenciones

| Elemento | Valor |
| --- | --- |
| Proyecto | `retailsmart-bd` |
| Región | `us-central1` (BigQuery en `US`) |
| Bucket del lago | `gs://retailsmart-lake-retailsmart-bd` con `landing/` y tablas Iceberg en `lakehouse/` |
| Datasets de BigQuery | `retailsmart_raw` y `retailsmart_smart` |
| Pub/Sub | tópico `clicks`; suscripciones `clicks-sub` (inspección) y `clicks-beam` (pipeline) |
| Cluster de Spark | `spark-retail` (se crea al empezar a trabajar y se borra al terminar) |
| API | servicio de Cloud Run `api-retailsmart` |

Nombres de tablas Iceberg: `retailsmart.<zona>.<capa>_<dominio>_<entidad>_v<major>`, por ejemplo `retailsmart.smart.gold_comercial_fact_ventas_v1`. La zona es el esquema (`universal`, `smart`), y el dominio, `logistica` (Olist), `ecommerce` (thelook), `tienda` (UCI) o `comercial` (Smart).

Variables de sesión. Cloud Shell las pierde al reiniciarse, así que se repiten al retomar el trabajo:

```bash
export PROJECT_ID=retailsmart-bd
gcloud config set project $PROJECT_ID
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
export SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
export BUCKET=gs://retailsmart-lake-$PROJECT_ID
```

No se suben al repositorio claves de cuentas de servicio, tokens ni datos crudos.

## 3. Preparación del proyecto

### 3.1 Proyecto, facturación y presupuesto (consola)

1. Crear el proyecto `retailsmart-bd` y vincularlo a la cuenta de facturación con créditos.
2. **Facturación → Presupuestos y alertas → Crear presupuesto**:
   - Tipo **Alerts only** (avisa, no apaga servicios).
   - Alcance: solo el proyecto, todos los servicios, periodo mensual.
   - Desmarcar *Savings programs* y *Other savings*, para que el presupuesto cuente el gasto antes de aplicar créditos.
   - Importe: el máximo que se esté dispuesto a gastar, no el saldo de créditos. Umbrales de 50, 90 y 100 %, con aviso por correo.

### 3.2 Servicios, bucket, mensajería y datasets

```bash
gcloud services enable storage.googleapis.com compute.googleapis.com dataproc.googleapis.com \
  dataflow.googleapis.com pubsub.googleapis.com bigquery.googleapis.com \
  run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com

gcloud storage buckets create $BUCKET \
  --location=us-central1 --uniform-bucket-level-access --public-access-prevention

gcloud pubsub topics create clicks
gcloud pubsub subscriptions create clicks-sub --topic=clicks
gcloud pubsub subscriptions create clicks-beam --topic=clicks

bq --location=US mk --dataset $PROJECT_ID:retailsmart_raw
bq --location=US mk --dataset $PROJECT_ID:retailsmart_smart
```

Una suscripción solo recibe los mensajes publicados después de crearse, por lo que `clicks-beam` debe existir antes de publicar eventos.

### 3.3 Permisos y red

La cuenta de Compute Engine (`$SA`) ejecuta el cluster, los workers de Dataflow y las construcciones de Cloud Build. Roles con el alcance más estrecho posible:

```bash
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/dataproc.worker"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/dataflow.worker"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/bigquery.dataEditor"
gcloud storage buckets add-iam-policy-binding $BUCKET --member="serviceAccount:$SA" --role="roles/storage.objectAdmin"
gcloud pubsub subscriptions add-iam-policy-binding clicks-beam --member="serviceAccount:$SA" --role="roles/pubsub.subscriber"

gcloud artifacts repositories create cloud-run-source-deploy --repository-format=docker --location=us-central1
gcloud artifacts repositories add-iam-policy-binding cloud-run-source-deploy \
  --location=us-central1 --member="serviceAccount:$SA" --role="roles/artifactregistry.writer"

gcloud compute networks subnets update default --region=us-central1 --enable-private-ip-google-access
```

El acceso privado a las APIs de Google es necesario porque el cluster de Spark no tiene IP externa. `bigquery.dataEditor` se concede a nivel de proyecto por simplicidad; lo ideal es acotarlo al dataset `retailsmart_smart` desde la consola (dataset → *Sharing* → *Permissions*).

## 4. Ingesta batch a Landing

### 4.1 thelook (BigQuery público → Parquet)

```bash
for t in events orders order_items products users inventory_items distribution_centers; do
  bq cp -f bigquery-public-data:thelook_ecommerce.$t $PROJECT_ID:retailsmart_raw.$t
done
for t in events orders order_items products users inventory_items distribution_centers; do
  bq extract --destination_format=PARQUET $PROJECT_ID:retailsmart_raw.$t \
    $BUCKET/landing/thelook/$t/part-*.parquet
done
gcloud storage du --summarize --readable-sizes $BUCKET/landing/thelook/*
```

| Tabla | Filas |
| --- | --- |
| events | 2.429.664 |
| inventory_items | 490.408 |
| order_items | 181.644 |
| orders | 125.138 |
| users | 100.000 |
| products | 29.120 |
| distribution_centers | 10 |

Resultado esperado: siete carpetas en `landing/thelook/`, unos 258 MiB en total. Los conteos pueden verificarse sin consultar los datos con `bq show --format=prettyjson <tabla> | grep numRows`.

### 4.2 UCI Online Retail

El archivo original es un Excel; se conserva intacto y se guarda además una copia en CSV sin limpiar. `InvoiceNo` y `StockCode` se leen como texto, porque las facturas que empiezan con "C" son cancelaciones.

```bash
mkdir -p ~/datos && cd ~/datos
wget -O uci.zip https://archive.ics.uci.edu/static/public/352/online+retail.zip
unzip -o uci.zip
pip install --user pandas openpyxl
python3 -c "
import pandas as pd
df = pd.read_excel('Online Retail.xlsx', dtype={'InvoiceNo': str, 'StockCode': str})
print(len(df))
df.to_csv('online_retail.csv', index=False)
"
gcloud storage cp "Online Retail.xlsx" $BUCKET/landing/uci/original/
gcloud storage cp online_retail.csv $BUCKET/landing/uci/
```

Resultado esperado: se imprime `541909`; en Landing quedan el original (22,6 MiB) y el CSV (45,8 MiB).

### 4.3 Olist

Kaggle exige iniciar sesión, por lo que la descarga es manual: descargar el dataset de Olist (Brazilian E-Commerce), descomprimirlo y subir los nueve CSV a `landing/olist/` desde la consola (**Cloud Storage → bucket → `landing` → Create folder `olist` → Upload files**).

```bash
gcloud storage ls -l $BUCKET/landing/olist/
```

Resultado esperado: nueve archivos, 120 MiB en total (clientes, geolocalización, ítems, pagos, reseñas, pedidos, productos, vendedores y traducción de categorías).

## 5. Procesamiento batch con Spark

### 5.1 Cluster y Jupyter

```bash
gcloud dataproc clusters create spark-retail \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --single-node \
  --master-machine-type=n4-standard-4 \
  --master-boot-disk-type=hyperdisk-balanced \
  --master-boot-disk-size=100 \
  --enable-component-gateway \
  --subnet=default \
  --no-address \
  --image-version=2.3-debian12 \
  --optional-components=ICEBERG,DELTA,JUPYTER \
  --scopes='https://www.googleapis.com/auth/cloud-platform' \
  --max-idle=30m
```

Abrir JupyterLab desde la consola (**Dataproc → Clusters → `spark-retail` → Web Interfaces → JupyterLab**), subir los notebooks de `spark/` y usar el kernel PySpark. El aviso de firewall que muestra la creación del cluster proviene de las reglas por defecto de la red. Mientras el cluster esté encendido genera costo, y `--max-idle=30m` lo apaga tras 30 minutos de inactividad; aun así debe borrarse al terminar (sección 9).

### 5.2 Celda inicial de cada notebook

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
BUCKET = "gs://retailsmart-lake-retailsmart-bd"
spark.conf.set("spark.sql.catalog.retailsmart", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.retailsmart.type", "hadoop")
spark.conf.set("spark.sql.catalog.retailsmart.warehouse", f"{BUCKET}/lakehouse")
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.sql("CREATE NAMESPACE IF NOT EXISTS retailsmart.universal")
```

El almacén del catálogo debe ser una subcarpeta del bucket (`lakehouse`) y no la raíz. En el notebook de Smart se crea además el esquema `retailsmart.smart`. Las tablas quedan en `gs://.../lakehouse/universal/` y `gs://.../lakehouse/smart/`.

### 5.3 Notebooks

Se ejecutan en orden, con el cluster encendido.

| Notebook | Qué hace |
| --- | --- |
| `00_prueba_landing` | Lee `olist_orders_dataset.csv` desde Landing y comprueba que Spark accede al bucket (99.441 pedidos, 8 columnas) |
| `01_olist_pedidos` | Pedidos de Olist: Landing → Universal |
| `02_olist_tablas` | Resto de Olist: Landing → Universal |
| `03_uci_ventas` | UCI: Landing → Universal |
| `04_thelook` | thelook: Landing → Universal |
| `05_smart_dimensiones` | Dimensiones, tabla de hechos y publicación en BigQuery |

Patrón de la zona Universal. Cada entidad se lee, se aplican reglas de calidad y se separa en dos tablas: `silver_<dominio>_<entidad>_v1` y `cuarentena_<dominio>_<entidad>_v1` (con una columna `motivo`). Los duplicados se eliminan por clave de negocio, y se añaden `_fuente` y `_cargado_en` como columnas de trazabilidad. Si varias reglas fallan en un registro, se informa la primera. Las escrituras usan `writeTo(...).using("iceberg").createOrReplace()`, por lo que cada notebook puede repetirse sin duplicar datos.

### 5.4 Universal: Olist

Reglas de `pedidos`: `order_id` y fecha de compra no nulos, estado dentro del catálogo (`created`, `approved`, `invoiced`, `processing`, `shipped`, `delivered`, `unavailable`, `canceled`) y entrega no anterior a la compra. Los códigos postales se rellenan a cinco dígitos con ceros. Las reseñas se leen con `multiLine=True` y `escape='"'`, porque los comentarios contienen saltos de línea y comillas.

| Entidad | Tabla Silver (`retailsmart.universal.`) | Leídas | Válidas | Rechazadas |
| --- | --- | --- | --- | --- |
| Pedidos | `silver_logistica_pedidos_v1` | 99.441 | 99.441 | 0 |
| Ítems de pedido | `silver_logistica_items_pedido_v1` | 112.650 | 112.650 | 0 |
| Pagos | `silver_logistica_pagos_v1` | 103.886 | 103.883 | 3 |
| Clientes | `silver_logistica_clientes_v1` | 99.441 | 99.441 | 0 |
| Vendedores | `silver_logistica_vendedores_v1` | 3.095 | 3.095 | 0 |
| Productos | `silver_logistica_productos_v1` | 32.951 | 32.951 | 0 |
| Categorías | `silver_logistica_categorias_v1` | 71 | 71 | 0 |
| Geolocalización | `silver_logistica_geolocalizacion_v1` | 1.000.163 | 738.299 | 42 (más 261.822 duplicados) |
| Reseñas | `silver_logistica_resenas_v1` | 99.224 | 99.224 | 0 |

Reglas del resto: ítems (identificadores no nulos, precio y flete no negativos); pagos (monto no negativo y tipo de pago definido; los 3 rechazos son pagos `not_defined` por valor 0); clientes y vendedores (identificadores no nulos); productos (peso no negativo; la categoría nula se conserva); geolocalización (coordenadas no nulas y dentro de un recuadro aproximado de Brasil, con latitud entre -34 y 6 y longitud entre -74 y -34); reseñas (puntaje entre 1 y 5, clave `review_id` más `order_id`).

### 5.5 Universal: UCI

Las columnas se renombran (`factura`, `codigo_producto`, `descripcion`, `cantidad`, `fecha_factura`, `precio_unitario`, `cliente_id`, `pais`). `cliente_id` se convierte a texto, porque el CSV lo trae como decimal (`17850.0`), y `es_cancelacion` es verdadero para las facturas que empiezan con "C". Las cancelaciones se conservan marcadas. Se rechazan los registros con factura, código o fecha nulos, precio negativo o cero, o cantidad no positiva fuera de cancelaciones.

Tablas: `silver_tienda_ventas_v1` y `cuarentena_tienda_ventas_v1`. Resultado esperado: 541.909 leídas, 534.129 válidas, 2.517 rechazadas (2.515 por precio cero y 2 por precio negativo) y 5.263 duplicados. En las válidas, 132.565 líneas no identifican al cliente y se conservan con `cliente_id` nulo.

### 5.6 Universal: thelook

Las fechas `timestamp_ntz` se convierten a `timestamp` y se descartan las columnas binarias de geometría (`user_geom`, `distribution_center_geom`), porque `latitude` y `longitude` ya contienen la ubicación. `users` conserva datos personales en esta zona; se enmascaran en Smart.

| Entidad | Tabla Silver (`retailsmart.universal.`) | Leídas | Válidas | Rechazadas |
| --- | --- | --- | --- | --- |
| Usuarios | `silver_ecommerce_usuarios_v1` | 100.000 | 100.000 | 0 |
| Pedidos | `silver_ecommerce_pedidos_v1` | 125.138 | 125.138 | 0 |
| Ítems de pedido | `silver_ecommerce_items_pedido_v1` | 181.644 | 181.644 | 0 |
| Productos | `silver_ecommerce_productos_v1` | 29.120 | 29.120 | 0 |
| Inventario | `silver_ecommerce_inventario_v1` | 490.408 | 490.408 | 0 |
| Centros de distribución | `silver_ecommerce_centros_distribucion_v1` | 10 | 10 | 0 |
| Eventos | `silver_ecommerce_eventos_v1` | 2.429.664 | 2.429.664 | 0 |

En `items_pedido`, una parte de los registros trae secuencias de fechas imposibles (envío anterior a la creación, o entrega anterior al envío). Esos ítems tienen precio, producto y pedido válidos, por lo que se conservan y se marcan con la columna booleana `fechas_inconsistentes` en lugar de rechazarlos. La cuarentena de esa tabla queda para errores que impiden usar el registro (identificadores nulos o precio inválido). Los análisis de tiempos de entrega deben excluir los marcados o usar las fechas de `pedidos`, que son coherentes.

### 5.7 Smart: dimensiones y tabla de hechos

Las claves sustitutas son `xxhash64(fuente, id_origen)`: estables entre ejecuciones y sin secuencias. Las tablas se llaman `retailsmart.smart.gold_comercial_<tabla>_v1`.

| Tabla | Filas | Contenido |
| --- | --- | --- |
| `dim_fecha` | 6.939 | Calendario diario de 2009-01-01 a 2027-12-31 (`fecha_sk` en formato aaaammdd) |
| `dim_canal` | 3 | thelook = web, Olist = marketplace, UCI = tienda_mayorista |
| `dim_producto` | 66.009 | Un producto por fuente, con unidad de negocio: 32.951 Olist, 29.120 thelook y 3.938 códigos UCI |
| `dim_cliente` | 200.467 | Olist por `customer_unique_id`, thelook por usuario y UCI por `cliente_id`; el correo se guarda como hash y no se incluyen nombres ni direcciones |
| `dim_geo` | 68 | País y, para Brasil, estado |
| `fact_ventas` | 828.423 | Una fila por línea de venta |

**Unidades de negocio.** Las categorías de Olist se asignan por palabras clave a Hogar y decoración, Ocio/deportes/cultura, Electrónica y electrodomésticos, Belleza/salud/bebés, Construcción/auto/industria, Moda y accesorios, Mascotas, Alimentos y bebidas, Sin categoría y Otros. thelook se asigna a Moda y accesorios, y UCI a Hogar y decoración.

**Geografía.** Los nombres de país se normalizan entre fuentes (`Brasil`→`Brazil`, `España`→`Spain`, `USA`→`United States`, `EIRE`→`Ireland`, `RSA`→`South Africa`) y los estados de thelook, que vienen con nombre completo, se convierten a las 27 siglas que usa Olist. Las ocho regiones logísticas se definen en el modelo:

| Región | Filas de `dim_geo` |
| --- | --- |
| Brasil Norte (AC, AP, AM, PA, RO, RR, TO) | 7 |
| Brasil Nordeste (AL, BA, CE, MA, PB, PE, PI, RN, SE) | 9 |
| Brasil Centro-Oeste (DF, GO, MT, MS) | 4 |
| Brasil Sudeste (ES, MG, RJ, SP) | 4 |
| Brasil Sur (PR, RS, SC) | 3 |
| Europa | 24 |
| Norteamérica | 2 |
| Asia-Pacífico y resto | 13 |
| Sin asignar (UCI en Brasil sin estado y `Unspecified`) | 2 |

**Tabla de hechos.** Columnas: `fuente`, `id_pedido`, `id_linea`, `fecha_sk`, `producto_sk`, `cliente_sk`, `geo_sk`, `canal_sk`, `cantidad`, `monto`, `costo_envio` (solo Olist), `moneda`, `estado` y `fechas_inconsistentes`. En thelook cada ítem es una unidad y la fecha es la del pedido; en Olist el cliente es la persona y no el pedido; en UCI el monto es cantidad por precio, de modo que las cancelaciones son negativas. El contrato de datos está en `contratos/fact_ventas.yaml`.

| Fuente | Moneda | Filas | Monto total |
| --- | --- | --- | --- |
| Olist | BRL | 112.650 | 13.591.643,70 |
| thelook | USD | 181.644 | 10.843.606,01 |
| UCI | GBP | 534.129 | 9.748.131,07 |

Los montos se conservan en la moneda de origen y no se suman entre fuentes. Verificación de integridad: cada clave de `fact_ventas` (excepto `cliente_sk` nulo) existe en su dimensión; las filas huérfanas contra `dim_producto`, `dim_geo`, `dim_fecha` y `dim_cliente` deben ser 0.

### 5.8 Publicación en BigQuery

```python
TABLAS = ["dim_fecha", "dim_canal", "dim_producto", "dim_cliente", "dim_geo", "fact_ventas"]
for t in TABLAS:
    (spark.table(f"retailsmart.smart.gold_comercial_{t}_v1").write.format("bigquery")
        .option("table", f"retailsmart-bd.retailsmart_smart.{t}")
        .option("temporaryGcsBucket", "retailsmart-lake-retailsmart-bd")
        .mode("overwrite").save())
```

Verificación (Cloud Shell):

```bash
for t in dim_fecha dim_canal dim_producto dim_cliente dim_geo fact_ventas; do
  echo "== $t"; bq show --format=prettyjson $PROJECT_ID:retailsmart_smart.$t | grep numRows
done

bq query --use_legacy_sql=false '
SELECT g.region, f.moneda, COUNT(*) AS lineas, ROUND(SUM(f.monto)) AS monto
FROM `retailsmart-bd.retailsmart_smart.fact_ventas` f
JOIN `retailsmart-bd.retailsmart_smart.dim_geo` g USING (geo_sk)
GROUP BY 1, 2 ORDER BY lineas DESC'
```

Resultado esperado: 6.939, 3, 66.009, 200.467, 68 y 828.423 filas; la consulta devuelve 17 combinaciones de región y moneda, con 112.650 líneas en BRL, 181.644 en USD y 534.129 en GBP.

## 6. Streaming

### 6.1 Productor de eventos

`ingesta/productor_eventos.py` lee eventos de `retailsmart_raw.events`, los une con `retailsmart_raw.users` para añadir `pais` y `estado` (normalizando `Brasil` y `España`), reemplaza la marca de tiempo por la hora actual (`event_ts`) y los publica en el tópico `clicks` con el atributo `fuente=thelook`. La muestra es aleatoria (`ORDER BY RAND() LIMIT n`). Parámetros: `--n` (cantidad de mensajes) y `--rate` (mensajes por segundo).

```bash
pip install --user google-cloud-pubsub google-cloud-bigquery
python3 ingesta/productor_eventos.py --n 2000 --rate 100
gcloud pubsub subscriptions pull clicks-sub --auto-ack --limit=3
```

Resultado esperado: se imprime `publicados 2000 mensajes`, y los mensajes son JSON con `event_id`, `user_id`, `session_id`, `event_type`, `uri`, `browser`, `traffic_source`, `pais`, `estado` y `event_ts`, con tipos de evento y países variados. Tipos de evento en la tabla original: `product` 844.954, `department` 595.408, `cart` 595.012, `purchase` 181.644, `cancel` 125.169 y `home` 87.477; las visitas a producto traen `uri` con el formato `/product/<id>`. Entre los eventos con usuario conocido, `product` representa el 26,5 %.

### 6.2 Pipeline de agregados (Apache Beam)

`streaming/agregados_eventos.py` (Beam 2.77.0) lee la suscripción `clicks-beam`, agrupa en ventanas fijas de `--ventana_seg` segundos y escribe con inserciones en streaming dos tablas de `retailsmart_smart`: `rt_eventos_ventana` (país y tipo de evento) y `rt_productos_ventana` (visitas por país y producto, extraído del `uri`). Las tablas se crean si no existen.

```bash
pip install --user "apache-beam[gcp]"
```

**Prueba local.** El runner local lee de Pub/Sub 10 mensajes por consulta, lo que limita su rendimiento a unos pocos mensajes por segundo; se usa para validar la lógica con un ritmo bajo (5 por segundo), y el rendimiento se mide en Dataflow. Se usa `BundleBasedDirectRunner`, el runner clásico, que soporta la lectura de Pub/Sub.

```bash
pkill -f agregados_eventos.py
bq rm -f -t retailsmart_smart.rt_eventos_ventana
bq rm -f -t retailsmart_smart.rt_productos_ventana
gcloud pubsub subscriptions delete clicks-beam
gcloud pubsub subscriptions create clicks-beam --topic=clicks
gcloud pubsub subscriptions add-iam-policy-binding clicks-beam --member="serviceAccount:$SA" --role="roles/pubsub.subscriber"

cd ~/retailsmart
nohup python3 streaming/agregados_eventos.py --runner=BundleBasedDirectRunner --project=retailsmart-bd --ventana_seg=30 > /tmp/beam.log 2>&1 &
sleep 45
python3 ingesta/productor_eventos.py --n 300 --rate 5
sleep 45
python3 ingesta/productor_eventos.py --n 10 --rate 5
sleep 60
pkill -f agregados_eventos.py
```

Siempre se detiene el pipeline en marcha (`pkill`) antes de borrar o recrear la suscripción. Los 10 mensajes finales hacen avanzar el tiempo para que Beam cierre la última ventana.

Consulta de totales (se usa también con Dataflow):

```bash
bq query --use_legacy_sql=false '
SELECT "eventos" AS tabla, ventana_inicio, SUM(eventos) AS total
FROM `retailsmart-bd.retailsmart_smart.rt_eventos_ventana` GROUP BY 2
UNION ALL
SELECT "productos", ventana_inicio, SUM(vistas)
FROM `retailsmart-bd.retailsmart_smart.rt_productos_ventana` GROUP BY 2
ORDER BY 2 DESC, 1'
```

Resultado esperado: la suma de `eventos` es **310** (lo publicado), repartida en tres o cuatro ventanas de 30 segundos. Las visitas a producto son cerca del 26 % (por ejemplo 75 de 310), y coinciden con la suma de eventos `product` en `rt_eventos_ventana`.

### 6.3 Dataflow

Con los permisos de la sección 3.3 y las tablas `rt_*` y el pipeline local detenidos (no debe haber dos consumidores sobre `clicks-beam`):

```bash
bq rm -f -t retailsmart_smart.rt_eventos_ventana
bq rm -f -t retailsmart_smart.rt_productos_ventana
cd ~/retailsmart
python3 streaming/agregados_eventos.py \
  --runner=DataflowRunner --project=retailsmart-bd --region=us-central1 \
  --temp_location=$BUCKET/tmp/dataflow --staging_location=$BUCKET/tmp/staging \
  --job_name=agregados-eventos --num_workers=1 --max_num_workers=1 --ventana_seg=60
gcloud dataflow jobs list --region=us-central1 --status=active
```

El job `agregados-eventos` aparece como `Running` en 3 a 6 minutos. Con un solo worker se acota el costo; el job cobra mientras esté activo.

```bash
cd ~/retailsmart
python3 ingesta/productor_eventos.py --n 3000 --rate 100
sleep 60
python3 ingesta/productor_eventos.py --n 20 --rate 20
sleep 60
```

Se ejecuta la consulta de totales de 6.2. Resultado esperado: **3.020 eventos** (ventanas de 60 segundos con 300, 2.700 y 20) y las visitas a producto son cerca del 26 % del total (769 en la ejecución de referencia). Un worker procesa 100 mensajes por segundo sin pérdidas; no se ha probado un ritmo mayor ni más de un worker.

Al terminar, se cancela el job:

```bash
gcloud dataflow jobs cancel $(gcloud dataflow jobs list --region=us-central1 --status=active --format="value(JOB_ID)") --region=us-central1
```

De la página del job en la consola (**Dataflow → Jobs → `agregados-eventos`**) se anotan la duración y los recursos consumidos (vCPU-horas, memoria y datos procesados) para la calculadora de consumo.

## 7. API y panel web

### 7.1 Contenido de `api/`

| Archivo | Función |
| --- | --- |
| `main.py` | FastAPI con consultas parametrizadas a BigQuery y la ruta `/` que sirve el panel |
| `panel.html` | Panel de una sola página, sin dependencias externas |
| `requirements.txt` | `fastapi`, `uvicorn[standard]` y `google-cloud-bigquery` con versiones fijas |
| `Dockerfile` | Imagen de Python 3.12 con `main.py` y `panel.html` |
| `test_api.py` | Siete pruebas con un cliente de BigQuery simulado |

| Ruta | Respuesta |
| --- | --- |
| `GET /` | Panel web |
| `GET /v1/salud` | Estado del servicio |
| `GET /v1/ventas/resumen?region=&moneda=` | Líneas y monto por región y moneda |
| `GET /v1/ventas/por-unidad?moneda=&limite=` | Líneas y monto por unidad de negocio |
| `GET /v1/tiempo-real/eventos?minutos=5&pais=` | Eventos por tipo en los últimos minutos (1 a 1440) |
| `GET /v1/tiempo-real/productos-top?minutos=5&pais=&limite=10` | Productos más vistos, con nombre y unidad |

La documentación interactiva queda en `/docs`. Pruebas locales: `pip install -r requirements.txt pytest httpx && python -m pytest -q test_api.py` (siete pruebas pasan). Estas pruebas validan rutas, parámetros y nombres de tablas y columnas, pero no ejecutan el SQL contra BigQuery.

### 7.2 Cuenta de servicio y despliegue

Una cuenta propia de solo lectura para la API:

```bash
gcloud iam service-accounts create api-retailsmart --display-name="API RetailSmart"
export SA_API="api-retailsmart@${PROJECT_ID}.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_API" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_API" --role="roles/bigquery.dataViewer"

cd api
gcloud run deploy api-retailsmart --source . --region us-central1 \
  --service-account=$SA_API --no-allow-unauthenticated --max-instances=2 --memory=512Mi
```

El servicio exige autenticación y limita a 2 instancias para acotar el costo.

### 7.3 Pruebas contra BigQuery

```bash
URL=$(gcloud run services describe api-retailsmart --region us-central1 --format='value(status.url)')
TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $TOKEN" "$URL/v1/salud"
curl -s -H "Authorization: Bearer $TOKEN" "$URL/v1/ventas/resumen?moneda=BRL"
curl -s -H "Authorization: Bearer $TOKEN" "$URL/v1/tiempo-real/eventos?minutos=600"
curl -s -H "Authorization: Bearer $TOKEN" "$URL/v1/tiempo-real/productos-top?minutos=600&limite=5"
```

Resultado esperado: `{"estado":"ok"}`; cinco regiones de Brasil con 77.413, 16.151, 10.409, 6.613 y 2.064 líneas (112.650 en total) y montos que suman 13.591.643,70 BRL; los eventos de las últimas 10 horas con el total publicado (por ejemplo 3.020 tras la prueba de Dataflow, siempre que esos datos estén dentro de la ventana); y cinco productos de thelook con nombre y unidad de negocio. Los endpoints de tiempo real devuelven listas vacías si no hubo eventos en la ventana pedida.

### 7.4 Panel

El panel muestra eventos por tipo y productos más vistos (se refresca cada 5 segundos, con ventana y país seleccionables) y las ventas por región y moneda, con montos de dos decimales. Como el servicio exige autenticación, se abre con un proxy que añade las credenciales:

```bash
gcloud run services proxy api-retailsmart --region us-central1
```

En Cloud Shell, **Vista previa en la Web → Preview on port 8080** abre el panel; si el puerto está ocupado, se usa `--port=8081` y se cambia el puerto de la vista previa. Con la ventana de 5 minutos y sin pipeline en marcha, los paneles de tiempo real muestran "Sin eventos en esta ventana".

Ensayo sin costo de Dataflow, con el proxy abierto: se inicia el pipeline local y se publica a ritmo bajo (secciones 6.2 y 6.1). Con 300 mensajes publicados, la suma de eventos por tipo en el panel es 300. La demostración con volumen usa Dataflow (6.3) y 100 mensajes por segundo.

## 8. Verificación de extremo a extremo

| Comprobación | Resultado esperado |
| --- | --- |
| Landing | thelook 258 MiB en 7 carpetas; UCI con 541.909 líneas; Olist con 9 archivos |
| Universal | Tablas Silver y de cuarentena según las secciones 5.4 a 5.6 |
| `fact_ventas` | 828.423 filas (181.644 thelook, 112.650 Olist, 534.129 UCI), con 0 filas huérfanas |
| BigQuery | Seis tablas Smart con 6.939, 3, 66.009, 200.467, 68 y 828.423 filas |
| Streaming local | 310 de 310 mensajes agregados a 5 por segundo |
| Dataflow | 3.020 de 3.020 mensajes agregados a 100 por segundo con un worker |
| API | Cinco endpoints con cifras iguales a las de `fact_ventas` y a los agregados de streaming |
| Panel | Eventos por tipo que suman lo publicado, productos con nombre y ventas por región |

## 9. Operación y costos

Al terminar cada sesión de trabajo se apaga todo lo que cobra por tiempo de ejecución:

```bash
gcloud dataproc clusters delete spark-retail --region=us-central1
gcloud dataflow jobs cancel $(gcloud dataflow jobs list --region=us-central1 --status=active --format="value(JOB_ID)") --region=us-central1
pkill -f agregados_eventos.py
```

Y se comprueba que no quede nada activo:

```bash
gcloud dataproc clusters list --region=us-central1
gcloud dataflow jobs list --region=us-central1 --status=active
pgrep -f agregados_eventos.py
```

Los dos primeros deben decir `Listed 0 items.` y el tercero no imprime nada. El proxy de Cloud Run se cierra con `Ctrl + C`. Los datos y las tablas viven en el bucket y en BigQuery, por lo que no se pierden al borrar el cluster; Cloud Run cobra por uso y no mantiene instancias encendidas sin tráfico; el servicio está limitado a 2 instancias.

Esta guía no incluye cifras de costo. Para la calculadora de consumo, los costos reales se toman de **Facturación → Informes** (agrupado por servicio y por día) y de la página del job de Dataflow.

## 10. Supuestos y limitaciones

- Los montos se conservan en la moneda de origen (BRL en Olist, GBP en UCI y USD en thelook, supuesto) y no se suman entre fuentes; consolidarlos exige una tabla de tipos de cambio.
- Las ocho regiones logísticas no vienen en los datos: se definen en el modelo (cinco macrorregiones de Brasil según el IBGE, Norteamérica, Europa y Asia-Pacífico y resto).
- UCI es venta mayorista en línea y se usa como sustituto de la tienda física. Todo UCI se asigna a la unidad Hogar y decoración, y cerca de una cuarta parte de sus líneas no identifica al cliente.
- Las fuentes son sobre todo complementarias: thelook solo aporta Moda en USD, Olist solo Brasil en BRL y UCI casi solo Europa en GBP. Brasil Sudeste con Moda es la única combinación con dos fuentes.
- Los datos de clics se reproducen a partir de thelook con marcas de tiempo actuales; no hay sensores IoT ni GPS reales.
- El rendimiento de streaming se probó con un worker a 100 mensajes por segundo.
- El catálogo Iceberg es de tipo Hadoop; en producción debe sustituirse por un catálogo gestionado (REST o metastore) para soportar escrituras concurrentes sobre almacenamiento de objetos.
- Los permisos de la cuenta de Compute Engine incluyen `bigquery.dataEditor` a nivel de proyecto; lo ideal es acotarlo al dataset de Smart y usar una cuenta de servicio por componente.
- El contrato de datos de `fact_ventas` es una especificación: sus reglas y compromisos de servicio no se validan automáticamente.
- El panel y la API exigen autenticación y no están expuestos públicamente.

## 11. Estructura del repositorio

```
api/          API FastAPI, panel web, Dockerfile y pruebas
contratos/    contrato de datos de fact_ventas
docs/         documento de arquitectura y esta guía
ingesta/      productor de eventos hacia Pub/Sub
spark/        notebooks 00 a 05
streaming/    pipeline Apache Beam de agregados por ventana
```
