# RetailSmart Global: plataforma de datos

Back-end de datos para RetailSmart: integra tres fuentes públicas y tres fuentes simuladas en un Lakehouse, procesa en batch y en streaming, pronostica la demanda, sugiere reposición y expone los resultados mediante una API, un panel web y un asistente de IA generativa.

Proyecto de Google Cloud `retailsmart-bd`, región `us-central1`.

## Arquitectura

| Capa | Implementación (Google Cloud) | Equivalente abierto |
| --- | --- | --- |
| Ingesta streaming | Productor Python y Pub/Sub | Apache Kafka |
| Ingesta batch | Carga de archivos a Cloud Storage | Airflow y MinIO |
| Cómputo batch | Apache Spark en Dataproc | Apache Spark |
| Cómputo streaming | Apache Beam en Dataflow: agregados de clics y alertas de frío | Apache Flink |
| Almacenamiento | Cloud Storage con tablas Apache Iceberg (Landing, Universal, Smart) y BigQuery | MinIO con Iceberg, Trino o ClickHouse |
| Pronóstico y reposición | BigQuery ML (ARIMA_PLUS) y tabla `plan_reposicion` | Prophet o statsforecast |
| Exposición | FastAPI en Cloud Run y panel web | FastAPI en Kubernetes |
| Asistente | Gemini en Vertex AI con herramientas sobre BigQuery | Modelo abierto con llamadas a herramientas |
| Red | VPC `vpc-retailsmart` con subred `sn-datos`, sin IP externa y salida por Cloud NAT | Red privada con NAT |
| Orquestación del lote | Diseño de producción con Cloud Composer (no implementado) | Airflow |

Zonas del lago: **Landing** (crudo e inmutable), **Universal** (limpio, tipado y sin duplicados, con tabla de cuarentena por entidad) y **Smart** (esquema en estrella: `fact_ventas` y cinco dimensiones conformadas).

## Fuentes

Ventas, de tres mercados y con tres datasets públicos:

- `thelook_ecommerce` (BigQuery público): tienda web, clics y centros de distribución.
- Olist (Kaggle): marketplace de Brasil, con logística y última milla.
- UCI Online Retail: ventas del Reino Unido, usadas como sustituto de la tienda física.

Fuentes simuladas, generadas de forma reproducible (`ingesta/simulacion/`):

- ERP de inventario: stock y plazos de reposición por centro de distribución, a partir del inventario de thelook.
- Cámaras de frío: 40 cámaras con temperatura, compresor y puerta, con fallas inyectadas.
- GPS de reparto: 60 vehículos que parten de los centros de distribución.

Los clics se reproducen hacia Pub/Sub con marcas de tiempo actuales (`ingesta/productor_eventos.py`). Los centros de distribución comparten `centro_id` en el stock, los sensores y el GPS.

## Estructura

```
api/          API FastAPI, panel web, asistente de reposición (agente.py), Dockerfile y pruebas
contratos/    contrato de datos de fact_ventas y reglas de calidad
ingesta/      productor de clics y simulador de ERP, sensores y GPS
spark/        notebooks de Landing -> Universal -> Smart (00 a 05)
sql/          metadatos de Smart, consultas de verificación y pronóstico de demanda (sql/ml)
streaming/    pipelines Apache Beam: agregados por ventana, alertas de frío y notificación por correo
docs/         guía del demo, estimación de costos y entregables (informe y presentación)
```

## Resultados verificados

| Componente | Resultado |
| --- | --- |
| Landing | thelook en Parquet (7 tablas), UCI (541.909 líneas) y Olist (9 archivos) |
| Smart | `fact_ventas` con 828.423 líneas (181.644 thelook, 112.650 Olist, 534.129 UCI); `dim_fecha` 6.939, `dim_canal` 3, `dim_producto` 66.009, `dim_cliente` 200.467, `dim_geo` 68 |
| Streaming | Prueba en Dataflow con un worker: 3.020 de 3.020 mensajes agregados a 100 por segundo |
| ERP simulado | `erp_stock` y `erp_plazos` con 29.060 productos; cobertura de 7,5 a 22,5 días por centro; `demanda_diaria_centro` sin huecos |
| Alertas de frío | 8 de 8 fallas inyectadas detectadas; 23 alertas, ninguna sin falla; tiempo medio de detección de 51 s; 174 lecturas imposibles en `rt_errores` |
| Catálogo | Las seis tablas de Smart con descripción de tabla y de columna, y etiquetas de dominio, zona y sensibilidad |
| Pronóstico | ARIMA_PLUS por centro, entrenado hasta el 10 de octubre de 2026; WAPE de 0,238 frente a 0,209 de la línea base de 28 días, por lo que la meta no se cumple |
| Plan de reposición | 606 unidades sugeridas en 123 de 256 combinaciones de centro y categoría; el asistente responde con las mismas cifras de la tabla |
| Red y autoescalado | Los jobs `agregados-eventos-vpc` y `alertas-frio` corren en `sn-datos` sin IP externa; el de clics escaló de uno a tres workers bajo carga |
| Notificación | La alerta de sobretemperatura llega por correo desde Cloud Monitoring (`streaming/ALERTAS_POR_CORREO.md`) |
| API | Endpoints probados contra BigQuery; las cifras coinciden con `fact_ventas` y con los agregados de streaming |

# Demo de extremo a extremo

Flujo: clics y sensores publicados en Pub/Sub, procesados por Dataflow, escritos en BigQuery y vistos en el panel, con una alerta de frío por correo y preguntas al asistente de reposición.

## 1. Revisar qué está encendido

```bash
gcloud dataflow jobs list --region=us-central1 --status=active
gcloud run services list --region=us-central1
gcloud dataproc clusters list --region=us-central1
```

## 2. Iniciar los servicios

Jobs de Dataflow (cada comando queda ejecutándose mientras el job vive; el job continúa en Google Cloud aunque Cloud Shell se cierre). El job aparece como `Running` entre 3 y 6 minutos después de lanzarse:

```bash
# Clics
python3 streaming/agregados_eventos.py --runner=DataflowRunner --project=retailsmart-bd --region=us-central1 \
  --temp_location=gs://retailsmart-lake-retailsmart-bd/tmp/dataflow \
  --staging_location=gs://retailsmart-lake-retailsmart-bd/tmp/staging \
  --job_name=agregados-eventos-vpc --network=vpc-retailsmart \
  --subnetwork=regions/us-central1/subnetworks/sn-datos --no_use_public_ips \
  --num_workers=1 --max_num_workers=3 --autoscaling_algorithm=THROUGHPUT_BASED --ventana_seg=60

# Alertas de frío
python3 streaming/alertas_frio.py --runner=DataflowRunner --project=retailsmart-bd --region=us-central1 \
  --temp_location=gs://retailsmart-lake-retailsmart-bd/tmp/dataflow \
  --staging_location=gs://retailsmart-lake-retailsmart-bd/tmp/staging \
  --job_name=alertas-frio --num_workers=1 --max_num_workers=1 --ventana_seg=30 \
  --subnetwork=regions/us-central1/subnetworks/sn-datos --no_use_public_ips
```

Si un job con el mismo nombre sigue activo, Dataflow responde que ya existe: cancélalo antes con `gcloud dataflow jobs cancel <ID> --region=us-central1`.

Cloud Run sin arranque en frío durante la demo:

```bash
gcloud run services update api-retailsmart --region=us-central1 --min-instances=1
```

## 3. Generar tráfico

```bash
# Clics continuos durante 15 minutos
python3 ingesta/productor_eventos.py --duracion 900 --rate 40

# Sensores de frío con fallas inyectadas (desde ingesta/simulacion)
python3 generador_iot.py --ticks 150
```

El primer correo de alerta llega uno o dos minutos después de la primera alerta de sobretemperatura y luego como máximo uno cada cinco minutos. No repitas la prueba de correo antes de que pasen diez minutos de la anterior.

## 4. Abrir el panel

```bash
gcloud config set project retailsmart-bd
gcloud run services proxy api-retailsmart --region us-central1    # panel en http://localhost:8080/
```

El proxy desde Cloud Shell se cae tras unos 20 minutos de inactividad; desde la laptop se mantiene. El panel se actualiza solo cada cinco segundos; las ventas se cargan una vez. Los agregados de clics usan ventanas de 60 segundos, así que el panel va uno o dos minutos por detrás: selecciona «Últimos 15 minutos».

## 5. Detener todo al terminar

```bash
gcloud dataflow jobs list --region=us-central1 --status=active --format="value(JOB_ID)" \
  | xargs -r gcloud dataflow jobs cancel --region=us-central1
gcloud run services update api-retailsmart --region=us-central1 --min-instances=0
gcloud dataproc clusters list --region=us-central1      # debe estar vacío
```

## Equipo

Bryan Guerra Suarez    
Juan Carlos Vasquez    
Anyela Rosado     
Rodrigo Rodriguez
