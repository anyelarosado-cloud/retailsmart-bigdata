# RetailSmart Global: plataforma de datos

Back-end de datos para RetailSmart: integra tres fuentes públicas y tres fuentes simuladas en un Lakehouse, procesa en batch y en streaming, y expone los resultados mediante una API y un panel web. 

## Arquitectura

| Capa | Implementación (Google Cloud) | Equivalente abierto |
| --- | --- | --- |
| Ingesta streaming | Productor Python y Pub/Sub | Apache Kafka |
| Ingesta batch | Carga de archivos a Cloud Storage | Airflow y MinIO |
| Cómputo batch | Apache Spark en Dataproc | Apache Spark |
| Cómputo streaming | Apache Beam en Dataflow: agregados de clics y alertas de frío | Apache Flink |
| Almacenamiento | Cloud Storage con tablas Apache Iceberg (Landing, Universal, Smart) y BigQuery | MinIO con Iceberg, Trino o ClickHouse |
| Exposición | FastAPI en Cloud Run y panel web | FastAPI en Kubernetes |

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
api/          API FastAPI, panel web, Dockerfile y pruebas
contratos/    contrato de datos de fact_ventas y reglas de calidad
ingesta/      productor de clics y simulador de ERP, sensores y GPS
spark/        notebooks de Landing -> Universal -> Smart (00 a 05)
sql/          metadatos de Smart, consultas de verificación y pronóstico de demanda
streaming/    pipelines Apache Beam: agregados por ventana y alertas de frío
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
| API | Endpoints probados contra BigQuery; las cifras coinciden con `fact_ventas` y con los agregados de streaming |

## Ejecutar la API

```bash
cd api
pip install -r requirements.txt pytest httpx
python -m pytest -q test_api.py
```

Despliegue en Cloud Run (requiere el proyecto y las tablas de BigQuery):

```bash
gcloud run deploy api-retailsmart --source . --region us-central1 \
  --service-account=$SA_API --no-allow-unauthenticated --max-instances=2 --memory=512Mi
gcloud run services proxy api-retailsmart --region us-central1   # panel en http://localhost:8080/
```

## Datos simulados y alertas de frío

```bash
cd ingesta/simulacion
python3 cargar_erp.py --landing                                   # stock, plazos, demanda y stock operativo
python3 generador_iot.py --ticks 600 --verdad verdad_fallas.json  # sensores y GPS hacia Pub/Sub
```

Antes de publicar se crean los tópicos y las suscripciones (`sensores-beam` y `gps-beam`); los comandos están en `ingesta/simulacion/README.md`. El pipeline de alertas y su medición:

```bash
python3 streaming/alertas_frio.py --runner=DataflowRunner --project=retailsmart-bd --region=us-central1 \
  --temp_location=gs://retailsmart-lake-retailsmart-bd/tmp/dataflow \
  --staging_location=gs://retailsmart-lake-retailsmart-bd/tmp/staging \
  --job_name=alertas-frio --num_workers=1 --max_num_workers=1 --ventana_seg=30
python3 streaming/medir_deteccion.py --verdad ingesta/simulacion/verdad_fallas.json
```

## Pruebas

```bash
pip install -r streaming/requirements.txt
python -m pytest -q ingesta/simulacion streaming      # 18 pruebas sin conexión
cd api && python -m pytest -q test_api.py
```

## Supuestos y limitaciones

- Los montos se conservan en la moneda de origen (BRL, GBP y USD) y no se suman entre fuentes.
- Las ocho regiones logísticas no vienen en los datos; se definieron en el modelo (cinco macrorregiones de Brasil, Norteamérica, Europa y Asia-Pacífico y resto).
- UCI no identifica al cliente en cerca de una cuarta parte de sus líneas.
- El stock del ERP, los sensores de frío y el GPS son simulados con semilla fija. El stock se reescala a una cobertura de 5 a 25 días a partir de la demanda real del último año, porque el inventario original de thelook equivale a casi cuatro años de demanda.
- El GPS se publica en Pub/Sub; ningún pipeline lo procesa todavía.
- Las alertas de frío usan la hora de publicación en Pub/Sub como tiempo del evento, que en el generador coincide con `event_ts`.
- El rendimiento de streaming se probó con un worker a 100 mensajes por segundo; no se probó un ritmo mayor.
- El catálogo Iceberg es de tipo Hadoop; en producción debe sustituirse por un catálogo gestionado para soportar escrituras concurrentes.

## Equipo

Bryan Guerra Suarez    
Juan Carlos Vasquez    
Anyela Rosado     
Rodrigo Rodriguez
