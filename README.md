# RetailSmart Global: plataforma de datos (Lumina Retail)

Back-end de datos para RetailSmart: integra tres fuentes heterogéneas en un Lakehouse, procesa en batch y en streaming, y expone los resultados mediante una API y un panel web. El diseño completo está en `docs/RetailSmart_Arquitectura_Big_Data.docx`; el registro de lo ejecutado, con comandos y resultados, en `docs/bitacora_implementacion.md`.

## Arquitectura

| Capa | Implementación (Google Cloud) | Equivalente abierto |
| --- | --- | --- |
| Ingesta streaming | Productor Python y Pub/Sub | Apache Kafka |
| Ingesta batch | Carga de archivos a Cloud Storage | Airflow y MinIO |
| Cómputo batch | Apache Spark en Dataproc | Apache Spark |
| Cómputo streaming | Apache Beam en Dataflow | Apache Flink |
| Almacenamiento | Cloud Storage con tablas Apache Iceberg (Landing, Universal, Smart) y BigQuery | MinIO con Iceberg, Trino o ClickHouse |
| Exposición | FastAPI en Cloud Run y panel web | FastAPI en Kubernetes |

Zonas del lago: **Landing** (crudo e inmutable), **Universal** (limpio, tipado y sin duplicados, con tabla de cuarentena por entidad) y **Smart** (esquema en estrella: `fact_ventas` y cinco dimensiones conformadas).

## Fuentes

- `thelook_ecommerce` (BigQuery público): canal web, clics y centros de distribución.
- Olist (Kaggle): logística y última milla de Brasil.
- UCI Online Retail: venta mayorista en línea, usada como sustituto de la tienda física.

Los clics se reproducen hacia Pub/Sub con marcas de tiempo actuales (`ingesta/productor_eventos.py`).

## Estructura

```
api/          API FastAPI, panel web, Dockerfile y pruebas
contratos/    contrato de datos de fact_ventas
docs/         documento de arquitectura y bitácora de implementación
ingesta/      productor de eventos hacia Pub/Sub
spark/        notebooks de Landing -> Universal -> Smart (00 a 05)
streaming/    pipeline Apache Beam de agregados por ventana
```

## Resultados verificados

| Componente | Resultado |
| --- | --- |
| Landing | thelook en Parquet (7 tablas), UCI (541.909 líneas) y Olist (9 archivos) |
| Smart | `fact_ventas` con 828.423 líneas (181.644 thelook, 112.650 Olist, 534.129 UCI); `dim_fecha` 6.939, `dim_canal` 3, `dim_producto` 66.009, `dim_cliente` 200.467, `dim_geo` 68 |
| Streaming | Prueba en Dataflow con un worker: 3.020 de 3.020 mensajes agregados a 100 por segundo |
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

## Supuestos y limitaciones

- Los montos se conservan en la moneda de origen (BRL, GBP y USD) y no se suman entre fuentes.
- Las ocho regiones logísticas no vienen en los datos; se definieron en el modelo (cinco macrorregiones de Brasil, Norteamérica, Europa y Asia-Pacífico y resto).
- UCI no identifica al cliente en cerca de una cuarta parte de sus líneas.
- El rendimiento de streaming se probó con un worker a 100 mensajes por segundo; no se probó un ritmo mayor.
- El catálogo Iceberg es de tipo Hadoop; en producción debe sustituirse por un catálogo gestionado para soportar escrituras concurrentes.

## Equipo

Bryan Guerra Suarez    
Juan Carlo Vasquez    
Anyela Rosado     
Rodrigo Rodriguez
