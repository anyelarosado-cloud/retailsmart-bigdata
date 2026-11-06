# Lista de capturas del informe técnico (Entrega 2)

Guarda cada captura con el nombre indicado (por ejemplo `F05_presupuesto.png`). El número es el de la figura en el Word; al insertar, solo hay que seguir el orden.

Las 5 figuras restantes del informe (arquitectura, red, estrella y dos de costos) ya están incluidas.


## Grupo 1. Ya existe en el proyecto: solo abrir la consola y capturar

| Hecha | Figura | Archivo | Qué capturar |
| --- | --- | --- | --- |
| ☐ | 2 | `F02_presupuesto.png` | **Presupuesto del proyecto con sus umbrales.** Facturación → Presupuestos y alertas → Presupuesto de retailsmart-bd con los umbrales de 50, 90 y 100 % |
| ☐ | 3 | `F03_apis.png` | **Servicios de Google Cloud habilitados.** APIs y servicios → Servicios habilitados → Storage, Compute Engine, Dataproc, Dataflow, Pub/Sub, BigQuery, Cloud Run, Artifact Registry y Cloud Build |
| ☐ | 4 | `F04_bucket.png` | **Configuración del bucket del lago.** Cloud Storage → retailsmart-lake-retailsmart-bd → Configuración → Ubicación us-central1, control de acceso uniforme y prevención de acceso público |
| ☐ | 5 | `F05_pubsub.png` | **Tópicos y suscripciones de Pub/Sub.** Pub/Sub → Tópicos y Suscripciones → clicks, sensores y gps con sus suscripciones |
| ☐ | 6 | `F06_iam_cap.png` | **Roles de la cuenta de servicio de Compute Engine.** IAM y administración → IAM → Fila de la cuenta de Compute Engine con sus roles |
| ☐ | 7 | `F07_iam_ds.png` | **Permisos del conjunto de datos retailsmart_smart.** BigQuery → retailsmart_smart → Compartir → Permisos → Principales con acceso al conjunto de datos |
| ☐ | 9 | `F09_red_cap.png` | **Subred con acceso privado a Google.** VPC → Redes de VPC → default → subred us-central1 → Acceso privado a Google: Activado |
| ☐ | 10 | `F10_fw_cap.png` | **Reglas de firewall de la red.** VPC → Firewall → Reglas de la red default |
| ☐ | 12 | `F12_landing_thelook.png` | **Carpetas de thelook en Landing.** Cloud Storage → bucket → landing → thelook → Las siete carpetas, o la salida de gcloud storage du --summarize --readable-sizes |
| ☐ | 13 | `F13_landing_uci_olist.png` | **Archivos de UCI y Olist en Landing.** Cloud Storage → bucket → landing → uci y olist → Original y CSV de UCI; los nueve archivos de Olist |
| ☐ | 23 | `F23_nb_out.png` | **Notebook de Universal ejecutado.** JupyterLab → notebook 01_olist_pedidos (o 03_uci_ventas) → Celdas ejecutadas con el conteo de registros leídos, válidos y rechazados |
| ☐ | 24 | `F24_universal_gcs.png` | **Tablas Iceberg de la zona Universal.** Cloud Storage → bucket → lakehouse → universal → Carpetas silver_* y cuarentena_* |
| ☐ | 25 | `F25_cuarentena.png` | **Registros en cuarentena de UCI.** JupyterLab → consulta a cuarentena_tienda_ventas_v1 → Registros con su columna motivo |
| ☐ | 27 | `F27_bq_tablas.png` | **Tablas de la zona Smart en BigQuery.** BigQuery → retailsmart_smart → Seis tablas con su número de filas (6.939, 3, 66.009, 200.467, 68 y 828.423) |
| ☐ | 31 | `F31_df_grafo.png` | **Grafo del job agregados-eventos.** Dataflow → Trabajos → agregados-eventos → Grafo del trabajo → Pasos del pipeline, ID del job y estado |
| ☐ | 34 | `F34_rt_tablas.png` | **Tablas de tiempo real.** BigQuery → retailsmart_smart → rt_eventos_ventana y rt_productos_ventana → Esquema y vista previa |
| ☐ | 39 | `F39_run_cap.png` | **Servicio de Cloud Run.** Cloud Run → api-retailsmart → Revisiones y Seguridad → Autenticación requerida, cuenta de servicio y máximo de dos instancias |
| ☐ | 51 | `F51_fact_cap.png` | **Informe de facturación del proyecto.** Facturación → Informes → Filtro por proyecto retailsmart-bd, agrupado por servicio, sin créditos |

## Grupo 2. Se obtiene con un comando o una consulta rápida (segundos, centavos)

| Hecha | Figura | Archivo | Qué capturar |
| --- | --- | --- | --- |
| ☐ | 14 | `F14_erp_tablas.png` | **Tablas del ERP simulado en BigQuery.** BigQuery → retailsmart_smart → erp_stock, erp_plazos, erp_stock_operativo y demanda_diaria_centro → Esquema y vista previa de cada una |
| ☐ | 15 | `F15_erp_verif.png` | **Resultados de la verificación del stock.** BigQuery → consulta con verificar_erp.sql → Resultados de las consultas de la tabla de verificación |
| ☐ | 16 | `F16_erp_cobertura.png` | **Cobertura de stock por centro antes y después de la calibración.** BigQuery → resultado de la consulta de cobertura → Antes: unos 4.400 a 4.500 días; después: entre 3 y 35 días |
| ☐ | 17 | `F17_pytest_cap.png` | **Pruebas automáticas del generador.** Cloud Shell → salida de python3 -m pytest -q test_generador_iot.py → 8 passed |
| ☐ | 18 | `F18_iot_cap.png` | **Corrida del generador y mensajes de muestra.** Cloud Shell → salida del generador con sus estadísticas → Un mensaje de sensor y uno de GPS obtenidos con gcloud pubsub subscriptions pull |
| ☐ | 19 | `F19_verdad_cap.png` | **Fallas inyectadas registradas.** Contenido de verdad_fallas.json → Cámara, tipo de falla, inicio y fin |
| ☐ | 20 | `F20_productor_cap.png` | **Publicación de eventos de clics.** Cloud Shell → salida del productor y de gcloud pubsub subscriptions pull → Mensajes publicados y tres mensajes de muestra |
| ☐ | 28 | `F28_bq_region.png` | **Ventas por región y moneda.** BigQuery → resultado de la consulta por región y moneda → 17 combinaciones |
| ☐ | 29 | `F29_bq_huerfanas.png` | **Verificación de integridad referencial.** BigQuery → consulta de filas huérfanas → Resultados en 0 para producto, geografía, fecha y cliente |
| ☐ | 30 | `F30_beam_local.png` | **Prueba local del pipeline.** BigQuery o Cloud Shell → resultado de la consulta de totales → Suma de 310 eventos en ventanas de 30 segundos |
| ☐ | 33 | `F33_df_consulta.png` | **Totales del streaming en BigQuery.** BigQuery → resultado de la consulta de totales → 3.020 eventos y 769 visitas a producto |
| ☐ | 40 | `F40_curl_cap.png` | **Respuestas de la API.** Cloud Shell → salida de los comandos curl → Salud, resumen de ventas y eventos en tiempo real |
| ☐ | 41 | `F41_panel_cap.png` | **Panel web.** Vista previa en la web → panel con datos → Eventos por tipo, productos más vistos y ventas por región |
| ☐ | 42 | `F42_contrato_cap.png` | **Resultado de la validación del contrato.** Cloud Shell → salida de validar_contrato.py → Comprobaciones con su estado |

## Grupo 3. Hay que encender algo (cluster o Dataflow); capturar y apagar

| Hecha | Figura | Archivo | Qué capturar |
| --- | --- | --- | --- |
| ☐ | 11 | `F11_noip.png` | **Cluster de Spark sin IP externa.** Dataproc → Clústeres → spark-retail → Configuración → Red, subred y ausencia de IP externa |
| ☐ | 21 | `F21_cluster.png` | **Creación del cluster de Spark.** Dataproc → Clústeres → spark-retail → Estado En ejecución, tipo de máquina y componentes opcionales |
| ☐ | 22 | `F22_jupyter.png` | **JupyterLab sobre el cluster.** Dataproc → spark-retail → Interfaces web → JupyterLab → Lista de notebooks de la carpeta spark/ |
| ☐ | 32 | `F32_df_metricas.png` | **Métricas del job de Dataflow.** Dataflow → agregados-eventos → Métricas del trabajo → Throughput, workers y utilización con fechas y unidades visibles |
| ☐ | 35 | `F35_dedup_cap.png` | **Resultado de la deduplicación.** BigQuery → consulta de totales con y sin --deduplicar → Comparación con los registros de logs/productor_*.json |

## Grupo 4. Trabajo nuevo, todavía no ejecutado

| Hecha | Figura | Archivo | Qué capturar |
| --- | --- | --- | --- |
| ☐ | 36 | `F36_alertas_cap.png` | **Alertas de frío en BigQuery.** BigQuery → retailsmart_smart → rt_alertas_frio → Alertas generadas durante la corrida del generador |
| ☐ | 37 | `F37_modelo_cap.png` | **Modelo de pronóstico en BigQuery.** BigQuery → retailsmart_smart → modelo_demanda_centro → Pestañas Detalles y Evaluación del modelo |
| ☐ | 38 | `F38_plan_cap.png` | **Plan de reposición sugerido.** BigQuery → resultado de la consulta del plan de reposición → Centro, categoría, disponible, demanda prevista y reposición |
| ☐ | 43 | `F43_kc_catalogo.png` | **Tablas de Smart en Knowledge Catalog.** BigQuery → Gobernanza → Knowledge Catalog (o Dataplex) → Tablas de retailsmart_smart con su descripción y aspectos |
| ☐ | 44 | `F44_kc_desc.png` | **Descripciones de columnas de fact_ventas.** BigQuery → fact_ventas → Esquema → Columnas con su descripción |
| ☐ | 45 | `F45_kc_perfil.png` | **Perfil de datos de fact_ventas.** Knowledge Catalog → Perfilado de datos → resultado del escaneo → Nulos, valores distintos y rangos por columna |
| ☐ | 46 | `F46_kc_calidad.png` | **Escaneo de calidad de fact_ventas.** Knowledge Catalog → Calidad de datos → resultado del escaneo → Reglas con su resultado y puntaje |
| ☐ | 47 | `F47_sdp_cap.png` | **Trabajos de inspección de Sensitive Data Protection.** Seguridad → Sensitive Data Protection → Inspección → Resumen de hallazgos de la tabla cruda y de dim_cliente |
| ☐ | 50 | `F50_calc_gcp.png` | **Estimación en la calculadora de precios de Google Cloud.** cloud.google.com/products/calculator → Estimación con Pub/Sub, Dataflow, Dataproc, Cloud Storage, BigQuery, Cloud Run, Memorystore y Datastream |
| ☐ | 52 | `F52_sim_cap.png` | **Antigüedad del mensaje más antiguo durante el simulacro.** Pub/Sub → clicks-beam → Métricas → Aumento de la antigüedad durante la caída y descenso a cero tras el relanzamiento |
| ☐ | 53 | `F53_sim_job.png` | **Job de Dataflow relanzado.** Dataflow → Trabajos → Job original cancelado y job nuevo en ejecución |
