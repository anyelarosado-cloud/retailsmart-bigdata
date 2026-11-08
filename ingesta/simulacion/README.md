# Datos simulados: ERP, sensores de frío y GPS

Estas fuentes no existen en los datasets públicos del taller. Se generan de forma
reproducible y quedan marcadas como simuladas (`origen = 'simulado'`, `fuente = 'simulado'`).

| Fuente | Cómo se genera | Dónde queda |
| --- | --- | --- |
| Stock del ERP | `erp_stock.sql` sobre `thelook_ecommerce.inventory_items` (existencias reales de thelook; reservadas y plazos simulados con hash) | `retailsmart_smart.erp_stock` y `erp_plazos` |
| Sensores de frío | `generador_iot.py`, con fallas inyectadas (deriva de temperatura, sensor mudo, lectura inválida) | tópico Pub/Sub `sensores` |
| GPS de repartidores | `generador_iot.py`, vehículos que parten de cada centro de distribución | tópico Pub/Sub `gps` |

Los centros de distribución son los mismos en las tres fuentes (`centro_id` de thelook), por lo
que stock, sensores y GPS se pueden cruzar.

## Preparar Pub/Sub

Una suscripción solo recibe lo publicado después de crearse; créelas antes de generar eventos.

```bash
gcloud pubsub topics create sensores
gcloud pubsub topics create gps
gcloud pubsub subscriptions create sensores-beam --topic=sensores
gcloud pubsub subscriptions create gps-beam --topic=gps
gcloud pubsub subscriptions add-iam-policy-binding sensores-beam --member="serviceAccount:$SA" --role=roles/pubsub.subscriber
gcloud pubsub subscriptions add-iam-policy-binding gps-beam --member="serviceAccount:$SA" --role=roles/pubsub.subscriber
```

## Ejecutar

```bash
python cargar_erp.py --landing                     # stock y plazos del ERP
python generador_iot.py --ticks 600 --verdad verdad_fallas.json
python generador_iot.py --ticks 600 --p-duplicado 0.01 --p-invalido 0.005   # para probar deduplicación y cola de errores
python -m pytest -q test_generador_iot.py          # pruebas sin conexión
```

Cada ronda (`tick`) emite una lectura por cámara y una posición por vehículo. Con los valores
por defecto (40 cámaras y 60 vehículos) son unos 100 mensajes por segundo.

## Verdad de terreno

Los mensajes no dicen si una lectura corresponde a una falla. Las fallas inyectadas, con su
inicio y fin, se guardan en `verdad_fallas.json`. Sirve para medir cuántas detecta el pipeline
de alertas (`streaming/medir_deteccion.py`).

## Límites

- Los datos son sintéticos y se generan con semilla fija (`--seed 42`).
- El movimiento del GPS usa 30 segundos simulados por ronda para que se vea desplazamiento.
- `as_of` del stock es la hora de la ejecución; el stock no cambia en el tiempo.
