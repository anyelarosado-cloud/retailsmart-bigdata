-- Pronóstico de demanda con BigQuery ML (ARIMA_PLUS), versión 2 con datos depurados.
--
-- Hallazgos que motivan esta versión (consultas de diagnóstico del 16 de noviembre):
--   1) El 21 de octubre de 2026 está truncado: su último registro es 00:19 UTC (1 unidad). Se excluye.
--   2) Del 11 al 18 de octubre la demanda sube hasta 1.774 unidades/día (normal: ~350). No se repite en
--      2025 y las cancelaciones crecen en la misma proporción, así que no es estacional ni duplicado.
--      Se trata como un evento atípico de la fuente.
-- Decisiones:
--   - El modelo y el plan usan solo días completos: hasta el 20 de octubre (tabla demanda_diaria_modelo).
--   - La validación del modelo se hace en una ventana limpia anterior al evento (13 sep a 10 oct).
--     La validación original (últimos 28 días, con el evento) se conserva como evidencia.
--   - La desviación estándar del stock de seguridad excluye el evento (11 a 18 oct).
-- Ejecuta cada paso por separado. Los pasos 1 y 3 crean modelos y tardan unos minutos.
-- ANTES del paso 5, refresca el stock (el plan marca como obsoleto lo que supera 24 h):
--   python3 ingesta/simulacion/cargar_erp.py --solo-operativo

-- ============ PASO 0: demanda diaria con días completos ============
CREATE OR REPLACE TABLE `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo` AS
SELECT * FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro`
WHERE dia <= DATE '2026-10-20';

-- Comprobación: debe haber 10 centros y el mismo número de días en todos, con fin el 2026-10-20.
SELECT centro_id, COUNT(*) AS dias, MIN(dia) AS desde, MAX(dia) AS hasta
FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`
GROUP BY centro_id ORDER BY centro_id;

-- ============ PASO 1: modelo de prueba ============
CREATE OR REPLACE MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_prueba_limpia`
OPTIONS (model_type = 'ARIMA_PLUS', time_series_timestamp_col = 'dia',
         time_series_data_col = 'unidades', time_series_id_col = 'centro_id',
         data_frequency = 'DAILY', horizon = 28) AS
SELECT dia, centro_id, CAST(unidades AS FLOAT64) AS unidades
FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`
WHERE dia <= DATE '2026-09-12';

-- ============ PASO 2: WAPE en la ventana limpia (13 sep a 10 oct) ============
-- Línea base: promedio de los 28 días anteriores a la ventana (16 ago a 12 sep).
-- La fila con centro_id nulo es el total. Menor WAPE es mejor. Captura el resultado.
WITH previsto AS (
  SELECT centro_id, DATE(forecast_timestamp) AS dia, forecast_value AS previsto
  FROM ML.FORECAST(MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_prueba_limpia`,
                   STRUCT(28 AS horizon, 0.9 AS confidence_level))
),
real AS (
  SELECT centro_id, dia, unidades AS real
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`
  WHERE dia BETWEEN DATE '2026-09-13' AND DATE '2026-10-10'
),
base AS (
  SELECT centro_id, AVG(unidades) AS promedio
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`
  WHERE dia BETWEEN DATE '2026-08-16' AND DATE '2026-09-12'
  GROUP BY centro_id
)
SELECT r.centro_id,
       ROUND(SUM(ABS(r.real - p.previsto)) / SUM(r.real), 3) AS wape_modelo,
       ROUND(SUM(ABS(r.real - b.promedio)) / SUM(r.real), 3) AS wape_linea_base
FROM real r
JOIN previsto p ON p.centro_id = r.centro_id AND p.dia = r.dia
JOIN base b ON b.centro_id = r.centro_id
GROUP BY ROLLUP(r.centro_id)
ORDER BY r.centro_id IS NULL, r.centro_id;

-- ============ PASO 2b: el mismo WAPE sobre totales semanales (4 semanas) ============
WITH previsto AS (
  SELECT centro_id, DATE(forecast_timestamp) AS dia, forecast_value AS previsto
  FROM ML.FORECAST(MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_prueba_limpia`,
                   STRUCT(28 AS horizon, 0.9 AS confidence_level))
),
real AS (
  SELECT centro_id, dia, unidades AS real
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`
  WHERE dia BETWEEN DATE '2026-09-13' AND DATE '2026-10-10'
),
base AS (
  SELECT centro_id, AVG(unidades) AS promedio
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`
  WHERE dia BETWEEN DATE '2026-08-16' AND DATE '2026-09-12'
  GROUP BY centro_id
),
semanal AS (
  SELECT r.centro_id,
         DIV(DATE_DIFF(r.dia, DATE '2026-09-13', DAY), 7) AS semana,
         SUM(r.real) AS real, SUM(p.previsto) AS previsto, SUM(b.promedio) AS base
  FROM real r
  JOIN previsto p ON p.centro_id = r.centro_id AND p.dia = r.dia
  JOIN base b ON b.centro_id = r.centro_id
  GROUP BY r.centro_id, semana
)
SELECT centro_id,
       ROUND(SUM(ABS(real - previsto)) / SUM(real), 3) AS wape_modelo_semanal,
       ROUND(SUM(ABS(real - base)) / SUM(real), 3) AS wape_base_semanal
FROM semanal
GROUP BY ROLLUP(centro_id)
ORDER BY centro_id IS NULL, centro_id;

-- ============ PASO 3: modelo final (serie completa hasta el 20 oct, pronóstico a 14 días) ============
CREATE OR REPLACE MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_centro`
OPTIONS (model_type = 'ARIMA_PLUS', time_series_timestamp_col = 'dia',
         time_series_data_col = 'unidades', time_series_id_col = 'centro_id',
         data_frequency = 'DAILY', horizon = 14) AS
SELECT dia, centro_id, CAST(unidades AS FLOAT64) AS unidades
FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`;

-- ============ PASO 4: pronóstico de 14 días por centro ============
CREATE OR REPLACE TABLE `retailsmart-bd.retailsmart_smart.pronostico_demanda` AS
SELECT centro_id,
       DATE(forecast_timestamp) AS dia,
       forecast_value AS unidades_previstas,
       prediction_interval_lower_bound AS inferior,
       prediction_interval_upper_bound AS superior
FROM ML.FORECAST(MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_centro`,
                 STRUCT(14 AS horizon, 0.9 AS confidence_level));

-- Comprobación de coherencia: el pronóstico diario debe quedar cerca del nivel reciente SIN el evento.
-- Si la previsión sale muy por encima (efecto del pico), avísame antes de seguir.
SELECT p.centro_id,
       ROUND(AVG(p.unidades_previstas), 1) AS prevision_diaria,
       ROUND(ANY_VALUE(h.reciente), 1) AS promedio_19_20_oct,
       ROUND(ANY_VALUE(h.previo), 1) AS promedio_26sep_10oct
FROM `retailsmart-bd.retailsmart_smart.pronostico_demanda` p
JOIN (
  SELECT centro_id,
         AVG(IF(dia BETWEEN DATE '2026-10-19' AND DATE '2026-10-20', unidades, NULL)) AS reciente,
         AVG(IF(dia BETWEEN DATE '2026-09-26' AND DATE '2026-10-10', unidades, NULL)) AS previo
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`
  GROUP BY centro_id
) h ON h.centro_id = p.centro_id
GROUP BY p.centro_id ORDER BY p.centro_id;

-- ============ PASO 5: plan de reposición por centro y categoría ============
-- reposición = máx(0, ⌈ demanda prevista durante el plazo + stock de seguridad − disponible ⌉)
-- stock de seguridad = 1,65 × σ × √plazo, repartido según la participación de la categoría.
-- σ = desviación estándar diaria de los últimos 90 días, sin el evento atípico del 11 al 18 de octubre.
CREATE OR REPLACE TABLE `retailsmart-bd.retailsmart_smart.plan_reposicion` AS
WITH corte AS (SELECT MAX(dia) AS fecha FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo`),
prev AS (
  SELECT p.centro_id, DATE_DIFF(p.dia, c.fecha, DAY) AS dia_n, p.unidades_previstas
  FROM `retailsmart-bd.retailsmart_smart.pronostico_demanda` p, corte c
),
sigma AS (
  SELECT d.centro_id, STDDEV_SAMP(d.unidades) AS sigma
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_modelo` d, corte c
  WHERE d.dia > DATE_SUB(c.fecha, INTERVAL 90 DAY)
    AND d.dia NOT BETWEEN DATE '2026-10-11' AND DATE '2026-10-18'
  GROUP BY d.centro_id
),
total_centro AS (
  SELECT centro_id, SUM(demanda_diaria) AS total
  FROM `retailsmart-bd.retailsmart_smart.erp_stock_operativo`
  GROUP BY centro_id
),
base AS (
  SELECT s.centro_id, s.categoria,
         s.existencias - s.reservadas AS disponible,
         COALESCE(s.lead_time_dias, 7) AS plazo,
         SAFE_DIVIDE(s.demanda_diaria, t.total) AS participacion,
         s.as_of
  FROM `retailsmart-bd.retailsmart_smart.erp_stock_operativo` s
  JOIN total_centro t ON t.centro_id = s.centro_id
),
agregado AS (
  SELECT b.centro_id, b.categoria, b.disponible, b.plazo, b.participacion, b.as_of, sg.sigma,
         SUM(p.unidades_previstas) AS prevista_centro
  FROM base b
  JOIN prev p ON p.centro_id = b.centro_id AND p.dia_n <= b.plazo
  JOIN sigma sg ON sg.centro_id = b.centro_id
  GROUP BY b.centro_id, b.categoria, b.disponible, b.plazo, b.participacion, b.as_of, sg.sigma
),
calculo AS (
  SELECT centro_id, categoria, disponible, plazo,
         ROUND(participacion * prevista_centro, 1) AS demanda_en_plazo,
         ROUND(1.65 * sigma * SQRT(plazo) * participacion, 1) AS stock_seguridad,
         IF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), as_of, HOUR) > 24, 'sin_dato', 'ok') AS estado
  FROM agregado
)
SELECT centro_id, categoria, disponible, plazo AS plazo_dias, demanda_en_plazo, stock_seguridad, estado,
       IF(estado = 'ok',
          GREATEST(0, CAST(CEIL(demanda_en_plazo + stock_seguridad - disponible) AS INT64)),
          NULL) AS reposicion_sugerida
FROM calculo;

-- ============ PASO 6: lectura de resultados (capturas) ============
-- 6a) Plan por centro y categoría, de mayor a menor reposición
SELECT * FROM `retailsmart-bd.retailsmart_smart.plan_reposicion`
ORDER BY reposicion_sugerida DESC, centro_id, categoria LIMIT 30;

-- 6b) Resumen por centro
SELECT centro_id, COUNTIF(reposicion_sugerida > 0) AS categorias_a_reponer,
       SUM(reposicion_sugerida) AS unidades_sugeridas, COUNTIF(estado = 'sin_dato') AS sin_dato
FROM `retailsmart-bd.retailsmart_smart.plan_reposicion`
GROUP BY centro_id ORDER BY centro_id;
