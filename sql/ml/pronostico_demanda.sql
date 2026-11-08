-- Capa de ML: pronóstico de demanda con BigQuery ML (ARIMA_PLUS) y plan de reposición.
-- ANTES de empezar, refresca el stock para que el plan no lo marque como obsoleto (más de 24 h):
--   python3 ~/retailsmart-bigdata-main/ingesta/simulacion/cargar_erp.py --solo-operativo
-- Ejecuta cada paso por separado en BigQuery. Los pasos 1 y 3 crean modelos y tardan unos minutos.

-- ============ PASO 1: modelo de prueba (entrenado sin los últimos 28 días) ============
CREATE OR REPLACE MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_prueba`
OPTIONS (model_type = 'ARIMA_PLUS', time_series_timestamp_col = 'dia',
         time_series_data_col = 'unidades', time_series_id_col = 'centro_id',
         data_frequency = 'DAILY', horizon = 28) AS
SELECT dia, centro_id, CAST(unidades AS FLOAT64) AS unidades
FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro`
WHERE dia <= DATE_SUB((SELECT MAX(dia) FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro`), INTERVAL 28 DAY);

-- ============ PASO 2: evaluación contra los últimos 28 días reales (retrospectiva) ============
-- Compara el modelo con una línea base ingenua: el promedio de los 28 días anteriores al corte.
-- WAPE = suma de errores absolutos / suma de ventas reales (menor es mejor). Captura el resultado.
WITH ult AS (SELECT MAX(dia) AS fecha FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro`),
previsto AS (
  SELECT centro_id, DATE(forecast_timestamp) AS dia, forecast_value AS previsto
  FROM ML.FORECAST(MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_prueba`,
                   STRUCT(28 AS horizon, 0.9 AS confidence_level))
),
real AS (
  SELECT d.centro_id, d.dia, d.unidades AS real
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro` d, ult
  WHERE d.dia > DATE_SUB(ult.fecha, INTERVAL 28 DAY)
),
base AS (
  SELECT d.centro_id, AVG(d.unidades) AS promedio
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro` d, ult
  WHERE d.dia <= DATE_SUB(ult.fecha, INTERVAL 28 DAY) AND d.dia > DATE_SUB(ult.fecha, INTERVAL 56 DAY)
  GROUP BY d.centro_id
)
SELECT r.centro_id,
       ROUND(SUM(ABS(r.real - p.previsto)) / SUM(r.real), 3) AS wape_modelo,
       ROUND(SUM(ABS(r.real - b.promedio)) / SUM(r.real), 3) AS wape_linea_base
FROM real r
JOIN previsto p ON p.centro_id = r.centro_id AND p.dia = r.dia
JOIN base b ON b.centro_id = r.centro_id
GROUP BY r.centro_id
ORDER BY r.centro_id;

-- ============ PASO 3: modelo final (toda la serie, pronóstico a 14 días) ============
CREATE OR REPLACE MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_centro`
OPTIONS (model_type = 'ARIMA_PLUS', time_series_timestamp_col = 'dia',
         time_series_data_col = 'unidades', time_series_id_col = 'centro_id',
         data_frequency = 'DAILY', horizon = 14) AS
SELECT dia, centro_id, CAST(unidades AS FLOAT64) AS unidades
FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro`;

-- ============ PASO 4: pronóstico de 14 días por centro ============
CREATE OR REPLACE TABLE `retailsmart-bd.retailsmart_smart.pronostico_demanda` AS
SELECT centro_id,
       DATE(forecast_timestamp) AS dia,
       forecast_value AS unidades_previstas,
       prediction_interval_lower_bound AS inferior,
       prediction_interval_upper_bound AS superior
FROM ML.FORECAST(MODEL `retailsmart-bd.retailsmart_smart.modelo_demanda_centro`,
                 STRUCT(14 AS horizon, 0.9 AS confidence_level));

-- ============ PASO 5: plan de reposición por centro y categoría ============
-- reposición = máx(0, ⌈ demanda prevista durante el plazo + stock de seguridad − disponible ⌉)
-- stock de seguridad = 1,65 × σ × √plazo, repartido según la participación de la categoría.
CREATE OR REPLACE TABLE `retailsmart-bd.retailsmart_smart.plan_reposicion` AS
WITH corte AS (SELECT MAX(dia) AS fecha FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro`),
prev AS (
  SELECT p.centro_id, DATE_DIFF(p.dia, c.fecha, DAY) AS dia_n, p.unidades_previstas
  FROM `retailsmart-bd.retailsmart_smart.pronostico_demanda` p, corte c
),
sigma AS (
  SELECT d.centro_id, STDDEV_SAMP(d.unidades) AS sigma
  FROM `retailsmart-bd.retailsmart_smart.demanda_diaria_centro` d, corte c
  WHERE d.dia > DATE_SUB(c.fecha, INTERVAL 90 DAY)
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
