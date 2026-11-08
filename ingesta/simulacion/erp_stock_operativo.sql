-- Calibración del stock simulado y serie de demanda diaria por centro de distribución.
-- Motivo: el inventario sintético de thelook equivale a unos 12 años de ventas (4.400 a 4.500 días
-- de cobertura), con lo que un plan de reposición nunca pediría nada. Aquí el stock se reescala a
-- una cobertura objetivo de 5 a 25 días por centro, y se asigna por categoría según la demanda real
-- del último año. La demanda es real (thelook); la cobertura y las variaciones son supuestos simulados.
-- Reproducible: las variaciones salen de un hash (FARM_FINGERPRINT), no de RAND().
-- Requiere haber corrido erp_stock.sql antes (usa erp_plazos). {PROJECT} lo reemplaza cargar_erp.py.

-- 1) Demanda diaria por centro, con ceros en los días sin ventas (serie regular para BigQuery ML).
CREATE OR REPLACE TABLE `{PROJECT}.retailsmart_smart.demanda_diaria_centro` AS
WITH ventas AS (
  SELECT ii.product_distribution_center_id AS centro_id,
         DATE(oi.created_at) AS dia,
         COUNT(*) AS unidades
  FROM `{PROJECT}.retailsmart_raw.order_items` oi
  JOIN `{PROJECT}.retailsmart_raw.inventory_items` ii ON oi.inventory_item_id = ii.id
  GROUP BY centro_id, dia
),
rango AS (
  SELECT centro_id, MIN(dia) AS desde, (SELECT MAX(dia) FROM ventas) AS hasta
  FROM ventas
  GROUP BY centro_id
)
SELECT r.centro_id, d AS dia, IFNULL(v.unidades, 0) AS unidades
FROM rango r
CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(r.desde, r.hasta)) AS d
LEFT JOIN ventas v ON v.centro_id = r.centro_id AND v.dia = d;

-- 2) Stock operativo por centro y categoría, con cobertura objetivo simulada.
CREATE OR REPLACE TABLE `{PROJECT}.retailsmart_smart.erp_stock_operativo` AS
WITH ancla AS (
  SELECT MAX(DATE(created_at)) AS hasta FROM `{PROJECT}.retailsmart_raw.order_items`
),
demanda AS (
  SELECT ii.product_distribution_center_id AS centro_id,
         IFNULL(ii.product_category, 'sin_categoria') AS categoria,
         COUNT(*) / 365 AS demanda_diaria
  FROM `{PROJECT}.retailsmart_raw.order_items` oi
  JOIN `{PROJECT}.retailsmart_raw.inventory_items` ii ON oi.inventory_item_id = ii.id
  CROSS JOIN ancla
  WHERE DATE(oi.created_at) > DATE_SUB(ancla.hasta, INTERVAL 365 DAY)
  GROUP BY centro_id, categoria
),
plazos AS (
  SELECT ii.product_distribution_center_id AS centro_id,
         IFNULL(ii.product_category, 'sin_categoria') AS categoria,
         CAST(ROUND(AVG(p.lead_time_dias)) AS INT64) AS lead_time_dias
  FROM (SELECT DISTINCT product_id, product_distribution_center_id, product_category
        FROM `{PROJECT}.retailsmart_raw.inventory_items`) ii
  JOIN `{PROJECT}.retailsmart_smart.erp_plazos` p ON p.product_id = ii.product_id
  GROUP BY centro_id, categoria
),
base AS (
  SELECT d.centro_id,
         d.categoria,
         d.demanda_diaria,
         -- cobertura objetivo del centro: entre 5 y 25 días
         5 + MOD(ABS(FARM_FINGERPRINT(CONCAT('cobertura|', CAST(d.centro_id AS STRING)))), 21) AS cobertura_objetivo_dias,
         -- variación por categoría: entre 0,30 y 1,70 veces la cobertura del centro
         0.3 + MOD(ABS(FARM_FINGERPRINT(CONCAT('var|', CAST(d.centro_id AS STRING), '|', d.categoria))), 141) / 100 AS factor_categoria,
         pl.lead_time_dias
  FROM demanda d
  LEFT JOIN plazos pl ON pl.centro_id = d.centro_id AND pl.categoria = d.categoria
),
con_existencias AS (
  SELECT *,
         GREATEST(0, CAST(ROUND(demanda_diaria * cobertura_objetivo_dias * factor_categoria) AS INT64)) AS existencias
  FROM base
)
SELECT centro_id,
       categoria,
       ROUND(demanda_diaria, 3) AS demanda_diaria,
       cobertura_objetivo_dias,
       existencias,
       -- reservadas: entre 0 % y 15 % de las existencias (simulado)
       CAST(ROUND(existencias * MOD(ABS(FARM_FINGERPRINT(CONCAT('res|', CAST(centro_id AS STRING), '|', categoria))), 16) / 100) AS INT64) AS reservadas,
       lead_time_dias,
       TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR) AS as_of,
       'simulado_calibrado' AS origen
FROM con_existencias;
