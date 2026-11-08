-- Stock y plazos del ERP simulados a partir de thelook_ecommerce.inventory_items.
-- Reproducible: los campos simulados salen de un hash (FARM_FINGERPRINT), no de RAND(),
-- así que dos ejecuciones sobre los mismos datos dan las mismas cifras.
-- {PROJECT} se reemplaza al ejecutar con cargar_erp.py.

-- 1) Foto de existencias por centro de distribución y producto (ítems aún no vendidos).
CREATE OR REPLACE TABLE `{PROJECT}.retailsmart_smart.erp_stock` AS
WITH base AS (
  SELECT product_distribution_center_id AS centro_id,
         product_id,
         ANY_VALUE(product_sku) AS sku,
         COUNT(*) AS existencias
  FROM `{PROJECT}.retailsmart_raw.inventory_items`
  WHERE sold_at IS NULL
  GROUP BY centro_id, product_id
)
SELECT centro_id,
       product_id,
       sku,
       existencias,
       -- reservadas: entre 0 % y 15 % de las existencias (simulado)
       CAST(ROUND(existencias * MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(centro_id AS STRING), '|', CAST(product_id AS STRING), '|r'))), 16) / 100) AS INT64) AS reservadas,
       TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR) AS as_of,
       'simulado' AS origen
FROM base;

-- 2) Plazo de reposición por producto: entre 2 y 14 días (simulado); proveedor = marca.
CREATE OR REPLACE TABLE `{PROJECT}.retailsmart_smart.erp_plazos` AS
SELECT product_id,
       ANY_VALUE(product_brand) AS proveedor,
       2 + MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(product_id AS STRING), '|lt'))), 13) AS lead_time_dias,
       'simulado' AS origen
FROM `{PROJECT}.retailsmart_raw.inventory_items`
GROUP BY product_id;
