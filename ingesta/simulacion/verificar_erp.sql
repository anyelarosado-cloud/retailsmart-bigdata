-- Verificación del stock y plazos simulados. Reemplaza {PROJECT} (por ejemplo retailsmart-bd).
-- Cada consulta indica el resultado esperado. Capture los resultados para el documento técnico.

-- 1) Invariante: la suma de existencias debe ser igual a los ítems no vendidos del origen.
SELECT
  (SELECT SUM(existencias) FROM `{PROJECT}.retailsmart_smart.erp_stock`) AS suma_existencias,
  (SELECT COUNT(*) FROM `{PROJECT}.retailsmart_raw.inventory_items` WHERE sold_at IS NULL) AS items_no_vendidos;
-- esperado: ambas cifras iguales

-- 2) Reservadas dentro de rango (nunca negativas ni mayores que las existencias).
SELECT COUNTIF(reservadas < 0 OR reservadas > existencias) AS filas_fuera_de_rango,
       ROUND(SAFE_DIVIDE(SUM(reservadas), SUM(existencias)), 3) AS proporcion_reservada
FROM `{PROJECT}.retailsmart_smart.erp_stock`;
-- esperado: filas_fuera_de_rango = 0; proporcion_reservada cercana a 0,07 o 0,08

-- 3) Todos los centros existen en thelook (clave de unión con sensores y GPS).
SELECT COUNT(*) AS filas_con_centro_inexistente
FROM `{PROJECT}.retailsmart_smart.erp_stock` s
LEFT JOIN `{PROJECT}.retailsmart_raw.distribution_centers` d ON s.centro_id = d.id
WHERE d.id IS NULL;
-- esperado: 0

-- 4) Plazos: un registro por producto, entre 2 y 14 días.
SELECT COUNT(*) AS productos, COUNT(DISTINCT product_id) AS productos_distintos,
       MIN(lead_time_dias) AS plazo_min, MAX(lead_time_dias) AS plazo_max
FROM `{PROJECT}.retailsmart_smart.erp_plazos`;
-- esperado: productos = productos_distintos; plazo_min >= 2; plazo_max <= 14

-- 5) Todo producto con stock tiene plazo de reposición.
SELECT COUNT(*) AS stock_sin_plazo
FROM `{PROJECT}.retailsmart_smart.erp_stock` s
LEFT JOIN `{PROJECT}.retailsmart_smart.erp_plazos` p USING (product_id)
WHERE p.product_id IS NULL;
-- esperado: 0

-- 6) Resumen por centro de distribución.
SELECT centro_id, COUNT(*) AS productos, SUM(existencias) AS existencias, SUM(reservadas) AS reservadas
FROM `{PROJECT}.retailsmart_smart.erp_stock`
GROUP BY centro_id ORDER BY centro_id;

-- 7) Calibración: la cobertura por centro debe quedar entre 3 y 35 días (antes eran más de 4.000).
SELECT centro_id,
       ROUND(SUM(demanda_diaria), 1) AS unidades_por_dia,
       SUM(existencias - reservadas) AS disponible,
       ROUND(SUM(existencias - reservadas) / SUM(demanda_diaria), 1) AS dias_de_cobertura
FROM `{PROJECT}.retailsmart_smart.erp_stock_operativo`
GROUP BY centro_id ORDER BY centro_id;
-- esperado: dias_de_cobertura entre 3 y 35 en todos los centros

-- 8) La serie diaria es regular: una fila por centro y día, sin huecos.
SELECT centro_id, COUNT(*) AS dias, DATE_DIFF(MAX(dia), MIN(dia), DAY) + 1 AS dias_esperados, SUM(unidades) AS unidades
FROM `{PROJECT}.retailsmart_smart.demanda_diaria_centro`
GROUP BY centro_id ORDER BY centro_id;
-- esperado: dias = dias_esperados en cada centro
