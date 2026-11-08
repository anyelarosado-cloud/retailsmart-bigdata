-- Consultas para las capturas que faltan del informe técnico.
-- Ejecuta CADA consulta por separado en BigQuery (selecciónala y pulsa Run) y captura el resultado.
-- Nombre de archivo sugerido entre paréntesis.

-- 1) Filas de las tablas de Smart  (bq_filas.png)
-- Esperado: dim_fecha 6939, dim_canal 3, dim_producto 66009, dim_cliente 200467, dim_geo 68, fact_ventas 828423
SELECT table_id, row_count
FROM `retailsmart-bd.retailsmart_smart.__TABLES__`
WHERE table_id IN ('dim_fecha', 'dim_canal', 'dim_producto', 'dim_cliente', 'dim_geo', 'fact_ventas')
ORDER BY table_id;

-- 2) Ventas por región y moneda  (bq_region.png)
-- Esperado: 17 filas; 112.650 líneas en BRL, 181.644 en USD y 534.129 en GBP en total
SELECT g.region, f.moneda, COUNT(*) AS lineas, ROUND(SUM(f.monto)) AS monto
FROM `retailsmart-bd.retailsmart_smart.fact_ventas` f
JOIN `retailsmart-bd.retailsmart_smart.dim_geo` g ON f.geo_sk = g.geo_sk
GROUP BY 1, 2
ORDER BY lineas DESC;

-- 3) Integridad referencial: filas de hechos sin su dimensión  (bq_huerfanas.png)
-- Esperado: las cuatro columnas en 0 (los cliente_sk nulos de UCI no cuentan)
SELECT COUNTIF(p.producto_sk IS NULL) AS sin_producto,
       COUNTIF(g.geo_sk IS NULL) AS sin_geo,
       COUNTIF(d.fecha_sk IS NULL) AS sin_fecha,
       COUNTIF(f.cliente_sk IS NOT NULL AND c.cliente_sk IS NULL) AS sin_cliente
FROM `retailsmart-bd.retailsmart_smart.fact_ventas` f
LEFT JOIN `retailsmart-bd.retailsmart_smart.dim_producto` p ON f.producto_sk = p.producto_sk
LEFT JOIN `retailsmart-bd.retailsmart_smart.dim_geo` g ON f.geo_sk = g.geo_sk
LEFT JOIN `retailsmart-bd.retailsmart_smart.dim_fecha` d ON f.fecha_sk = d.fecha_sk
LEFT JOIN `retailsmart-bd.retailsmart_smart.dim_cliente` c ON f.cliente_sk = c.cliente_sk;

-- 4) Totales del streaming de clics  (df_consulta.png)
-- Esperado tras la prueba de Dataflow: 3020 eventos y 769 visitas a producto. Si las tablas
-- recibieron otras pruebas después, el total puede ser mayor: pega aquí el resultado y lo revisamos.
SELECT 'eventos' AS tabla, SUM(eventos) AS total
FROM `retailsmart-bd.retailsmart_smart.rt_eventos_ventana`
UNION ALL
SELECT 'visitas_producto', SUM(vistas)
FROM `retailsmart-bd.retailsmart_smart.rt_productos_ventana`;

-- 5) Stock sin plazo de reposición (completa la Tabla de verificación del stock; captura opcional)
-- Esperado: 0
SELECT COUNT(*) AS stock_sin_plazo
FROM `retailsmart-bd.retailsmart_smart.erp_stock` s
LEFT JOIN `retailsmart-bd.retailsmart_smart.erp_plazos` p ON s.product_id = p.product_id
WHERE p.product_id IS NULL;
