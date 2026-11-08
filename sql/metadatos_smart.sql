-- Descripciones y etiquetas de la zona Smart (catálogo de datos).
-- BigQuery permite como máximo 5 actualizaciones de metadatos por tabla cada 10 segundos, por eso cada
-- tabla se actualiza con DOS sentencias: una para la tabla (descripción y etiquetas) y una para TODAS sus columnas.
-- Ejecuta el archivo completo. Es seguro repetirlo; si ves el error de límite, espera 30 segundos y repite.

-- ===== fact_ventas =====
ALTER TABLE `retailsmart-bd.retailsmart_smart.fact_ventas` SET OPTIONS (
  description = 'Una fila por línea de venta de las tres fuentes (thelook, Olist y UCI). Montos en la moneda de origen.',
  labels = [('dominio', 'comercial'), ('zona', 'smart'), ('sensibilidad', 'baja')]);
ALTER TABLE `retailsmart-bd.retailsmart_smart.fact_ventas`
  ALTER COLUMN _cargado_en SET OPTIONS (description = 'Fecha y hora de carga de la fila en la zona Smart (columna de trazabilidad).'),
  ALTER COLUMN canal_sk SET OPTIONS (description = 'Clave de dim_canal.'),
  ALTER COLUMN cantidad SET OPTIONS (description = 'Unidades vendidas. Negativa en las cancelaciones de UCI.'),
  ALTER COLUMN cliente_sk SET OPTIONS (description = 'Clave sustituta de dim_cliente. Nula en las líneas de UCI sin cliente identificado.'),
  ALTER COLUMN costo_envio SET OPTIONS (description = 'Costo de envío de la línea. Solo disponible en Olist.'),
  ALTER COLUMN estado SET OPTIONS (description = 'Estado del pedido en la fuente de origen.'),
  ALTER COLUMN fecha_sk SET OPTIONS (description = 'Clave de dim_fecha en formato aaaammdd.'),
  ALTER COLUMN fechas_inconsistentes SET OPTIONS (description = 'Verdadero si las fechas de envío o entrega del ítem son anteriores a las de creación o envío (thelook).'),
  ALTER COLUMN fuente SET OPTIONS (description = 'Fuente de la línea: thelook, olist o uci.'),
  ALTER COLUMN geo_sk SET OPTIONS (description = 'Clave de dim_geo.'),
  ALTER COLUMN id_linea SET OPTIONS (description = 'Identificador de la línea dentro del pedido.'),
  ALTER COLUMN id_pedido SET OPTIONS (description = 'Identificador del pedido o la factura en la fuente de origen.'),
  ALTER COLUMN moneda SET OPTIONS (description = 'Moneda del monto: BRL, GBP o USD.'),
  ALTER COLUMN monto SET OPTIONS (description = 'Monto de la línea en la moneda de origen; no se suma entre monedas.'),
  ALTER COLUMN producto_sk SET OPTIONS (description = 'Clave sustituta de dim_producto: hash de la fuente y el producto de origen.');

-- ===== dim_fecha =====
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_fecha` SET OPTIONS (
  description = 'Calendario diario de 2009-01-01 a 2027-12-31.',
  labels = [('dominio', 'comercial'), ('zona', 'smart'), ('sensibilidad', 'baja')]);
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_fecha`
  ALTER COLUMN _cargado_en SET OPTIONS (description = 'Fecha y hora de carga de la fila en la zona Smart.'),
  ALTER COLUMN anio SET OPTIONS (description = 'Año de la fecha.'),
  ALTER COLUMN dia SET OPTIONS (description = 'Día del mes.'),
  ALTER COLUMN dia_semana SET OPTIONS (description = 'Día de la semana como número.'),
  ALTER COLUMN es_fin_de_semana SET OPTIONS (description = 'Verdadero si la fecha cae en sábado o domingo.'),
  ALTER COLUMN fecha SET OPTIONS (description = 'Fecha del calendario.'),
  ALTER COLUMN fecha_sk SET OPTIONS (description = 'Clave de la fecha en formato aaaammdd.'),
  ALTER COLUMN mes SET OPTIONS (description = 'Mes del año, de 1 a 12.'),
  ALTER COLUMN trimestre SET OPTIONS (description = 'Trimestre del año, de 1 a 4.');

-- ===== dim_canal =====
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_canal` SET OPTIONS (
  description = 'Canal de venta: web (thelook), marketplace (Olist) o tienda mayorista (UCI).',
  labels = [('dominio', 'comercial'), ('zona', 'smart'), ('sensibilidad', 'baja')]);
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_canal`
  ALTER COLUMN _cargado_en SET OPTIONS (description = 'Fecha y hora de carga de la fila en la zona Smart.'),
  ALTER COLUMN canal SET OPTIONS (description = 'Canal de venta: web, marketplace o tienda_mayorista.'),
  ALTER COLUMN canal_sk SET OPTIONS (description = 'Clave sustituta del canal.'),
  ALTER COLUMN fuente SET OPTIONS (description = 'Fuente de datos del canal: thelook, olist o uci.');

-- ===== dim_producto =====
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_producto` SET OPTIONS (
  description = 'Un producto por fuente, con su unidad de negocio.',
  labels = [('dominio', 'comercial'), ('zona', 'smart'), ('sensibilidad', 'baja')]);
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_producto`
  ALTER COLUMN _cargado_en SET OPTIONS (description = 'Fecha y hora de carga de la fila en la zona Smart.'),
  ALTER COLUMN categoria_origen SET OPTIONS (description = 'Categoría del producto en la fuente de origen.'),
  ALTER COLUMN fuente SET OPTIONS (description = 'Fuente del producto: thelook, olist o uci.'),
  ALTER COLUMN id_origen SET OPTIONS (description = 'Identificador del producto en la fuente de origen.'),
  ALTER COLUMN marca SET OPTIONS (description = 'Marca del producto, cuando la fuente la informa (thelook).'),
  ALTER COLUMN nombre SET OPTIONS (description = 'Nombre o descripción del producto.'),
  ALTER COLUMN producto_sk SET OPTIONS (description = 'Clave sustituta del producto: hash de la fuente y el producto de origen.'),
  ALTER COLUMN unidad_negocio SET OPTIONS (description = 'Unidad de negocio asignada al producto en el modelo, por ejemplo Moda y accesorios.');

-- ===== dim_cliente =====
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_cliente` SET OPTIONS (
  description = 'Un cliente por fuente. El correo se guarda como hash; no incluye nombres ni direcciones.',
  labels = [('dominio', 'comercial'), ('zona', 'smart'), ('sensibilidad', 'media')]);
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_cliente`
  ALTER COLUMN _cargado_en SET OPTIONS (description = 'Fecha y hora de carga de la fila en la zona Smart.'),
  ALTER COLUMN ciudad SET OPTIONS (description = 'Ciudad del cliente en la fuente de origen; nula si la fuente no la informa.'),
  ALTER COLUMN cliente_sk SET OPTIONS (description = 'Clave sustituta del cliente: hash de la fuente y el cliente de origen.'),
  ALTER COLUMN edad SET OPTIONS (description = 'Edad del cliente en años, cuando la fuente la informa (thelook).'),
  ALTER COLUMN email_hash SET OPTIONS (description = 'Hash del correo electrónico del cliente; el correo original no se almacena en Smart.'),
  ALTER COLUMN estado_origen SET OPTIONS (description = 'Estado o provincia del cliente en la fuente de origen.'),
  ALTER COLUMN fecha_alta SET OPTIONS (description = 'Fecha de alta del cliente en la fuente de origen, cuando existe.'),
  ALTER COLUMN fuente SET OPTIONS (description = 'Fuente del cliente: thelook, olist o uci.'),
  ALTER COLUMN genero SET OPTIONS (description = 'Género del cliente según la fuente (thelook); nulo en las demás.'),
  ALTER COLUMN id_origen SET OPTIONS (description = 'Identificador del cliente en la fuente de origen.'),
  ALTER COLUMN pais_origen SET OPTIONS (description = 'País del cliente en la fuente de origen.');

-- ===== dim_geo =====
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_geo` SET OPTIONS (
  description = 'País y, para Brasil, estado, con la región logística definida en el modelo.',
  labels = [('dominio', 'comercial'), ('zona', 'smart'), ('sensibilidad', 'baja')]);
ALTER TABLE `retailsmart-bd.retailsmart_smart.dim_geo`
  ALTER COLUMN _cargado_en SET OPTIONS (description = 'Fecha y hora de carga de la fila en la zona Smart.'),
  ALTER COLUMN estado SET OPTIONS (description = 'Estado de Brasil en siglas; nulo en los demás países.'),
  ALTER COLUMN geo_sk SET OPTIONS (description = 'Clave sustituta de la ubicación.'),
  ALTER COLUMN pais SET OPTIONS (description = 'Nombre del país, normalizado entre fuentes.'),
  ALTER COLUMN region SET OPTIONS (description = 'Región logística definida en el modelo: cinco de Brasil, Europa, Norteamérica, Asia-Pacífico y resto, o sin asignar.');

-- ===== Revisión: columnas que aún no tienen descripción (ejecuta esta consulta aparte; debe salir vacía) =====
-- SELECT table_name, field_path, data_type
-- FROM `retailsmart-bd.retailsmart_smart.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
-- WHERE table_name IN ('fact_ventas', 'dim_fecha', 'dim_canal', 'dim_producto', 'dim_cliente', 'dim_geo')
--   AND (description IS NULL OR description = '')
-- ORDER BY table_name, field_path;
