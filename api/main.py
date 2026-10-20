import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

PROYECTO = os.environ.get("PROYECTO", "retailsmart-bd")
DS = f"{PROYECTO}.retailsmart_smart"

app = FastAPI(
    title="RetailSmart API",
    version="1.0.0",
    description="Datos de ventas (zona Smart) y agregados en tiempo real de RetailSmart.",
)

_cliente = None
RUTA_PANEL = Path(__file__).with_name("panel.html")


def bq():
    global _cliente
    if _cliente is None:
        _cliente = bigquery.Client(project=PROYECTO)
    return _cliente


def consultar(sql, parametros):
    cfg = bigquery.QueryJobConfig(query_parameters=parametros)
    return [dict(fila) for fila in bq().query(sql, job_config=cfg).result()]


def texto(nombre, valor):
    return bigquery.ScalarQueryParameter(nombre, "STRING", valor)


def entero(nombre, valor):
    return bigquery.ScalarQueryParameter(nombre, "INT64", valor)


@app.get("/", include_in_schema=False)
def panel():
    return FileResponse(RUTA_PANEL, media_type="text/html")


@app.get("/v1/salud", tags=["operacion"])
def salud():
    return {"estado": "ok"}


@app.get("/v1/ventas/resumen", tags=["ventas"])
def ventas_resumen(
    region: Optional[str] = Query(None, description="Región logística, por ejemplo 'Brasil Sudeste'"),
    moneda: Optional[str] = Query(None, pattern="^[A-Z]{3}$", description="BRL, GBP o USD"),
):
    """Líneas y monto de venta por región y moneda. Los montos no se suman entre monedas."""
    sql = f"""
    SELECT g.region, f.moneda, COUNT(*) AS lineas, ROUND(SUM(f.monto), 2) AS monto
    FROM `{DS}.fact_ventas` f
    JOIN `{DS}.dim_geo` g ON f.geo_sk = g.geo_sk
    WHERE (@region IS NULL OR g.region = @region)
      AND (@moneda IS NULL OR f.moneda = @moneda)
    GROUP BY g.region, f.moneda
    ORDER BY lineas DESC"""
    filas = consultar(sql, [texto("region", region), texto("moneda", moneda)])
    return {"filtros": {"region": region, "moneda": moneda}, "resultados": filas}


@app.get("/v1/ventas/por-unidad", tags=["ventas"])
def ventas_por_unidad(
    moneda: Optional[str] = Query(None, pattern="^[A-Z]{3}$"),
    limite: int = Query(10, ge=1, le=50),
):
    """Líneas y monto por unidad de negocio y moneda."""
    sql = f"""
    SELECT p.unidad_negocio, f.moneda, COUNT(*) AS lineas, ROUND(SUM(f.monto), 2) AS monto
    FROM `{DS}.fact_ventas` f
    JOIN `{DS}.dim_producto` p ON f.producto_sk = p.producto_sk
    WHERE (@moneda IS NULL OR f.moneda = @moneda)
    GROUP BY p.unidad_negocio, f.moneda
    ORDER BY monto DESC
    LIMIT @limite"""
    filas = consultar(sql, [texto("moneda", moneda), entero("limite", limite)])
    return {"filtros": {"moneda": moneda, "limite": limite}, "resultados": filas}


@app.get("/v1/tiempo-real/eventos", tags=["tiempo real"])
def eventos_tiempo_real(
    minutos: int = Query(5, ge=1, le=1440, description="Ventana hacia atrás, en minutos"),
    pais: Optional[str] = Query(None, description="País, por ejemplo 'Brazil'"),
):
    """Eventos por tipo en los últimos minutos, a partir de los agregados del pipeline de streaming."""
    sql = f"""
    SELECT event_type, SUM(eventos) AS eventos
    FROM `{DS}.rt_eventos_ventana`
    WHERE ventana_inicio >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutos MINUTE)
      AND (@pais IS NULL OR pais = @pais)
    GROUP BY event_type
    ORDER BY eventos DESC"""
    try:
        filas = consultar(sql, [entero("minutos", minutos), texto("pais", pais)])
    except NotFound:
        filas = []
    return {"minutos": minutos, "pais": pais, "resultados": filas}


@app.get("/v1/tiempo-real/productos-top", tags=["tiempo real"])
def productos_top(
    minutos: int = Query(5, ge=1, le=1440),
    pais: Optional[str] = Query(None),
    limite: int = Query(10, ge=1, le=50),
):
    """Productos más vistos en los últimos minutos."""
    sql = f"""
    SELECT v.producto_id, ANY_VALUE(p.nombre) AS nombre,
           ANY_VALUE(p.unidad_negocio) AS unidad_negocio, SUM(v.vistas) AS vistas
    FROM `{DS}.rt_productos_ventana` v
    LEFT JOIN `{DS}.dim_producto` p
      ON p.fuente = 'thelook' AND p.id_origen = CAST(v.producto_id AS STRING)
    WHERE v.ventana_inicio >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutos MINUTE)
      AND (@pais IS NULL OR v.pais = @pais)
    GROUP BY v.producto_id
    ORDER BY vistas DESC, v.producto_id
    LIMIT @limite"""
    try:
        filas = consultar(sql, [entero("minutos", minutos), texto("pais", pais), entero("limite", limite)])
    except NotFound:
        filas = []
    return {"minutos": minutos, "pais": pais, "limite": limite, "resultados": filas}
