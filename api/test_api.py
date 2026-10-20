import datetime
import main
from fastapi.testclient import TestClient
from google.api_core.exceptions import NotFound

c = TestClient(main.app)
llamadas = []

def falso(sql, parametros):
    llamadas.append((sql, {p.name: p.value for p in parametros}))
    return [{"region": "Brasil Sudeste", "moneda": "BRL", "lineas": 5, "monto": 10.5,
             "ventana": datetime.datetime(2026, 10, 18, 6, 4)}]

main.consultar = falso

def test_salud():
    assert c.get("/v1/salud").json() == {"estado": "ok"}

def test_resumen_sin_filtros_y_con_filtros():
    r = c.get("/v1/ventas/resumen"); assert r.status_code == 200
    assert llamadas[-1][1] == {"region": None, "moneda": None}
    r = c.get("/v1/ventas/resumen", params={"region": "Brasil Sudeste", "moneda": "BRL"})
    assert r.status_code == 200 and llamadas[-1][1] == {"region": "Brasil Sudeste", "moneda": "BRL"}
    assert r.json()["resultados"][0]["lineas"] == 5

def test_validaciones():
    assert c.get("/v1/ventas/resumen", params={"moneda": "brl"}).status_code == 422
    assert c.get("/v1/ventas/por-unidad", params={"limite": 0}).status_code == 422
    assert c.get("/v1/ventas/por-unidad", params={"limite": 51}).status_code == 422
    assert c.get("/v1/tiempo-real/eventos", params={"minutos": 0}).status_code == 422
    assert c.get("/v1/tiempo-real/eventos", params={"minutos": 1441}).status_code == 422

def test_tiempo_real_parametros():
    r = c.get("/v1/tiempo-real/productos-top", params={"minutos": 30, "pais": "Brazil", "limite": 3})
    assert r.status_code == 200
    assert llamadas[-1][1] == {"minutos": 30, "pais": "Brazil", "limite": 3}
    r = c.get("/v1/tiempo-real/eventos")
    assert llamadas[-1][1] == {"minutos": 5, "pais": None}

def test_tabla_inexistente_devuelve_lista_vacia():
    def sin_tabla(sql, parametros): raise NotFound("no existe")
    main.consultar = sin_tabla
    r = c.get("/v1/tiempo-real/eventos")
    assert r.status_code == 200 and r.json()["resultados"] == []
    main.consultar = falso

def test_sql_usa_los_nombres_reales():
    c.get("/v1/ventas/por-unidad"); sql = llamadas[-1][0]
    assert "retailsmart-bd.retailsmart_smart.fact_ventas" in sql and "unidad_negocio" in sql
    c.get("/v1/tiempo-real/productos-top"); sql = llamadas[-1][0]
    assert "rt_productos_ventana" in sql and "p.id_origen = CAST(v.producto_id AS STRING)" in sql


def test_panel_se_sirve_y_usa_rutas_de_la_api():
    r = c.get("/")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    for ruta in ["/v1/tiempo-real/eventos", "/v1/tiempo-real/productos-top", "/v1/ventas/resumen"]:
        assert ruta in r.text
