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
    for ruta in ["/v1/tiempo-real/eventos", "/v1/tiempo-real/productos-top", "/v1/ventas/resumen",
                 "/v1/tiempo-real/alertas-frio"]:
        assert ruta in r.text


# ---------- agente ----------
import agente
from google.genai import types as gtypes


class FakeModelos:
    """Simula a Gemini: llama a una herramienta y devuelve texto. No usa red."""
    def __init__(self, herramienta, args):
        self.herramienta, self.args = herramienta, args

    def generate_content(self, model, contents, config):
        f = {t.__name__: t for t in config.tools}[self.herramienta]
        datos = f(**self.args)
        return type("R", (), {"text": f"Respuesta con {len(datos['filas'])} filas" if "filas" in datos else "texto"})()


class FakeCliente:
    def __init__(self, herramienta, args):
        self.models = FakeModelos(herramienta, args)


def test_agente_plan_usa_parametros_y_registra_traza():
    llamadas.clear()
    r = agente.responder("¿Qué reponer en el centro 3?", main.consultar, main.texto, main.entero,
                         cliente=FakeCliente("plan_reposicion", {"centro_id": "3", "limite": 5}))
    assert r["herramientas"][0]["herramienta"] == "plan_reposicion"
    sql, params = llamadas[-1]
    assert params == {"centro": "3", "categoria": None, "limite": 5}
    assert "@centro" in sql and "3" not in sql.replace("30", "")  # el valor va como parámetro, no en el SQL


def test_agente_limita_el_tope_de_filas():
    llamadas.clear()
    agente.responder("x", main.consultar, main.texto, main.entero,
                     cliente=FakeCliente("plan_reposicion", {"limite": 9999}))
    assert llamadas[-1][1]["limite"] == 30


def test_agente_rechaza_pregunta_vacia_o_larga():
    for p in ("   ", "a" * 501):
        try:
            agente.responder(p, main.consultar, main.texto, main.entero, cliente=FakeCliente("metodologia", {}))
            assert False
        except ValueError:
            pass


def test_metodologia_reporta_wape_sin_maquillar():
    traza = []
    h = {f.__name__: f for f in agente.construir_herramientas(main.consultar, main.texto, main.entero, traza)}
    t = h["metodologia"]("validacion")["texto"]
    assert "0,238" in t and "0,209" in t and "NO supera" in t


def test_limite_de_uso():
    t = [0.0]
    lim = agente.LimiteDeUso(maximo=2, reloj=lambda: t[0])
    assert lim.permitir() and lim.permitir() and not lim.permitir()
    t[0] = 61
    assert lim.permitir()


def test_endpoint_agente(monkeypatch):
    monkeypatch.setattr(agente, "responder", lambda *a, **k: {"respuesta": "ok", "herramientas": [], "modelo": "m"})
    assert c.post("/v1/agente", json={"pregunta": "hola"}).json()["respuesta"] == "ok"
    monkeypatch.setattr(agente, "responder", lambda *a, **k: (_ for _ in ()).throw(ValueError("vacía")))
    assert c.post("/v1/agente", json={"pregunta": " "}).status_code == 422
    monkeypatch.setattr(agente, "responder", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    assert c.post("/v1/agente", json={"pregunta": "hola"}).status_code == 502


def test_plan_marca_truncado_y_hay_herramienta_por_categoria(monkeypatch):
    filas = [{"categoria": "Swim", "reposicion_sugerida": 14, "filas_totales": 4}]
    r = agente.construir_herramientas(lambda sql, p: filas, main.texto, main.entero, [])
    h = {f.__name__: f for f in r}
    out = h["plan_reposicion"](categoria="Swim", limite=1)
    assert out["truncado"] is True and out["filas_totales"] == 4 and out["filas_devueltas"] == 1
    assert "reposicion_por_categoria" in h
    llamadas.clear()
    h2 = {f.__name__: f for f in agente.construir_herramientas(main.consultar, main.texto, main.entero, [])}
    h2["reposicion_por_categoria"](limite=5)
    assert "SUM(reposicion_sugerida)" in llamadas[-1][0] and llamadas[-1][1] == {"limite": 5}


def test_alertas_frio_parametros_total_y_tabla_inexistente():
    def alertas(sql, parametros):
        llamadas.append((sql, {p.name: p.value for p in parametros}))
        return [{"ventana_inicio": datetime.datetime(2026, 11, 20, 9, 0), "camara_id": "C01-01", "centro_id": 1,
                 "regla": "sobretemperatura", "temp_media": 9.1, "temp_max": 10.0, "total": 3}]
    main.consultar = alertas
    r = c.get("/v1/tiempo-real/alertas-frio", params={"minutos": 15, "centro_id": 1})
    assert r.status_code == 200 and llamadas[-1][1] == {"minutos": 15, "centro": "1"}
    assert "rt_alertas_frio" in llamadas[-1][0]
    assert r.json()["total"] == 3 and "total" not in r.json()["resultados"][0]
    c.get("/v1/tiempo-real/alertas-frio")
    assert llamadas[-1][1] == {"minutos": 60, "centro": None}
    assert c.get("/v1/tiempo-real/alertas-frio", params={"minutos": 0}).status_code == 422
    assert c.get("/v1/tiempo-real/alertas-frio", params={"centro_id": 0}).status_code == 422

    def sin_tabla(sql, parametros): raise NotFound("no existe")
    main.consultar = sin_tabla
    r = c.get("/v1/tiempo-real/alertas-frio")
    assert r.status_code == 200 and r.json()["resultados"] == [] and r.json()["total"] == 0
    main.consultar = falso


def test_herramientas_devuelven_fechas_y_decimales_serializables_a_json():
    """El SDK del modelo hace json.dumps de lo que devuelven las herramientas: una fecha de BigQuery
    (columna dia del pronóstico) o una marca de tiempo (alertas) no debe romper la respuesta."""
    import json
    from datetime import date, datetime, timezone
    from decimal import Decimal

    def consultar_falso(sql, params):
        return [{"dia": date(2026, 11, 21), "ventana_inicio": datetime(2026, 11, 20, 19, 45, tzinfo=timezone.utc),
                 "unidades_previstas": Decimal("12.5")}]

    traza = []
    h = {f.__name__: f for f in agente.construir_herramientas(consultar_falso, main.texto, main.entero, traza)}
    for nombre in ("pronostico_demanda", "alertas_frio"):
        salida = h[nombre]()
        json.dumps(salida)                       # no debe lanzar TypeError
    assert h["pronostico_demanda"]()["filas"][0]["dia"] == "2026-11-21"
