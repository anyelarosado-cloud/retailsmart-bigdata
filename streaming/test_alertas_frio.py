"""Pruebas sin conexión: usan el generador de sensores y el runner local de Beam en modo por lotes."""
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam import window
from apache_beam.transforms.window import TimestampedValue

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingesta", "simulacion"))
from generador_iot import CENTROS_APROX, Simulador  # noqa: E402
from alertas_frio import camaras_esperadas, construir, interpretar  # noqa: E402

BASE = datetime(2026, 11, 5, tzinfo=timezone.utc)


def mensajes_con_tiempo(sim, ticks):
    out = []
    for n in range(ticks):
        ts = BASE + timedelta(seconds=n)
        for _, payload in sim.tick(n, ts):
            out.append((payload, ts.timestamp()))
    return out


def ejecutar(entradas, ventana_seg=60, por_centro=4):
    """Corre el pipeline por lotes y devuelve las salidas leídas de archivos (los runners serializan
    los DoFn, por lo que no se puede mutar un diccionario local)."""
    res = {}
    with tempfile.TemporaryDirectory() as d:
        with TestPipeline(runner="BundleBasedDirectRunner") as p:
            con_tiempo = (p | beam.Create(entradas) | "Tiempo" >> beam.Map(lambda x: TimestampedValue(x[0], x[1])))
            agregados, alertas, errores = construir(con_tiempo, ventana_seg=ventana_seg, esperadas=camaras_esperadas(10, por_centro))
            for nombre, pc in (("agregados", agregados), ("alertas", alertas), ("errores", errores)):
                (pc
                 | f"Global_{nombre}" >> beam.WindowInto(window.GlobalWindows())
                 | f"Json_{nombre}" >> beam.Map(json.dumps)
                 | f"Archivo_{nombre}" >> beam.io.WriteToText(os.path.join(d, nombre), file_name_suffix=".jsonl",
                                                              shard_name_template=""))
        for nombre in ("agregados", "alertas", "errores"):
            ruta = os.path.join(d, nombre + ".jsonl")
            res[nombre] = [json.loads(l) for l in open(ruta, encoding="utf-8")] if os.path.exists(ruta) else []
    return res


def ts(fila, campo):
    return datetime.fromisoformat(fila[campo]).timestamp()


def test_interpretar_casos():
    ok = json.dumps({"evento_id": "s-1", "tipo": "sensor", "camara_id": "C01-01", "centro_id": 1, "temp_c": 4.1}).encode()
    assert interpretar(ok)[0] == "ok"
    assert interpretar(b"{no es json")[1]["motivo"] == "json_invalido"
    assert interpretar(json.dumps({"tipo": "sensor"}).encode())[1]["motivo"] == "falta_evento_id"
    texto = json.dumps({"evento_id": "s-1", "tipo": "sensor", "camara_id": "C01-01", "centro_id": 1, "temp_c": "NaN"}).encode()
    assert interpretar(texto)[1]["motivo"] == "tipo_de_dato_incorrecto"
    imposible = json.dumps({"evento_id": "s-1", "tipo": "sensor", "camara_id": "C01-01", "centro_id": 1, "temp_c": -999.0}).encode()
    estado, dato = interpretar(imposible)
    assert estado == "fuera_de_rango" and dato["motivo"] == "fuera_de_rango"


def test_sin_fallas_no_hay_alertas():
    sim = Simulador(CENTROS_APROX, p_falla=0.0, vehiculos_por_centro=0)
    res = ejecutar(mensajes_con_tiempo(sim, 240))
    assert res["alertas"] == [] or all(a["regla"] == "sin_datos" and a["ventana_inicio"] >= "2026-11-05T00:03:00" for a in res["alertas"])
    assert res["errores"] == []
    assert len(res["agregados"]) > 0


def detectadas(res, falla, tolerancia_s=120):
    ini = datetime.fromisoformat(falla["inicio_ts"]).timestamp()
    fin = datetime.fromisoformat(falla["fin_ts"]).timestamp()
    return [a for a in res["alertas"] if a["camara_id"] == falla["camara_id"]
            and ts(a, "ventana_fin") > ini and ts(a, "ventana_inicio") < fin + tolerancia_s]


def probar_tipo(tipo, regla):
    sim = Simulador(CENTROS_APROX, camaras_por_centro=2, vehiculos_por_centro=0, p_falla=0.5, tipos_falla=[tipo],
                    horizonte_ticks=400, duracion_falla=(150, 170), seed=3)
    entradas = mensajes_con_tiempo(sim, 400)
    res = ejecutar(entradas, por_centro=2)
    assert sim.verdad, "la prueba necesita al menos una falla"
    for f in sim.verdad:
        encontradas = detectadas(res, f)
        assert encontradas, f"falla {f['camara_id']} no detectada"
        assert all(a["regla"] == regla for a in encontradas if ts(a, "ventana_inicio") < ts(a, "ventana_fin"))
    # ninguna cámara sin falla genera alertas de sobretemperatura
    con_falla = {f["camara_id"] for f in sim.verdad}
    falsas = [a for a in res["alertas"] if a["camara_id"] not in con_falla]
    return res, falsas


def test_deriva_genera_sobretemperatura():
    res, falsas = probar_tipo("deriva", "sobretemperatura")
    assert falsas == []


def test_sensor_mudo_genera_sin_datos():
    res, falsas = probar_tipo("mudo", "sin_datos")
    assert falsas == []


def test_lecturas_invalidas_generan_sin_datos_y_errores():
    res, falsas = probar_tipo("invalida", "sin_datos")
    assert falsas == []
    assert res["errores"] and all(e["motivo"] == "fuera_de_rango" for e in res["errores"])


def test_duplicados_no_cambian_las_lecturas():
    sim1 = Simulador(CENTROS_APROX, camaras_por_centro=1, vehiculos_por_centro=0, p_falla=0.0, seed=5)
    sim2 = Simulador(CENTROS_APROX, camaras_por_centro=1, vehiculos_por_centro=0, p_falla=0.0, seed=5, p_duplicado=0.3)
    a = ejecutar(mensajes_con_tiempo(sim1, 120), por_centro=1)
    b = ejecutar(mensajes_con_tiempo(sim2, 120), por_centro=1)
    clave = lambda r: (r["camara_id"], r["ventana_inicio"], r["lecturas"])
    assert sorted(map(clave, a["agregados"])) == sorted(map(clave, b["agregados"]))
    assert sim2.stats["duplicados"] > 0


def test_mensajes_malformados_van_a_errores():
    sim = Simulador(CENTROS_APROX, camaras_por_centro=1, vehiculos_por_centro=0, p_falla=0.0, p_invalido=0.2, seed=9)
    res = ejecutar(mensajes_con_tiempo(sim, 120), por_centro=1)
    assert len(res["errores"]) == sim.stats["invalidos"]


def test_camaras_esperadas():
    c = camaras_esperadas(10, 4)
    assert len(c) == 40 and c[0] == "C01-01" and c[-1] == "C10-04"
