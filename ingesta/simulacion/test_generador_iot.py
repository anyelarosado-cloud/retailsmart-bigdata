import json
import math
from datetime import datetime, timedelta, timezone

from generador_iot import CENTROS_APROX, UMBRAL_ALERTA_C, Simulador

BASE = datetime(2026, 11, 5, tzinfo=timezone.utc)


def correr(sim, ticks):
    salida = []
    for n in range(ticks):
        salida.extend(sim.tick(n, BASE + timedelta(seconds=n)))
    return salida


def decodificar(salida, tipo=None):
    out = []
    for t, p in salida:
        try:
            m = json.loads(p)
        except Exception:
            continue
        if isinstance(m, dict) and (tipo is None or m.get("tipo") == tipo):
            out.append(m)
    return out


def test_es_reproducible():
    a = correr(Simulador(CENTROS_APROX, seed=7, p_invalido=0.05, p_duplicado=0.05), 80)
    b = correr(Simulador(CENTROS_APROX, seed=7, p_invalido=0.05, p_duplicado=0.05), 80)
    assert a == b
    c = correr(Simulador(CENTROS_APROX, seed=8, p_invalido=0.05, p_duplicado=0.05), 80)
    assert a != c


def test_sin_fallas_nunca_supera_el_umbral():
    sim = Simulador(CENTROS_APROX, p_falla=0.0)
    temps = [m["temp_c"] for m in decodificar(correr(sim, 600), "sensor")]
    assert temps and max(temps) < UMBRAL_ALERTA_C and min(temps) > 0
    assert sim.verdad == []


def test_deriva_supera_el_umbral_solo_en_la_camara_con_falla():
    sim = Simulador(CENTROS_APROX, p_falla=1.0, tipos_falla=["deriva"], horizonte_ticks=600,
                    duracion_falla=(120, 150))
    msgs = decodificar(correr(sim, 600), "sensor")
    assert len(sim.verdad) == len(sim.camaras)
    for f in sim.verdad[:5]:
        lecturas = [m["temp_c"] for m in msgs if m["camara_id"] == f["camara_id"]]
        assert max(lecturas) > UMBRAL_ALERTA_C
        assert f["fin_ts"] is not None


def test_sensor_mudo_deja_de_emitir():
    sim = Simulador(CENTROS_APROX, p_falla=1.0, tipos_falla=["mudo"], horizonte_ticks=300,
                    duracion_falla=(60, 90), vehiculos_por_centro=0)
    msgs = decodificar(correr(sim, 300), "sensor")
    cam = sim.camaras[0]["camara_id"]
    f = sim.fallas[cam]
    emitidos = {int(m["evento_id"].rsplit("-", 1)[1]) for m in msgs if m["camara_id"] == cam}
    assert all(n not in emitidos for n in range(f["ini"], f["fin"]))
    assert f["ini"] - 1 in emitidos


def test_lectura_invalida_queda_fuera_del_rango_fisico():
    sim = Simulador(CENTROS_APROX, p_falla=1.0, tipos_falla=["invalida"], horizonte_ticks=300,
                    duracion_falla=(60, 90))
    temps = [m["temp_c"] for m in decodificar(correr(sim, 300), "sensor")]
    assert -999.0 in temps and sim.stats["lecturas_fuera_de_rango"] > 0


def test_invalidos_y_duplicados_se_inyectan_y_se_cuentan():
    sim = Simulador(CENTROS_APROX, p_falla=0.0, p_invalido=0.1, p_duplicado=0.1)
    salida = correr(sim, 60)
    assert sim.stats["invalidos"] > 0 and sim.stats["duplicados"] > 0
    ids = [m["evento_id"] for m in decodificar(salida) if "evento_id" in m]
    assert len(ids) - len(set(ids)) == sim.stats["duplicados"]
    no_parsean = sum(1 for _, p in salida if not _parsea(p))
    assert no_parsean > 0


def _parsea(p):
    try:
        json.loads(p)
        return True
    except Exception:
        return False


def test_gps_se_mantiene_cerca_de_su_centro():
    sim = Simulador(CENTROS_APROX, p_falla=0.0, camaras_por_centro=0)
    msgs = decodificar(correr(sim, 800), "gps")
    centros = {c: (lat, lon) for c, lat, lon in CENTROS_APROX}
    for m in msgs:
        lat0, lon0 = centros[m["centro_id"]]
        dx = (m["lon"] - lon0) * 111 * math.cos(math.radians(lat0))
        dy = (m["lat"] - lat0) * 111
        assert math.hypot(dx, dy) < 60


def test_los_mensajes_no_llevan_la_verdad_de_terreno():
    sim = Simulador(CENTROS_APROX, p_falla=1.0)
    for m in decodificar(correr(sim, 200)):
        assert not ({"tipo_falla", "falla", "verdad"} & set(m))
