from medir_deteccion import evaluar

FALLAS = [
    {"camara_id": "C01-01", "tipo_falla": "deriva", "inicio_ts": "2026-11-06T21:54:06+00:00", "fin_ts": "2026-11-06T21:56:26+00:00"},
    {"camara_id": "C10-01", "tipo_falla": "mudo", "inicio_ts": "2026-11-06T21:53:37+00:00", "fin_ts": "2026-11-06T21:55:25+00:00"},
    {"camara_id": "C05-04", "tipo_falla": "invalida", "inicio_ts": "2026-11-06T21:55:13+00:00", "fin_ts": "2026-11-06T21:56:37+00:00"},
]


def alerta(cam, regla, ini, fin):
    return {"camara_id": cam, "regla": regla, "ventana_inicio": f"2026-11-06T{ini}+00:00", "ventana_fin": f"2026-11-06T{fin}+00:00"}


def test_detecta_y_mide_tiempos():
    alertas = [alerta("C01-01", "sobretemperatura", "21:55:00", "21:55:30"),
               alerta("C10-01", "sin_datos", "21:54:00", "21:54:30")]
    r = evaluar(FALLAS, alertas)
    assert r["fallas_inyectadas"] == 3 and r["fallas_detectadas"] == 2
    assert r["alertas_sin_falla"] == 0
    assert r["por_tipo"]["invalida"] == {"inyectadas": 1, "detectadas": 0}
    assert r["tiempo_medio_deteccion_s"] == (84 + 53) / 2


def test_regla_equivocada_o_camara_ajena_cuentan_como_falsas():
    alertas = [alerta("C01-01", "sin_datos", "21:55:00", "21:55:30"),      # regla que no corresponde a una deriva
               alerta("C07-02", "sobretemperatura", "21:55:00", "21:55:30")]  # cámara sin falla
    r = evaluar(FALLAS, alertas)
    assert r["fallas_detectadas"] == 0
    assert r["alertas_sin_falla"] == 1
