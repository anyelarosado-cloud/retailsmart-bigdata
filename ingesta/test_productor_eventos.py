import random
from productor_eventos import tasa_objetivo


def test_la_tasa_oscila_alrededor_de_la_base_y_no_es_negativa():
    rng = random.Random(1)
    v = [tasa_objetivo(t, 40, rng) for t in range(180)]
    assert min(v) >= 0
    assert min(v) < 40 < max(v)                    # sube y baja
    assert 30 <= sum(v) / len(v) <= 50             # el promedio queda cerca de la base


def test_es_reproducible_con_la_misma_semilla():
    a = [tasa_objetivo(t, 40, random.Random(7)) for t in range(30)]
    b = [tasa_objetivo(t, 40, random.Random(7)) for t in range(30)]
    assert a == b


def test_base_cero_no_publica_nada():
    assert all(tasa_objetivo(t, 0, random.Random(2)) == 0 for t in range(10))
