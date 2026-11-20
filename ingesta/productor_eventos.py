"""Reproduce clics de thelook hacia Pub/Sub (tópico clicks) con marcas de tiempo actuales.

Dos modos:
  * Por lotes (por defecto): publica --n mensajes a --rate por segundo y termina.
        python3 ingesta/productor_eventos.py --n 2000 --rate 100
  * Continuo (--duracion): simula tráfico normal. Toma una muestra de --pool eventos y
    publica durante --duracion segundos (0 = hasta Ctrl+C) un ritmo que sube y baja alrededor de
    --rate, con ruido, para que los paneles cambien todo el tiempo.
        python3 ingesta/productor_eventos.py --duracion 900 --rate 40
"""
import argparse, json, math, random, time
from datetime import datetime, timezone

PROJECT, TOPIC = "retailsmart-bd", "clicks"
ALIAS = {"Brasil": "Brazil", "España": "Spain"}


def tasa_objetivo(t, base, rng, amplitud=0.4, periodo=90.0, ruido=0.15):
    """Mensajes a publicar en el segundo t: una onda alrededor de base, con ruido. Nunca negativo."""
    onda = 1 + amplitud * math.sin(2 * math.pi * t / periodo)
    return max(0, round(base * onda * (1 + rng.uniform(-ruido, ruido))))


def _muestra(bq, n):
    sql = f"""
    SELECT e.id, e.user_id, e.session_id, e.event_type, e.uri, e.browser,
           e.traffic_source, u.country AS pais, u.state AS estado
    FROM `{PROJECT}.retailsmart_raw.events` e
    JOIN `{PROJECT}.retailsmart_raw.users` u ON u.id = e.user_id
    ORDER BY RAND() LIMIT {n}"""
    return list(bq.query(sql).result())


def _mensaje(r):
    return json.dumps({
        "event_id": r.id, "user_id": r.user_id, "session_id": r.session_id,
        "event_type": r.event_type, "uri": r.uri, "browser": r.browser,
        "traffic_source": r.traffic_source,
        "pais": ALIAS.get(r.pais, r.pais), "estado": r.estado,
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }).encode("utf-8")


def por_lotes(n, rate):
    from google.cloud import bigquery, pubsub_v1
    filas = _muestra(bigquery.Client(project=PROJECT), n)
    pub = pubsub_v1.PublisherClient()
    path = pub.topic_path(PROJECT, TOPIC)
    futuros = []
    for i, r in enumerate(filas, start=1):
        futuros.append(pub.publish(path, _mensaje(r), fuente="thelook"))
        if rate and i % rate == 0:
            time.sleep(1)
    for f in futuros:
        f.result()
    print(f"publicados {len(futuros)} mensajes")


def continuo(duracion, rate, pool, semilla=None):
    from google.cloud import bigquery, pubsub_v1
    rng = random.Random(semilla)
    filas = _muestra(bigquery.Client(project=PROJECT), pool)
    pub = pubsub_v1.PublisherClient()
    path = pub.topic_path(PROJECT, TOPIC)
    inicio, total, t = time.time(), 0, 0
    print(f"tráfico continuo: ritmo base {rate}/s, {'hasta Ctrl+C' if not duracion else f'{duracion} s'}")
    try:
        while not duracion or t < duracion:
            n = tasa_objetivo(t, rate, rng)
            futuros = [pub.publish(path, _mensaje(rng.choice(filas)), fuente="thelook") for _ in range(n)]
            for f in futuros:
                f.result()
            total += n
            if t % 10 == 0:
                print(f"t={t:>4}s  este segundo={n:>4}  total={total}")
            t += 1
            time.sleep(max(0, inicio + t - time.time()))
    except KeyboardInterrupt:
        pass
    print(f"publicados {total} mensajes en {t} s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000, help="modo por lotes: cantidad de mensajes")
    ap.add_argument("--rate", type=int, default=100, help="mensajes por segundo (en continuo, ritmo medio)")
    ap.add_argument("--duracion", type=int, default=None,
                    help="modo continuo: segundos de tráfico (0 = hasta Ctrl+C)")
    ap.add_argument("--pool", type=int, default=5000, help="modo continuo: eventos de muestra")
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args()
    if a.duracion is None:
        por_lotes(a.n, a.rate)
    else:
        continuo(a.duracion, a.rate, a.pool, a.seed)
