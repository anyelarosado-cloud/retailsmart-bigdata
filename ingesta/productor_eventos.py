import argparse, json, time
from datetime import datetime, timezone
from google.cloud import bigquery, pubsub_v1

PROJECT, TOPIC = "retailsmart-bd", "clicks"
ALIAS = {"Brasil": "Brazil", "España": "Spain"}

def main(n, rate):
    bq = bigquery.Client(project=PROJECT)
    sql = f"""
    SELECT e.id, e.user_id, e.session_id, e.event_type, e.uri, e.browser,
           e.traffic_source, u.country AS pais, u.state AS estado
    FROM `{PROJECT}.retailsmart_raw.events` e
    JOIN `{PROJECT}.retailsmart_raw.users` u ON u.id = e.user_id
    ORDER BY RAND() LIMIT {n}"""
    pub = pubsub_v1.PublisherClient()
    path = pub.topic_path(PROJECT, TOPIC)
    futuros = []
    for i, r in enumerate(bq.query(sql).result(), start=1):
        msg = {
            "event_id": r.id, "user_id": r.user_id, "session_id": r.session_id,
            "event_type": r.event_type, "uri": r.uri, "browser": r.browser,
            "traffic_source": r.traffic_source,
            "pais": ALIAS.get(r.pais, r.pais), "estado": r.estado,
            "event_ts": datetime.now(timezone.utc).isoformat(),
        }
        futuros.append(pub.publish(path, json.dumps(msg).encode("utf-8"), fuente="thelook"))
        if rate and i % rate == 0:
            time.sleep(1)
    for f in futuros:
        f.result()
    print(f"publicados {len(futuros)} mensajes")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--rate", type=int, default=100, help="mensajes por segundo")
    a = ap.parse_args()
    main(a.n, a.rate)
