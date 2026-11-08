"""Mide cuántas fallas inyectadas detectó el pipeline de alertas de frío.

Compara verdad_fallas.json (lo que generó el simulador) con la tabla rt_alertas_frio.
Una falla cuenta como detectada si hay una alerta de la regla esperada para la misma cámara cuya
ventana se superpone con la falla (o termina hasta 120 s después de ella):
  deriva de temperatura  -> sobretemperatura
  sensor mudo            -> sin_datos
  lectura inválida       -> sin_datos
Una alerta es "sin falla correspondiente" si no coincide con ninguna falla de su cámara.

    python3 medir_deteccion.py --verdad ../ingesta/simulacion/verdad_fallas.json
"""
import argparse
import json
from datetime import datetime, timedelta

ESPERADA = {"deriva": "sobretemperatura", "mudo": "sin_datos", "invalida": "sin_datos"}


def _t(x):
    return x if isinstance(x, datetime) else datetime.fromisoformat(x)


def evaluar(fallas, alertas, tolerancia_s=120):
    tol = timedelta(seconds=tolerancia_s)
    fallas = [{**f, "inicio": _t(f["inicio_ts"]), "fin": _t(f["fin_ts"])} for f in fallas]
    alertas = [{**a, "ventana_inicio": _t(a["ventana_inicio"]), "ventana_fin": _t(a["ventana_fin"])} for a in alertas]

    def solapa(a, f):
        return a["camara_id"] == f["camara_id"] and a["ventana_fin"] > f["inicio"] and a["ventana_inicio"] < f["fin"] + tol

    detalle = []
    for f in fallas:
        cand = [a for a in alertas if a["regla"] == ESPERADA[f["tipo_falla"]] and solapa(a, f)]
        primera = min(cand, key=lambda a: a["ventana_fin"]) if cand else None
        detalle.append({"camara_id": f["camara_id"], "tipo_falla": f["tipo_falla"], "detectada": bool(cand),
                        "segundos_hasta_alerta": (primera["ventana_fin"] - f["inicio"]).total_seconds() if primera else None})
    sin_falla = [a for a in alertas if not any(solapa(a, f) for f in fallas)]
    tiempos = [d["segundos_hasta_alerta"] for d in detalle if d["detectada"]]
    por_tipo = {}
    for d in detalle:
        t = por_tipo.setdefault(d["tipo_falla"], {"inyectadas": 0, "detectadas": 0})
        t["inyectadas"] += 1
        t["detectadas"] += int(d["detectada"])
    return {"fallas_inyectadas": len(fallas), "fallas_detectadas": sum(d["detectada"] for d in detalle),
            "alertas_totales": len(alertas), "alertas_sin_falla": len(sin_falla),
            "tiempo_medio_deteccion_s": round(sum(tiempos) / len(tiempos), 1) if tiempos else None,
            "por_tipo": por_tipo, "detalle": detalle}


def leer_alertas(project, dataset, desde, hasta):
    from google.cloud import bigquery
    sql = f"""SELECT camara_id, regla, ventana_inicio, ventana_fin
              FROM `{project}.{dataset}.rt_alertas_frio`
              WHERE ventana_fin > @desde AND ventana_inicio < @hasta"""
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("desde", "TIMESTAMP", desde),
                                                    bigquery.ScalarQueryParameter("hasta", "TIMESTAMP", hasta)])
    return [dict(r) for r in bigquery.Client(project=project).query(sql, job_config=cfg).result()]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--verdad", default="verdad_fallas.json")
    ap.add_argument("--project", default="retailsmart-bd")
    ap.add_argument("--dataset", default="retailsmart_smart")
    ap.add_argument("--salida", default="deteccion.json")
    a = ap.parse_args()
    fallas = json.load(open(a.verdad, encoding="utf-8"))["fallas"]
    ini = min(_t(f["inicio_ts"]) for f in fallas) - timedelta(minutes=2)
    fin = max(_t(f["fin_ts"]) for f in fallas) + timedelta(minutes=5)
    r = evaluar(fallas, leer_alertas(a.project, a.dataset, ini, fin))
    for k in ("fallas_inyectadas", "fallas_detectadas", "alertas_totales", "alertas_sin_falla", "tiempo_medio_deteccion_s"):
        print(f"{k}: {r[k]}")
    print("por tipo:", r["por_tipo"])
    for d in r["detalle"]:
        print(f"  {d['camara_id']:7s} {d['tipo_falla']:9s} detectada={d['detectada']} en {d['segundos_hasta_alerta']} s")
    json.dump(r, open(a.salida, "w", encoding="utf-8"), indent=2, default=str)


if __name__ == "__main__":
    main()
