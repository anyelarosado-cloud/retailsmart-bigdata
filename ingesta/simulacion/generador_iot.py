"""Generador de eventos IoT simulados para RetailSmart.

Simula dos flujos que no existen en los datasets públicos:
  * sensores de cámaras de frío (temperatura, compresor, puerta), con fallas inyectadas;
  * GPS de repartidores que salen de cada centro de distribución.

Todo es reproducible: con la misma --seed se obtienen los mismos mensajes y las mismas
fallas. La verdad de terreno (qué falla se inyectó y cuándo) NO viaja en los mensajes:
se guarda aparte con --verdad, para medir después cuántas fallas detecta el pipeline.

Uso (publicar a Pub/Sub, 100 mensajes por segundo aproximadamente):
    python generador_iot.py --ticks 600 --verdad verdad_fallas.json

Uso sin conexión (pruebas):
    python generador_iot.py --dry-run --ticks 120 --salida eventos.jsonl
"""
import argparse
import json
import math
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

PROJECT = "retailsmart-bd"
SETPOINT_C = 4.0           # temperatura objetivo de la cámara
UMBRAL_ALERTA_C = 8.0      # umbral que usará el pipeline de alertas
RANGO_FISICO_C = (-30.0, 60.0)
TIPOS_FALLA = ("deriva", "mudo", "invalida")

# Respaldo para pruebas sin conexión. En ejecución real se leen de BigQuery
# (retailsmart_raw.distribution_centers). Coordenadas aproximadas.
CENTROS_APROX = [
    (1, 35.1174, -89.9711), (2, 41.8369, -87.6847), (3, 29.7604, -95.3698),
    (4, 34.0500, -118.2500), (5, 29.9500, -90.0667), (6, 40.6340, -73.7834),
    (7, 39.9500, -75.1667), (8, 30.6944, -88.0431), (9, 32.7833, -79.9333),
    (10, 32.0167, -81.1167),
]


def cargar_centros_bq(project=PROJECT):
    from google.cloud import bigquery
    sql = f"SELECT id, latitude, longitude FROM `{project}.retailsmart_raw.distribution_centers` ORDER BY id"
    return [(r.id, r.latitude, r.longitude) for r in bigquery.Client(project=project).query(sql).result()]


class Simulador:
    def __init__(self, centros, camaras_por_centro=4, vehiculos_por_centro=6, seed=42,
                 p_falla=0.15, horizonte_ticks=900, duracion_falla=(60, 180),
                 tipos_falla=TIPOS_FALLA, p_duplicado=0.0, p_invalido=0.0,
                 seg_por_tick_gps=30):
        self.rng = random.Random(seed)
        self.p_duplicado, self.p_invalido = p_duplicado, p_invalido
        self.seg_por_tick_gps = seg_por_tick_gps
        self.stats = Counter()
        self.verdad = []
        self.camaras, self.vehiculos = [], []
        for centro_id, lat, lon in centros:
            for k in range(camaras_por_centro):
                self.camaras.append({"camara_id": f"C{centro_id:02d}-{k + 1:02d}", "centro_id": centro_id,
                                     "temp": SETPOINT_C + self.rng.uniform(-0.3, 0.3), "comp": False})
            for k in range(vehiculos_por_centro):
                self.vehiculos.append({"vehiculo_id": f"V{centro_id:02d}-{k + 1:02d}", "centro_id": centro_id,
                                       "lat0": lat, "lon0": lon, "lat": lat, "lon": lon,
                                       "rumbo": self.rng.uniform(0, 360), "vel": self.rng.uniform(25, 55)})
        # Fallas programadas por tick: {camara_id: dict(tipo, ini, fin)}
        self.fallas = {}
        for cam in self.camaras:
            if self.rng.random() < p_falla:
                dur = self.rng.randint(*duracion_falla)
                ini = self.rng.randint(30, max(31, horizonte_ticks - dur))
                self.fallas[cam["camara_id"]] = {"tipo": self.rng.choice(list(tipos_falla)), "ini": ini, "fin": ini + dur}
        self._verdad_idx = {}

    # ---- sensores ----
    def _falla_activa(self, camara_id, n):
        f = self.fallas.get(camara_id)
        return f if f and f["ini"] <= n < f["fin"] else None

    def _registrar_verdad(self, cam, n, ts):
        f = self.fallas.get(cam["camara_id"])
        if not f:
            return
        if n == f["ini"]:
            self.verdad.append({"camara_id": cam["camara_id"], "centro_id": cam["centro_id"],
                                "tipo_falla": f["tipo"], "inicio_ts": ts.isoformat(), "fin_ts": None})
            self._verdad_idx[cam["camara_id"]] = len(self.verdad) - 1
        elif n == f["fin"] and cam["camara_id"] in self._verdad_idx:
            self.verdad[self._verdad_idx[cam["camara_id"]]]["fin_ts"] = ts.isoformat()

    def _sensor(self, cam, n, ts):
        self._registrar_verdad(cam, n, ts)
        f = self._falla_activa(cam["camara_id"], n)
        puerta = False
        if f and f["tipo"] == "deriva":
            cam["comp"] = False
            cam["temp"] = min(cam["temp"] + 0.08, 14.0)
        else:
            puerta = self.rng.random() < 0.03
            cam["temp"] += -0.15 if cam["comp"] else 0.05
            cam["temp"] += 0.4 if puerta else 0.0
            cam["temp"] += self.rng.gauss(0, 0.05)
            if cam["temp"] > SETPOINT_C + 0.6:
                cam["comp"] = True
            elif cam["temp"] < SETPOINT_C - 0.6:
                cam["comp"] = False
        if f and f["tipo"] == "mudo":
            self.stats["sensor_mudo_omitidos"] += 1
            return None
        temp = round(cam["temp"], 2)
        if f and f["tipo"] == "invalida":
            temp = -999.0
            self.stats["lecturas_fuera_de_rango"] += 1
        return {"evento_id": f"sensor-{cam['camara_id']}-{n}", "tipo": "sensor", "camara_id": cam["camara_id"],
                "centro_id": cam["centro_id"], "temp_c": temp, "compresor_on": bool(cam["comp"]),
                "puerta_abierta": puerta, "event_ts": ts.isoformat()}

    # ---- GPS ----
    def _gps(self, v, n, ts):
        dist_km = v["vel"] * self.seg_por_tick_gps / 3600.0
        v["rumbo"] = (v["rumbo"] + self.rng.gauss(0, 12)) % 360
        v["vel"] = min(70.0, max(15.0, v["vel"] + self.rng.gauss(0, 2)))
        lat_r = math.radians(v["lat"])
        v["lat"] += dist_km * math.cos(math.radians(v["rumbo"])) / 111.0
        v["lon"] += dist_km * math.sin(math.radians(v["rumbo"])) / (111.0 * max(0.2, math.cos(lat_r)))
        # si se aleja más de ~40 km del centro, regresa hacia él
        d_lat, d_lon = (v["lat"] - v["lat0"]) * 111.0, (v["lon"] - v["lon0"]) * 111.0 * math.cos(lat_r)
        if math.hypot(d_lat, d_lon) > 40:
            v["rumbo"] = (math.degrees(math.atan2(-d_lon, -d_lat))) % 360
        return {"evento_id": f"gps-{v['vehiculo_id']}-{n}", "tipo": "gps", "vehiculo_id": v["vehiculo_id"],
                "centro_id": v["centro_id"], "lat": round(v["lat"], 5), "lon": round(v["lon"], 5),
                "velocidad_kmh": round(v["vel"], 1), "event_ts": ts.isoformat()}

    # ---- un tick: una lectura por sensor y una posición por vehículo ----
    def tick(self, n, ts):
        """Devuelve una lista de (tipo, bytes). Puede incluir duplicados y mensajes inválidos."""
        salida = []
        crudos = []
        for cam in self.camaras:
            m = self._sensor(cam, n, ts)
            if m:
                crudos.append(m)
        for v in self.vehiculos:
            crudos.append(self._gps(v, n, ts))
        for m in crudos:
            tipo = m["tipo"]
            if self.rng.random() < self.p_invalido:
                self.stats["invalidos"] += 1
                variante = self.rng.choice(["no_json", "sin_evento_id", "tipo_incorrecto"])
                if variante == "no_json":
                    salida.append((tipo, b"{esto no es json"))
                elif variante == "sin_evento_id":
                    sin = {k: val for k, val in m.items() if k != "evento_id"}
                    salida.append((tipo, json.dumps(sin).encode("utf-8")))
                else:
                    mal = dict(m)
                    mal["temp_c" if tipo == "sensor" else "lat"] = "NaN"
                    salida.append((tipo, json.dumps(mal).encode("utf-8")))
                continue
            payload = json.dumps(m).encode("utf-8")
            salida.append((tipo, payload))
            self.stats["validos_" + tipo] += 1
            if self.rng.random() < self.p_duplicado:
                self.stats["duplicados"] += 1
                salida.append((tipo, payload))
        return salida


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", default=PROJECT)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--ticks", type=int, default=600, help="número de rondas de lecturas")
    ap.add_argument("--intervalo", type=float, default=1.0, help="segundos reales entre rondas")
    ap.add_argument("--camaras-por-centro", type=int, default=4)
    ap.add_argument("--vehiculos-por-centro", type=int, default=6)
    ap.add_argument("--p-falla", type=float, default=0.15, help="probabilidad de que una cámara tenga una falla")
    ap.add_argument("--p-duplicado", type=float, default=0.0)
    ap.add_argument("--p-invalido", type=float, default=0.0)
    ap.add_argument("--topic-sensores", default="sensores")
    ap.add_argument("--topic-gps", default="gps")
    ap.add_argument("--centros-aprox", action="store_true", help="usar centros embebidos en vez de BigQuery")
    ap.add_argument("--dry-run", action="store_true", help="no publica; escribe JSONL")
    ap.add_argument("--salida", default="eventos_simulados.jsonl")
    ap.add_argument("--verdad", default="verdad_fallas.json")
    a = ap.parse_args()

    centros = CENTROS_APROX if (a.dry_run or a.centros_aprox) else cargar_centros_bq(a.project)
    sim = Simulador(centros, a.camaras_por_centro, a.vehiculos_por_centro, a.seed, a.p_falla,
                    horizonte_ticks=a.ticks, p_duplicado=a.p_duplicado, p_invalido=a.p_invalido)
    futuros = []
    if a.dry_run:
        base = datetime(2026, 11, 5, tzinfo=timezone.utc)
        with open(a.salida, "w", encoding="utf-8") as fh:
            for n in range(a.ticks):
                for _, payload in sim.tick(n, base + timedelta(seconds=n * a.intervalo)):
                    fh.write(payload.decode("utf-8") + "\n")
    else:
        from google.cloud import pubsub_v1
        pub = pubsub_v1.PublisherClient()
        rutas = {"sensor": pub.topic_path(a.project, a.topic_sensores), "gps": pub.topic_path(a.project, a.topic_gps)}
        t0 = time.monotonic()
        for n in range(a.ticks):
            for tipo, payload in sim.tick(n, datetime.now(timezone.utc)):
                futuros.append(pub.publish(rutas[tipo], payload, fuente="simulado", tipo=tipo))
            espera = t0 + (n + 1) * a.intervalo - time.monotonic()
            if espera > 0:
                time.sleep(espera)
        for f in futuros:
            f.result()
    with open(a.verdad, "w", encoding="utf-8") as fh:
        json.dump({"seed": a.seed, "umbral_alerta_c": UMBRAL_ALERTA_C, "fallas": sim.verdad}, fh, indent=2)
    print("estadísticas:", dict(sim.stats))
    print(f"fallas inyectadas: {len(sim.verdad)} (verdad de terreno en {a.verdad})")


if __name__ == "__main__":
    main()
