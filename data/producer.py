"""
producer.py - Générateur d'événements de trajets Pedali

Deux modes :

  1) Fichier (historique pour les Parties 2 et 3) :
       python producer.py --mode file --n 50000
     -> écrit trips_history.jsonl (un événement JSON par ligne)

  2) Kafka (flux temps réel pour les Parties 4 et 5) :
       python producer.py --mode kafka --rate 20
     -> publie environ 20 événements par seconde dans le topic "trips"
       (clé du message = trip_id, valeur = événement JSON en UTF-8)
       Ajoutez --dry-run pour afficher les événements sans Kafka.
"""

import argparse
import heapq
import json
import math
import os
import random
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------
# Paramètres de la simulation
# --------------------------------------------------------------------------
VEHICLE_TYPES = ["BIKE", "EBIKE", "SCOOTER"]
VEHICLE_MIX = [0.30, 0.40, 0.30]
VEHICLE_PREFIX = {"BIKE": "BK", "EBIKE": "EB", "SCOOTER": "SC"}
FLEET_SIZE = {"BIKE": 900, "EBIKE": 1100, "SCOOTER": 1000}
SPEED_KMH = {"BIKE": 12.0, "EBIKE": 17.0, "SCOOTER": 16.0}
BATTERY_PER_KM = {"BIKE": 0.0, "EBIKE": 2.0, "SCOOTER": 3.5}
# tarif : prix de déverrouillage + prix par minute (MAD)
PRICING = {"BIKE": (2.00, 0.40), "EBIKE": (4.00, 0.70), "SCOOTER": (5.00, 0.90)}

CITY_SHARE = {"Casablanca": 0.38, "Rabat": 0.22, "Marrakech": 0.22, "Tanger": 0.18}
N_USERS = 40000          # U-10000 ... U-49999
N_COMMUTERS = 2500       # usagers réguliers domicile <-> travail

P_CANCEL = 0.06          # réservation annulée
P_FAULT = 0.025          # signalement de panne pendant un trajet
P_CHAIN = 0.10           # l'usager enchaîne un nouveau trajet (< 5 min après)
P_ROUND = 0.03           # balade : retour à la station de départ
P_UNKNOWN_END = 0.002    # station d'arrivée hors référentiel (arête orpheline)
P_LATE = 0.05            # événement en retard
MAX_LATE_S = 15 * 60     # retard maximal : 15 minutes
P_DUP = 0.02             # doublon

# Scénarios injectés (à retrouver par l'analyse de graphe, Partie 6)
DRAIN_STATION = "S-CAS-074"          # station qui se vide sans réapprovisionnement
TOUR_LOOP = ["S-MAR-002", "S-MAR-004", "S-MAR-005"]  # boucle touristique A -> B -> C -> A
P_TOUR = 0.05                        # part des trajets de Marrakech (9h-19h) qui démarrent une boucle
FAULTY_VEHICLE = "V-SC-0137"         # véhicule qui accumule les pannes
P_FAULTY_PICK = 0.03                 # part des trajets en trottinette à Casablanca qui le prennent
P_FAULTY_FAULT = 0.45

# Profil horaire du volume (mode file uniquement), heures de event_time
HOURLY_WEEKDAY = [0.20, 0.10, 0.08, 0.08, 0.10, 0.30, 0.90, 2.20, 2.60, 1.50, 1.00, 1.00,
                  1.20, 1.20, 1.00, 1.00, 1.40, 2.30, 2.50, 1.60, 1.10, 0.80, 0.50, 0.30]
HOURLY_WEEKEND = [0.30, 0.20, 0.10, 0.08, 0.08, 0.10, 0.20, 0.40, 0.70, 1.00, 1.40, 1.60,
                  1.70, 1.60, 1.60, 1.70, 1.80, 1.80, 1.60, 1.30, 1.00, 0.80, 0.60, 0.40]

BASE_WEIGHT = {"gare": 3.0, "centre_affaires": 2.0, "commercial": 1.8, "universite": 1.6,
               "tourisme": 1.5, "residentiel": 1.0, "plage": 1.0}
# multiplicateurs (départ, arrivée) par période et type de zone
PERIOD_MULT = {
    "matin": ({"residentiel": 2.5, "gare": 1.5, "centre_affaires": 0.5, "universite": 0.4},
              {"centre_affaires": 3.0, "universite": 2.5, "gare": 1.3, "residentiel": 0.35}),
    "soir": ({"centre_affaires": 3.0, "universite": 2.2, "gare": 1.3, "residentiel": 0.4},
             {"residentiel": 2.6, "gare": 1.5, "centre_affaires": 0.45, "universite": 0.4}),
    "journee": ({"tourisme": 1.6, "plage": 1.5, "commercial": 1.3},
                {"tourisme": 1.6, "plage": 1.5, "commercial": 1.3}),
    "weekend": ({"tourisme": 1.8, "plage": 2.5, "commercial": 1.6, "centre_affaires": 0.5},
                {"tourisme": 1.8, "plage": 2.5, "commercial": 1.6, "centre_affaires": 0.5}),
    "nuit": ({}, {}),
}


def period_of(dt):
    """Période de la journée (sur l'heure de event_time) ; le week-end est traité à part."""
    if dt.weekday() >= 5:
        return "weekend"
    h = dt.hour
    if 6 <= h < 10:
        return "matin"
    if 10 <= h < 16:
        return "journee"
    if 16 <= h < 20:
        return "soir"
    return "nuit"


def haversine_km(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (a["latitude"], a["longitude"],
                                                b["latitude"], b["longitude"]))
    h = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def iso(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def weighted_choice(rng, items, weights):
    total = sum(weights)
    if total <= 0:
        return rng.choice(items)
    r = rng.random() * total
    acc = 0.0
    for it, w in zip(items, weights):
        acc += w
        if r < acc:
            return it
    return items[-1]


# --------------------------------------------------------------------------
# Simulateur (événements discrets)
# --------------------------------------------------------------------------
class Simulator:
    def __init__(self, stations, rng, start_ts, trips_per_s, use_profile, trip_counter=1):
        self.rng = rng
        self.st = {s["station_id"]: s for s in stations}
        self.by_city = {}
        for s in stations:
            self.by_city.setdefault(s["city"], []).append(s)
        self.cities = list(CITY_SHARE)
        self.city_w = [CITY_SHARE[c] for c in self.cities]
        self.t = float(start_ts)
        self.trips_per_s = trips_per_s
        self.use_profile = use_profile
        self.lam_max = trips_per_s * (max(max(HOURLY_WEEKDAY), max(HOURLY_WEEKEND))
                                      if use_profile else 1.0)
        self.trip_no = trip_counter
        self.heap = []
        self.seq = 0
        self.stats = Counter()
        self._dist_cache = {}
        self._build_fleet()
        self._build_users()
        self._push(self.t + rng.expovariate(self.lam_max), "NEW", None)

    # ---- initialisation -------------------------------------------------
    def _build_fleet(self):
        rng = self.rng
        self.fleet = {c: {vt: [] for vt in VEHICLE_TYPES} for c in self.cities}
        self.busy_until = {}
        self.battery = {}
        for vt in VEHICLE_TYPES:
            for i in range(1, FLEET_SIZE[vt] + 1):
                vid = f"V-{VEHICLE_PREFIX[vt]}-{i:04d}"
                city = weighted_choice(rng, self.cities, self.city_w)
                if vid == FAULTY_VEHICLE:
                    city = "Casablanca"
                self.fleet[city][vt].append(vid)
                self.busy_until[vid] = 0.0
                self.battery[vid] = None if vt == "BIKE" else rng.randint(35, 100)

    def _build_users(self):
        rng = self.rng
        self.users = {c: [] for c in self.cities}
        for i in range(N_USERS):
            self.users[weighted_choice(rng, self.cities, self.city_w)].append(f"U-{10000 + i}")
        # usagers pendulaires : domicile (résidentiel) -> travail (centre d'affaires, université, gare)
        self.commuters = {c: [] for c in self.cities}
        for c in self.cities:
            homes = [s for s in self.by_city[c]
                     if s["zone_type"] == "residentiel" and s["station_id"] != DRAIN_STATION]
            works = [s for s in self.by_city[c] if s["zone_type"] in ("centre_affaires", "universite", "gare")]
            ww = [{"centre_affaires": 3.0, "universite": 1.5, "gare": 1.0}[s["zone_type"]] for s in works]
            n = int(N_COMMUTERS * CITY_SHARE[c])
            for u in rng.sample(self.users[c], n):
                self.commuters[c].append((u, rng.choice(homes)["station_id"],
                                          weighted_choice(rng, works, ww)["station_id"],
                                          rng.choices(VEHICLE_TYPES, VEHICLE_MIX)[0]))

    # ---- utilitaires ----------------------------------------------------
    def _push(self, ts, kind, payload):
        self.seq += 1
        heapq.heappush(self.heap, (ts, self.seq, kind, payload))

    def _dist(self, a, b):
        k = (a, b) if a < b else (b, a)
        d = self._dist_cache.get(k)
        if d is None:
            d = haversine_km(self.st[a], self.st[b])
            self._dist_cache[k] = d
        return d

    def _profile(self, dt):
        prof = HOURLY_WEEKEND if dt.weekday() >= 5 else HOURLY_WEEKDAY
        return prof[dt.hour]

    def _station_weight(self, s, period, role):
        mult = PERIOD_MULT[period][0 if role == "origin" else 1]
        w = BASE_WEIGHT[s["zone_type"]] * (s["capacity"] / 20.0) * mult.get(s["zone_type"], 1.0)
        if s["station_id"] == DRAIN_STATION:
            w = w * 6.0 if role == "origin" else 0.0
        return w

    def _pick_origin(self, city, period):
        cand = self.by_city[city]
        return weighted_choice(self.rng, cand, [self._station_weight(s, period, "origin") for s in cand])["station_id"]

    def _pick_dest(self, city, origin, period):
        if self.rng.random() < P_ROUND and origin != DRAIN_STATION:
            return origin
        cand = [s for s in self.by_city[city] if s["station_id"] != origin]
        w = [self._station_weight(s, period, "dest") * math.exp(-self._dist(origin, s["station_id"]) / 3.0)
             for s in cand]
        return weighted_choice(self.rng, cand, w)["station_id"]

    def _pick_vehicle(self, city, vt, t):
        rng = self.rng
        if city == "Casablanca" and vt == "SCOOTER" and rng.random() < P_FAULTY_PICK \
                and self.busy_until[FAULTY_VEHICLE] <= t:
            return FAULTY_VEHICLE
        pool = self.fleet[city][vt]
        for _ in range(20):
            v = rng.choice(pool)
            if self.busy_until[v] <= t and v != FAULTY_VEHICLE:
                return v
        return rng.choice(pool)

    # ---- création d'un trajet -------------------------------------------
    def _new_trip(self, t):
        rng = self.rng
        dt = datetime.fromtimestamp(t, tz=timezone.utc)
        period = period_of(dt)
        city = weighted_choice(rng, self.cities, self.city_w)
        # boucle touristique à Marrakech
        if city == "Marrakech" and 9 <= dt.hour < 19 and rng.random() < P_TOUR:
            a, b, c = TOUR_LOOP
            user = rng.choice(self.users[city])
            vt = rng.choice(["BIKE", "EBIKE"])
            self.stats["tours"] += 1
            self._trip(t, city, user, vt, a, b, kind="tour", legs=[c, a])
            return
        # trajet pendulaire domicile -> travail (matin) ou travail -> domicile (soir)
        if period in ("matin", "soir") and rng.random() < 0.45:
            user, home, work, vt = rng.choice(self.commuters[city])
            o, d = (home, work) if period == "matin" else (work, home)
            self.stats["commute"] += 1
            self._trip(t, city, user, vt, o, d, kind="commute")
            return
        origin = self._pick_origin(city, period)
        dest = self._pick_dest(city, origin, period)
        user = rng.choice(self.users[city])
        vt = rng.choices(VEHICLE_TYPES, VEHICLE_MIX)[0]
        self._trip(t, city, user, vt, origin, dest, kind="random")

    def _trip(self, t, city, user, vt, origin, dest, kind, legs=None):
        rng = self.rng
        trip_id = f"T-{self.trip_no:08d}"
        self.trip_no += 1
        self.stats["trips"] += 1
        vid = self._pick_vehicle(city, vt, t)
        if dest is None:
            dest = self._pick_dest(city, origin, period_of(datetime.fromtimestamp(t, tz=timezone.utc)))

        # batterie : remplacée si trop faible
        bat0 = self.battery[vid]
        if bat0 is not None and bat0 < 20 and rng.random() < 0.8:
            bat0 = rng.randint(85, 100)

        base = {"trip_id": trip_id, "user_id": user, "vehicle_id": vid, "vehicle_type": vt,
                "start_station": origin, "end_station": None, "city": city}

        def ev(status, ts, battery, **extra):
            e = dict(base)
            e.update({"status": status, "duration_s": None, "distance_km": None,
                      "battery_pct": battery, "price_mad": None})
            e.update(extra)
            e["event_time"] = iso(ts)
            self._emit(ts, e)

        ev("RESERVED", t, bat0)
        p_cancel = P_CANCEL if kind == "random" else 0.02
        if rng.random() < p_cancel:
            ev("CANCELLED", t + rng.uniform(60, 900), bat0)
            self.busy_until[vid] = max(self.busy_until[vid], t + 900)
            return
        t_unlock = t + rng.uniform(15, 420)
        ev("UNLOCKED", t_unlock, bat0)

        # distance et durée
        if dest == origin:  # balade circulaire
            dist = rng.uniform(1.5, 8.0)
            speed = SPEED_KMH[vt] * rng.uniform(0.5, 0.8)
        else:
            dist = max(0.4, self._dist(origin, dest) * rng.uniform(1.20, 1.45))
            speed = SPEED_KMH[vt] * rng.uniform(0.75, 1.15)
            if kind == "tour":
                speed *= 0.7
        duration = dist / speed * 3600.0 + rng.uniform(20, 90)
        if kind == "random" and rng.random() < 0.08:  # arrêts en route
            duration *= rng.uniform(1.5, 3.0)
        duration = int(duration)
        t_end = t_unlock + duration

        bat_end = None
        if bat0 is not None:
            bat_end = max(3, int(round(bat0 - BATTERY_PER_KM[vt] * dist * rng.uniform(0.9, 1.1))))

        p_fault = P_FAULTY_FAULT if vid == FAULTY_VEHICLE else P_FAULT
        if rng.random() < p_fault:
            frac = rng.uniform(0.1, 0.9)
            bat_mid = None if bat0 is None else int(round(bat0 - (bat0 - bat_end) * frac))
            ev("FAULT_REPORTED", t_unlock + frac * duration, bat_mid)
            self.stats["faults"] += 1
            if vid == FAULTY_VEHICLE:
                self.stats["faulty_vehicle_faults"] += 1

        end_station = dest
        if rng.random() < P_UNKNOWN_END:
            end_station = f"S-{origin[2:5]}-{rng.randint(900, 999)}"
        _, per_min = PRICING[vt]
        price = round(PRICING[vt][0] + per_min * duration / 60.0, 2)
        ev("ENDED", t_end, bat_end, end_station=end_station, duration_s=duration,
           distance_km=round(dist, 1), price_mad=price)
        self.busy_until[vid] = t_end
        self.battery[vid] = bat_end

        # suite du parcours : boucle touristique ou enchaînement spontané
        if legs:
            self._push(t_end + rng.uniform(45, 240), "START",
                       (city, user, vt, dest, legs[0], kind, legs[1:]))
        elif kind != "tour" and dest != DRAIN_STATION and rng.random() < P_CHAIN:
            self.stats["chained"] += 1
            self._push(t_end + rng.uniform(30, 270), "START",
                       (city, user, rng.choices(VEHICLE_TYPES, VEHICLE_MIX)[0], dest, None, "random", None))

    def _emit(self, ts, event):
        rng = self.rng
        arrival = ts
        if rng.random() < P_LATE:
            arrival = ts + rng.uniform(30, MAX_LATE_S)
            self.stats["late"] += 1
        self._push(arrival, "EMIT", event)
        if rng.random() < P_DUP:
            self._push(arrival + rng.uniform(1, 90), "EMIT", event)
            self.stats["dup"] += 1

    # ---- boucle principale ----------------------------------------------
    def events(self):
        """Génère indéfiniment des couples (instant_d_arrivée, événement), dans l'ordre d'arrivée."""
        rng = self.rng
        while True:
            ts, _, kind, payload = heapq.heappop(self.heap)
            self.t = ts
            if kind == "EMIT":
                yield ts, payload
            elif kind == "NEW":
                accept = True
                if self.use_profile:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    accept = rng.random() < self.trips_per_s * self._profile(dt) / self.lam_max
                if accept:
                    self._new_trip(ts)
                self._push(ts + rng.expovariate(self.lam_max), "NEW", None)
            elif kind == "START":
                city, user, vt, origin, dest, k, legs = payload
                self._trip(ts, city, user, vt, origin, dest, kind=k, legs=legs)


# Nombre moyen d'événements émis par trajet créé (sert à caler le débit)
def events_per_trip():
    p_cancel = 0.055
    per_trip = 1 + p_cancel + (1 - p_cancel) * (2 + P_FAULT + 0.002)
    trips_per_new = 1.0 / (1 - P_CHAIN * 0.95)
    return per_trip * (1 + P_DUP) * trips_per_new


def load_stations(path):
    with open(path, encoding="utf-8") as f:
        txt = f.read().strip()
    if txt.startswith("["):
        return json.loads(txt)
    return [json.loads(line) for line in txt.splitlines() if line.strip()]


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------
def run_file(args, stations, rng):
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    avg_profile = (5 * sum(HOURLY_WEEKDAY) + 2 * sum(HOURLY_WEEKEND)) / (7 * 24)
    trips_per_s = args.n / events_per_trip() / (args.days * 86400 * avg_profile)
    sim = Simulator(stations, rng, start.timestamp(), trips_per_s, use_profile=True)
    status = Counter()
    first = last = None
    with open(args.output, "w", encoding="utf-8") as f:
        for i, (arrival, ev) in enumerate(sim.events()):
            if i >= args.n:
                break
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            status[ev["status"]] += 1
            first = first or ev["event_time"]
            last = ev["event_time"]
    print(f"{args.n} événements écrits dans {args.output}", file=sys.stderr)
    print(f"  période couverte : {first} -> {last}", file=sys.stderr)
    print(f"  statuts : {dict(status)}", file=sys.stderr)
    print(f"  trajets : {sim.stats['trips']}  (en retard : {sim.stats['late']}, doublons : {sim.stats['dup']})",
          file=sys.stderr)


class KafkaSink:
    def __init__(self, bootstrap, topic):
        self.topic = topic
        try:
            from kafka import KafkaProducer  # kafka-python
            self.kind = "kafka-python"
            try:
                self.p = KafkaProducer(bootstrap_servers=bootstrap.split(","), acks="all", linger_ms=20)
            except Exception as exc:  # NoBrokersAvailable, etc.
                sys.exit(f"Erreur : impossible de joindre Kafka sur {bootstrap} ({exc}). "
                         "Vérifiez --bootstrap (localhost:9092 depuis la machine hôte, "
                         "kafka:9092 depuis un conteneur du même réseau Docker).")
        except ImportError:
            try:
                from confluent_kafka import Producer  # confluent-kafka
            except ImportError:
                sys.exit("Erreur : installez kafka-python (pip install kafka-python) "
                         "ou confluent-kafka, ou utilisez --dry-run.")
            self.kind = "confluent-kafka"
            self.p = Producer({"bootstrap.servers": bootstrap, "acks": "all", "linger.ms": 20})

    def send(self, key, value):
        if self.kind == "kafka-python":
            self.p.send(self.topic, key=key.encode("utf-8"), value=value.encode("utf-8"))
        else:
            while True:
                try:
                    self.p.produce(self.topic, key=key.encode("utf-8"), value=value.encode("utf-8"))
                    break
                except BufferError:
                    self.p.poll(0.5)
            self.p.poll(0)

    def close(self):
        self.p.flush()
        if self.kind == "kafka-python":
            self.p.close()


class StdoutSink:
    def send(self, key, value):
        sys.stdout.write(value + "\n")
        sys.stdout.flush()

    def close(self):
        pass


def run_kafka(args, stations, rng):
    sink = StdoutSink() if args.dry_run else KafkaSink(args.bootstrap, args.topic)
    t0_wall = time.time()
    t0_sim = t0_wall
    trips_per_s = args.rate / events_per_trip() / args.speed
    # trip_id dérivés de l'heure de lancement : deux lancements successifs ne réutilisent pas les mêmes
    counter = int(t0_wall - 1_700_000_000) * 100
    # Préchauffage : la simulation démarre une heure plus tôt et les événements antérieurs au
    # lancement ne sont pas envoyés. Le flux démarre ainsi « en régime établi » (trajets en cours,
    # fins de trajet dès la première seconde) au lieu de ne contenir que des réservations.
    warmup = 3600.0
    sim = Simulator(stations, rng, t0_sim - warmup, trips_per_s, use_profile=False, trip_counter=counter)
    target = "stdout (dry-run)" if args.dry_run else f"{args.bootstrap} / topic {args.topic}"
    print(f"Envoi vers {target} : ~{args.rate} événements/s (Ctrl+C pour arrêter)", file=sys.stderr)

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("flag", True))
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("flag", True))

    sent = 0
    status = Counter()
    last_log, last_sent = time.time(), 0
    for arrival, ev in sim.events():
        if stop["flag"]:
            break
        if arrival < t0_sim:  # préchauffage
            continue
        wall_due =t0_wall + (arrival - t0_sim) / args.speed
        delay = wall_due - time.time()
        while delay > 0 and not stop["flag"]:
            time.sleep(min(delay, 0.5))
            delay = wall_due - time.time()
        if stop["flag"]:
            break
        sink.send(ev["trip_id"], json.dumps(ev, ensure_ascii=False))
        sent += 1
        status[ev["status"]] += 1
        now = time.time()
        if now - last_log >= 10:
            print(f"[{iso(now)}] {sent} événements envoyés ({(sent - last_sent) / (now - last_log):.1f}/s)",
                  file=sys.stderr)
            last_log, last_sent = now, sent
        if args.max_events and sent >= args.max_events:
            break
        if args.duration and now - t0_wall >= args.duration:
            break
    sink.close()
    elapsed = max(time.time() - t0_wall, 1e-9)
    print(f"Terminé : {sent} événements en {elapsed:.0f} s ({sent / elapsed:.1f}/s), statuts : {dict(status)}",
          file=sys.stderr)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Générateur d'événements de trajets Pedali.ma")
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--stations", default=os.path.join(here, "stations.json"),
                    help="référentiel des stations (défaut : stations.json à côté du script)")
    ap.add_argument("--seed", type=int, default=42, help="graine aléatoire (défaut : 42)")
    g = ap.add_argument_group("mode file")
    g.add_argument("--n", type=int, default=50000, help="nombre d'événements (défaut : 50000)")
    g.add_argument("--output", default="trips_history.jsonl", help="fichier de sortie")
    g.add_argument("--start", default="2026-11-09", help="premier jour simulé, AAAA-MM-JJ (défaut : 2026-11-09)")
    g.add_argument("--days", type=float, default=7, help="nombre de jours simulés (défaut : 7)")
    g = ap.add_argument_group("mode kafka")
    g.add_argument("--rate", type=float, default=20, help="événements par seconde (défaut : 20)")
    g.add_argument("--bootstrap", default="localhost:9092",
                   help="serveur(s) Kafka (défaut : localhost:9092 ; kafka:9092 depuis un conteneur)")
    g.add_argument("--topic", default="trips", help="topic Kafka (défaut : trips)")
    g.add_argument("--speed", type=float, default=1.0,
                   help="accélération du temps simulé (défaut : 1 = temps réel)")
    g.add_argument("--max-events", type=int, default=0, help="arrêt après N événements (0 = sans fin)")
    g.add_argument("--duration", type=float, default=0, help="arrêt après N secondes (0 = sans fin)")
    g.add_argument("--dry-run", action="store_true", help="affiche les événements au lieu de les envoyer")
    args = ap.parse_args()

    stations = load_stations(args.stations)
    rng = random.Random(args.seed)
    if args.mode == "file":
        run_file(args, stations, rng)
    else:
        run_kafka(args, stations, rng)


if __name__ == "__main__":
    main()
