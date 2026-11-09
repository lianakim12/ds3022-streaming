"""OpenSky -> Kafka topic "opensky_aircraft_states"

REST polling (PULL): periodically fetch the current aircraft states from the OpenSky API.

Run: python producer.py

Polls every 60 seconds.
"""
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "opensky_aircraft_states"
URL = "https://opensky-network.org/api/states/all"
EVERY = 60   # seconds: no account = 100 world-wide requests a day per IP

STATE_FIELDS = ["icao24", "callsign", "origin_country", "time_position",
                "last_contact", "longitude", "latitude", "baro_altitude",
                "on_ground", "velocity", "true_track", "vertical_rate",
                "sensors", "geo_altitude", "squawk", "spi", "position_source"]


def send(producer, message):
    """Part 2: one Kafka record per message, keyed by icao24."""
    unid = message["data"].get("icao24")
    if not unid:
        logging.warning("Skipping aircraft record without icao24")
        return None
    producer.produce(topic=TOPIC, key=unid, value=json.dumps(message))
    return unid


async def history(producer, days):
    """
    Original historical backfill function from the earthquake template.
    
    Not used for the OpenSky project.
    """
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    total = 0
    for back in range(days - 1, -1, -1):  # the last N days, today included
        start = today - timedelta(days=back)
        end = start + timedelta(days=1)
        r = requests.get(
            URL,
            params={
                "start": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "end": end.strftime("%Y-%m-%dT%H:%M:%S"),
                "format": "json",
                "limit": 20000,
                "orderby": "time-asc",
            },
            timeout=60,
        )
        if r.status_code == 204:  # no events that day
            continue
        r.raise_for_status()
        features = r.json()["features"]
        for feature in features:
            send(producer, {"action": "history", "data": feature})
        total += len(features)
        logging.info("%s  %4d events   total %d", start.date(), len(features), total)
        time.sleep(1)  # be polite to a free public service
    logging.info("History done: %d events sent to %s", total, TOPIC)


def live(producer):
    """
    Poll OpenSky periodically using REST requests.

    Each request returns a snapshot of currently tracked aircraft.
    """
    while True:
        try:
            r = requests.get(URL, timeout=60)
            if r.status_code == 429:  # no credits left for this IP address
                wait = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 3600))
                logging.warning("No credits left: waiting %d s", wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            body = r.json()
            for s in body.get("states") or []:
                state = dict(zip(STATE_FIELDS, s))   # a list -> a dict
                state["snapshot_time"] = body.get("time")
                send(producer, {"action": "poll", "data": state})
            logging.info("Snapshot %s sent", body.get("time"))
        except requests.RequestException as e:
            logging.warning("Request failed (%r): trying again", e)
        time.sleep(EVERY)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "live"
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        if mode == "history":
            asyncio.run(history(producer, int(sys.argv[2]) if len(sys.argv) > 2 else 7))
        else:
            live(producer)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        logging.info("OpenSky producer stopped.")
