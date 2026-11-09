# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name: Liana Kim**
**Date: 5 Nov 2026**

## 1. The source
- Source and URL: https://opensky-network.org/api/states/all
- Method (WebSocket push, REST polling or REST history pull): REST polling (pull)
- Account, key or User-Agent needed: None required
- Limits I know about (rate limits, paging, reconnects): 400 API credits per day, shared by IP address. Since a worldwide /states/all request costs 4 credits, we can make approximately 100 requests per day.

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
  "icao24": "39de4e",
  "callsign": "TVF6411 ",
  "origin_country": "France",
  "time_position": 1794230125,
  "last_contact": 1794230126,
  "longitude": 3.4897,
  "latitude": 48.0164,
  "baro_altitude": 5623.56,
  "on_ground": false,
  "velocity": 160.07,
  "true_track": 314.22,
  "vertical_rate": -9.43,
  "sensors": null,
  "geo_altitude": 5775.96,
  "squawk": "1000",
  "spi": false,
  "position_source": 0,
  "snapshot_time": 1794230126
}
```

## 3. Volume maths
- Measured rate (from the probe): 9,610 per request
- Live capture needed for 50,000: 5.2 requests (6 in practice)
- History / backfill available? How many days? Not available through the anonymous /states/all endpoint
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): I will make 6 requests across 60-second intervals to complete polling for 50,000+ records. I will retain the raw responses and deduplicate observations using icao24 + time_position during processing.

## 4. Topic design
- Topic name: opensky_aircraft_states (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: icao24
- Why this key (ordering, spread across partitions): icao24 is the unique ICAO transponder identifier assigned to an aircraft. It allows us to group all observations belonging to the same aircraft together.

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) Two records are considered the same Kafka record if they have the same topic, partition and offset. The partition identifies which section of the topic contains the record, while the offset identifies its position within that partition.
- What makes two records **the same thing** (the same trade, post, aircraft position)? For Opensky, having the same icao24 and time_position would signal two records being the same real-world observation.
- If the same thing can arrive in several versions, which one do I keep? Only keep one record of a unique icao24 + time_position and drop the rest of the duplicates.

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: How does the number of aircraft observed in the sky change over time, and when is air traffic busiest?

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
