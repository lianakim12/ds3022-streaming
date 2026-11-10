# TODO: your project title

**Author:** TODO
**DS 3022 Data Project 3, Fall 2026**

> There is no interview for this project: this README is where you explain and defend your pipeline. Fill in the title, your name and every italic placeholder, then delete this note. Keep the headings: `kit/check_project.py` looks for them.

## Data source

_(replace: what source, which URL or stream, pull or push, what one message contains. Mention any account, key or User-Agent needed. If you replay an archive rather than stream live, say so.)_

## How to run

_(replace: the exact commands, in order, from a fresh clone. A classmate will follow them in Session 4 (Mon 16 Nov) without asking you. Include: virtual environment and `pip install`, starting the cluster, creating the topic, running the producer and lander, running the flow on the full data and on the sample:)_

```bash
cd project3
python flow.py data/sample      # runs without Kafka, on the committed sample
```

## Architecture

_(replace: a diagram (an image in docs/ or a text diagram) of producer → topic → lander → raw Parquet → flow tasks → tables → charts, with the names of your files, topic, tables and charts.)_

## Design decisions

Answer each in 2-4 sentences, in your own words, with your own numbers.

1. **Key and partitions.** _(replace: which key, why, and what it guarantees about order. What would go wrong with another key?)_
2. **Batch size and time.** _(replace: your batch size and time limit. With your source's rate, which rule closes most batches? How many files per hour does that make?)_
3. **Offsets after the file.** _(replace: what happens if the lander crashes after writing a file but before storing offsets, and why you accept that.)_
4. **Duplicates.** _(replace: what makes two records the same Kafka record, and what makes them the same thing. Which SQL removes each kind?)_
5. **Windows and time.** _(replace: which time column your analysis uses (event time or landing time), which window size, and why.)_
6. **Ten times the volume.** _(replace: what in your pipeline would break or slow down first at 10x your rate, and what you would change.)_

## Chaos round

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D1 Crash between file and offsets | The number of raw records will increase | Ran python lander.py crash then ran python lander.py | raw 205,328 ; malformed 0 ; unique Kafka records 205,328 ; unique icao24+time_position 183,936 -> raw 212,088 ; malformed 0 ; unique Kafka records 211,088 ; unique icao24+time_position 189,652 | The number of records increased because the offset was not stored, so the records that landed before the crash were reprocessed after lander.py was run again. Additionally, there is an increase of unique Kafka records and unique icao24+time_position, but they increased less than the raw, indicating that the reprocess did result in duplicates, which will be handled in the flow. |
| D2 Poison message | The run will raise an exception and the number of malformed records will increase
 | Sent a bad message after running docker exec -it redpanda-0 rpk topic produce opensky_aircraft_states -k bad | raw 212,088 ; malformed 0 ; unique Kafka records 211,088 ; unique icao24+time_position 189,652 -> raw 212,089 ; malformed 1 ; unique Kafka records 211,088 ; unique icao24+time_position 189,652 | The poison message was landed but is flagged as being malformed. Because it is not a valid message, the number of unique Kafka records and unique icao24+time_position does not change. If lander.py crashed, this would mean that subsequent records are not able to land, so flagging this record allows landing to continue smoothly without stopping. |
| D5 Send again | The number of raw and unique Kafka records will increase but the number of unique entities will not | Sent records that were already landed by running two polls 5 seconds apart | raw 307,563 ; malformed 1 ; unique Kafka records 306,562 ; unique icao24+time_position 248,657 -> raw 323,563 ; malformed 1 ; unique Kafka records 322,562 ; unique icao24+time_position 257,080 | When identical records are published again, the raw and unique Kafka records increase since Kafka assigns new offsets to each publication. However, the unique entities should not increase because they represent the same real-world observations. What actually happened: Initially, I polled OpenSky twice, five seconds apart. However, the resulting snapshots were not identical, so the four-number summary showed an increase in unique entities, which was not supposed to happen. I then modified producer.py to publish the exact same snapshot twice instead of fetching two separate snapshots. This successfully demonstrated duplicate publication, as the snapshot-specific results showed 16,998 raw records, 16,998 unique Kafka records, and 8,499 unique entity records. Therefore, this experiment worked, although the four_numbers.py summary does not explicitly show the anticipated results. |

## The four numbers

From the flow log on the full data (screenshot in `docs/`):

| Number | Full data | Sample (`data/sample`) |
|---|---:|---:|
| Raw records landed | TODO | TODO |
| Malformed (kept in raw) | TODO | TODO |
| Unique Kafka records | TODO | TODO |
| Unique TODO (your entity) | TODO | TODO |

## Challenges / Obstacles

_(replace: what this source made hard in gathering, processing and analysis, and how you worked through it. What methods and tools did you use?)_

## Analysis

_(replace: one clear paragraph. Your research question, what the data shows, one finding that surprised you or is not obvious, and one limitation of your data or method.)_

## Plot / Visualization

_(replace: at least one chart, saved in charts/ and shown here. Give it a title and labelled axes with units.)_

![TODO chart title](charts/TODO.png)

## Reflection

_(replace: about 150 words, first person. What did you learn about streaming systems that you did not know before Thu 22 Oct? What would you do differently if you started again?)_

## Acknowledgements

_(replace: everyone who helped you and how (classmates, instructor, online sources), code you reused (for example connection code from probe.py), and any AI assistance: what for, and how you checked the result. Write "None" if there is nothing to declare.)_
