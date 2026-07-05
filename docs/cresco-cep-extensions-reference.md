# Cresco CEP & Siddhi Extensions — Complete Reference

Cresco embeds the [Siddhi](https://github.com/siddhi-io/siddhi) streaming SQL / Complex-Event-Processing
engine so you can **capture data flowing on the dataplane and do something with it in real time** —
measure it, filter it, window it, aggregate it, correlate it, join it, and emit derived streams —
without deploying a plugin. A CEP rule is just a Siddhi query installed over a named dataplane
stream; its output is another named dataplane stream. This is how you build **data streams** in Cresco.

This document is the exhaustive reference for that capability: the architecture, how to create and
feed CEP rules over the dataplane, the SiddhiQL language, and every bundled extension function.

> **Status:** Siddhi **5.1.33** + the four core subprojects + **9 extensions** (1 mapper + 8 execution)
> are embedded in the `io.cresco.library` uber-bundle and **proven** on a live agent:
> **10/10** CEP rules (core + every extension) compile and start, and **end-to-end data flow is
> verified** — feeding `price` into a `math:sqrt` rule returns `root`, and a windowed `sum` returns
> running totals, both entirely over the dataplane. See [§9 Verified results](#9-verified-results).

---

## 1. What Siddhi provides

Siddhi is a cloud-native streaming + CEP engine driven by a streaming-SQL dialect, **SiddhiQL**. It
consumes events from *sources*, runs continuous *queries* against them (using *windows* to retain
history for aggregation / pattern matching / joins), and publishes results to *sinks*. In Cresco the
source and sink are always the **Cresco dataplane** (Siddhi's own transports are not used — the
dataplane *is* the transport).

Capabilities used through Cresco:

| Capability | SiddhiQL | Provided by |
|---|---|---|
| Streams & filters | `from S[price > 100] select ...` | siddhi-core |
| Windows | `#window.length(5)`, `#window.time(10 sec)`, session, sort, … | siddhi-core |
| Aggregations | `sum`, `avg`, `count`, `min`, `max`, `distinctCount`, `stdDev` | siddhi-core |
| Group by / having | `group by symbol having total > 1000` | siddhi-core |
| Patterns & sequences | `from every e1=S -> e2=S[...]` | siddhi-core |
| Joins | `from A#window.time(1 min) join B on A.id == B.id` | siddhi-core |
| Partitions | `partition with (symbol of S) begin ... end` | siddhi-core |
| Named/incremental aggregation | `define aggregation ... aggregate every sec...year` | siddhi-core |
| Scalar functions / stream processors | `math:sqrt`, `str:concat`, `unique:length`, … | **extensions** |
| Serialization | `@map(type='json')` | **siddhi-map-json** |

### The four subprojects

`siddhi-core` transitively pulls the other three; together they are the engine:

| Module | Role |
|---|---|
| `io.siddhi:siddhi-core` | Runtime: stream junctions, windows/tables, query execution, thread management, the built-in `inMemory` source/sink. |
| `io.siddhi:siddhi-query-api` | The object model for streams / definitions / queries (build queries programmatically). |
| `io.siddhi:siddhi-query-compiler` | Compiles SiddhiQL text (ANTLR grammar) into the query-api object model. Cresco passes text, so this is what parses your rule. |
| `io.siddhi:siddhi-annotations` | `@Extension`/`@Parameter`/`@ReturnAttribute` meta-annotations + the classindex used to discover extensions at runtime. |

---

## 2. Versions & packaging

| Artifact | Version | Notes |
|---|---|---|
| `io.siddhi:siddhi-core` (+ query-api, query-compiler, annotations) | **5.1.33** | latest 5.1.x; bytecode is Java 8 → runs on JDK 21. API-compatible with the previous 5.1.2. |
| `io.siddhi.extension.map.json:siddhi-map-json` | **5.2.5** | the `@map(type='json')` mapper every CEP uses |
| `io.siddhi.extension.execution.math` | **5.0.5** | |
| `io.siddhi.extension.execution.string` | **5.0.12** | |
| `io.siddhi.extension.execution.time` | **5.0.8** | |
| `io.siddhi.extension.execution.regex` | **5.0.7** | |
| `io.siddhi.extension.execution.map` | **5.0.7** | |
| `io.siddhi.extension.execution.json` | **2.0.11** | |
| `io.siddhi.extension.execution.unique` | **5.0.5** | |
| `io.siddhi.extension.execution.reorder` | **5.0.3** | |

All extensions declare the OSGi import range `[5.0.0, 6.0.0)`, so they wire cleanly against core 5.1.33.

### Packaging notes (why the build looks the way it does)

The library is a shaded OSGi **uber-bundle** that embeds and re-exports Siddhi. Four things were needed
to make 5.1.33 + extensions work inside it — all documented inline in `library/pom.xml`:

1. **siddhi-core shades log4j2** (including log4j's own OSGi `BundleActivator` classes). maven-bundle-plugin
   scans embedded jars for `BundleActivator`s and appends them to `Bundle-Activator`, which then fails
   ("multiple types"). Fix: an antrun step (`strip-log4j-from-siddhi-core`) unpacks siddhi-core, deletes
   the shaded `org/apache/logging/log4j` + `org/apache/log4j`, and the bundle inlines the clean jar via
   `<Include-Resource>`. Siddhi logs via SLF4J (which Cresco exports), so the shaded log4j is dead weight.
2. **guava:** siddhi-core 5.1.2 shaded guava 23; 5.1.33 pulls guava 33 as a normal dep. Consumer bundles
   (controller/wsapi/stunnel) import `com.google.common.*` with range `[23,24)`; siddhi 5.1.33's own range
   is `[23,34)`. Pinning **guava 23.6.1-jre** satisfies both — no consumer rebuild, zero API drift.
3. **accessors-smart:** json-smart 2.5.x needs `net.minidev.asm.*` (accessors-smart). The bundle only
   embeds *direct* deps (`Embed-Transitive` is off), so it's declared directly — otherwise the json
   `@map` fails at runtime with `NoClassDefFoundError: net/minidev/asm/FieldFilter`.
4. **`NoOpActivator`** is declared as the bundle activator so bnd doesn't try to auto-derive one.

---

## 3. How CEP works in Cresco (the data path)

```
  your client                     wsapi (global)                 controller (agent)
  ───────────                     ──────────────                 ──────────────────
  dp.send(json)  ──ws──▶  /api/dataplane  ──▶  global.event.<shardFor(IN)>
                                                     │  selector stream_name='IN'
                                                     ▼
                                            CEPInstance listener ──▶ Siddhi inMemory source
                                                                     (@map json → event)
                                                                          │
                                                              your SiddhiQL query runs
                                                              (windows / functions / joins)
                                                                          │
                                                     Siddhi inMemory sink (@map json)
                                                                          ▼
                                            OutputSubscriber ──▶ global.event.<shardFor(OUT)>
                                                                     stream_name='OUT'
  callback(json) ◀──ws──  /api/dataplane  ◀──────────────────────────────┘
```

Key facts:

- **Routing is by the `stream_name` message property.** A CEP with input stream `IN` installs a
  dataplane listener with selector `stream_name='IN'`; anything published to the dataplane with
  `stream_name=IN` becomes an input event. Output events are published with `stream_name=OUT`.
- **CEP I/O rides the GLOBAL sharded dataplane** — the same topic the wsapi `/api/dataplane` endpoint
  uses — so any external client can feed a CEP's input and read its output. Publisher and subscriber
  derive the same shard from the stream name (`shardFor(name)`), so they always meet.
- **The mapper is JSON.** Input events must be JSON matching the input schema; output events are JSON
  of the output schema, wrapped by Siddhi's json sink as `{"event": { ... }}`.
- CEP rules live on the agent they were created on; the `SiddhiManager` and its extension registry are
  built with the thread-context classloader pinned to the library bundle (`CEPEngine`/`CEPInstance`),
  which is what makes extension resolution reliable under OSGi.

Relevant source: [`CEPEngine.java`](../code/controller/src/main/java/io/cresco/agent/data/CEPEngine.java),
[`CEPInstance.java`](../code/controller/src/main/java/io/cresco/agent/data/CEPInstance.java),
[`OutputSubscriber.java`](../code/controller/src/main/java/io/cresco/agent/data/OutputSubscriber.java).

---

## 4. Creating a CEP rule — the `cepadd` action

CEP is created with the **`cepadd`** MsgEvent action (type `CONFIG`) on the agent controller
(`AgentExecutor`). Parameters (packed into the compressed `cepparams` object):

| Field | Meaning |
|---|---|
| `input_stream` | input stream name (also the `stream_name` you publish to) |
| `input_stream_desc` | Siddhi attribute list, e.g. `symbol string, price double, volume int` |
| `output_stream` | output stream name (the `stream_name` you subscribe to) |
| `output_stream_desc` | output attribute list, e.g. `symbol string, avgPrice double` |
| `query` | the SiddhiQL query (references `input_stream` and `output_stream` by name) |

Returns `status_code=10` + `cepid` on success. A malformed query or an unknown extension returns
`status_code=9` with the Siddhi error text in `status_desc`/`error`.

Attribute types: `string`, `int`, `long`, `float`, `double`, `bool`, `object`.

### Python client

```python
from pycrescolib.clientlib import clientlib
c = clientlib("localhost", 8282, "test-service-key-0001"); c.connect()

reply = c.agents.cepadd(
    input_stream="trades_in",
    input_stream_desc="symbol string, price double, volume int",
    output_stream="trades_avg",
    output_stream_desc="symbol string, avgPrice double, cnt long",
    query=("from trades_in#window.length(10) "
           "select symbol, avg(price) as avgPrice, count() as cnt "
           "group by symbol insert into trades_avg;"),
    dst_region="my-region", dst_agent="my-agent")
assert reply["status_code"] == "10"
cepid = reply["cepid"]
```

### Java client

```java
GlobalController gc = client.getGlobalController();
// cepadd is a CONFIG MsgEvent to AgentExecutor; build cepparams and send via the agent path.
```

---

## 5. Feeding input & reading output over the dataplane

A dataplane connection is opened with a small JSON **stream config**; the `ident_id` is the stream name
and `ident_key` is `stream_name` (the property CEP routes on):

```python
import json, threading
from pycrescolib.clientlib import clientlib

def cfg(name):
    return json.dumps({"ident_key": "stream_name", "ident_id": name,
                       "io_type_key": "type", "output_id": "output", "input_id": "input"})

c = clientlib("localhost", 8282, "test-service-key-0001"); c.connect()

# reader on the OUTPUT stream
out = []
rx = c.get_dataplane(cfg("trades_avg"), lambda m: out.append(json.loads(m)))
rx.connect()

# writer on the INPUT stream
tx = c.get_dataplane(cfg("trades_in"))
tx.connect()

import time; time.sleep(2)                      # let listeners register on the shard
tx.send(json.dumps({"symbol": "AAA", "price": 101.0, "volume": 5}))
tx.send(json.dumps({"symbol": "AAA", "price": 103.0, "volume": 7}))
time.sleep(2)
for e in out:                                   # {"event": {"symbol":"AAA","avgPrice":102.0,"cnt":2}}
    print(e.get("event", e))
```

Notes:
- Send **JSON matching the input schema**. Extra fields are ignored; missing required fields drop the event.
- Output arrives as `{"event": {...}}` (Siddhi json sink framing) — read `.get("event", obj)`.
- There is a brief registration delay after `connect()`; sleep ~1–2 s before the first `send`.

---

## 6. SiddhiQL fundamentals (with Cresco examples)

Assume `define stream trades_in (symbol string, price double, volume int);` (Cresco builds this from
`input_stream` + `input_stream_desc`). You write only the `from ... insert into <output_stream>;` part.

**Filter / projection**
```sql
from trades_in[price > 100.0 and volume > 0]
select symbol, price
insert into trades_out;
```

**Length window + aggregation** (last N events)
```sql
from trades_in#window.length(10)
select symbol, avg(price) as avgPrice, max(price) as hi, min(price) as lo, count() as n
group by symbol
insert into trades_out;
```

**Time window** (events in the last 30 s)
```sql
from trades_in#window.time(30 sec)
select symbol, sum(volume) as vol
group by symbol
insert into trades_out;
```

**Time-batch (tumbling) window** — emit one row per 1-minute bucket
```sql
from trades_in#window.timeBatch(1 min)
select symbol, avg(price) as avgPrice
group by symbol
insert into trades_out;
```

**Having** (threshold alerting)
```sql
from trades_in#window.time(1 min)
select symbol, sum(volume) as vol
group by symbol
having vol > 1000
insert into trades_alerts;
```

**Pattern** (detect a drop then recovery for a symbol)
```sql
from every e1=trades_in -> e2=trades_in[e2.symbol == e1.symbol and e2.price < e1.price * 0.9]
select e1.symbol as symbol, e1.price as before, e2.price as after
insert into trades_drops;
```

**Join two streams** (combining streams — e.g. enrich trades with quotes)
```sql
from trades_in#window.time(1 min) as t
  join quotes_in#window.length(1) as q on t.symbol == q.symbol
select t.symbol as symbol, t.price as trade, q.bid as bid
insert into enriched_out;
```

Windows available in core: `length`, `lengthBatch`, `time`, `timeBatch`, `externalTime`,
`externalTimeBatch`, `session`, `sort`, `cron`, `delay`, `timeLength`, `frequent`, `lossyFrequent`.

---

## 7. Extension reference

Each extension adds functions (used in `select`) or stream-processors/windows (used with `#`). Namespaces
are the prefix before `:`. Everything below is embedded and available to any CEP query.

### 7.1 `math:` — mathematical & statistical functions (siddhi-execution-math 5.0.5)

**Use for:** measurement and numeric transforms on dataplane telemetry.

`abs`, `acos`, `asin`, `atan`, `bin`, `cbrt`, `ceil`, `conv`, `copySign`, `cos`, `cosh`, `e`, `exp`,
`floor`, `getExponent`, `hex`, `isInfinite`, `isNan`, `ln`, `log`, `log10`, `log2`, `max`, `min`, `oct`,
`parseDouble`, `parseFloat`, `parseInt`, `parseLong`, `pi`, `power`, `rand`, `round`, `signum`, `sin`,
`sinh`, `sqrt`, `tan`, `tanh`, `toDegrees`, `toRadians`.

```sql
-- normalize a reading and flag out-of-range magnitude
from sensor_in
select id, math:sqrt(power) as rms, math:log10(math:abs(value) + 1.0) as mag
insert into sensor_norm;
```
> Verified live: feeding `price` = 16, 81, 100 into `select math:sqrt(price) as root` returns
> `root` = 4.0, 9.0, 10.0. (For descriptive statistics over a window use core aggregators —
> `avg`, `stdDev`, `min`, `max`, `count` — with `#window.*`.)

### 7.2 `str:` — string functions (siddhi-execution-string 5.0.12)

**Use for:** parsing/normalizing text payloads, building keys, log processing.

`charAt`, `charFrequency`, `coalesce`, `concat`, `contains`, `equalsIgnoreCase`, `fillTemplate`,
`groupConcat`, `hex`, `length`, `locate`, `lower`, `regexp`, `repeat`, `replaceAll`, `replaceFirst`,
`reverse`, `split`, `strcmp`, `substr`, `tokenize`, `trim`, `unhex`, `upper`.

```sql
from log_in
select str:upper(str:trim(level)) as level,
       str:split(msg, ' ', 0) as verb,
       str:contains(msg, 'ERROR') as isError
insert into log_norm;
```
`str:tokenize` and `str:groupConcat` are stream/aggregate processors (one row → many, or many → one).

### 7.3 `time:` — time & date functions (siddhi-execution-time 5.0.8)

**Use for:** timestamping, bucketing, age/latency computation, timezone handling.

`currentDate`, `currentTime`, `currentTimestamp`, `date`, `dateAdd`, `dateDiff`, `dateFormat`,
`dateSub`, `dayOfWeek`, `extract`, `timestampInMilliseconds`, `timezoneConvert`, `utcTimestamp`.

```sql
from event_in
select id,
       time:timestampInMilliseconds() as ingestMs,
       time:extract('HOUR', ts, 'yyyy-MM-dd HH:mm:ss') as hour
insert into event_stamped;
```
> Verified live: `select time:timestampInMilliseconds() as ts` compiles and emits a `long`.

### 7.4 `regex:` — regular expressions (siddhi-execution-regex 5.0.7)

**Use for:** matching/extracting from text streams. Precompile once per pattern for speed.

`find`, `group`, `lookingAt`, `matches`.

```sql
from log_in
select msg,
       regex:matches('.*\\bERROR\\b.*', msg) as isError,
       regex:group('user=(\\w+)', msg, 1) as user
insert into log_parsed;
```
> Verified live: `select regex:find('[A-Z]+', symbol) as matched` compiles and emits a `bool`.

### 7.5 `map:` — in-stream key/value maps (siddhi-execution-map 5.0.7)

**Use for:** carrying/manipulating dynamic key/value data (an attribute of type `object`) inside a stream —
building, merging, and (de)serializing maps. Not to be confused with the `@map(...)` *mapper*.

`clear`, `clone`, `combineByKey`, `containsKey`, `containsValue`, `create`, `createFromJSON`,
`createFromXML`, `get`, `isEmpty`, `isMap`, `put`, `putAll`, `putIfAbsent`, `remove`, `replace`,
`replaceAll`, `toJSON`, `toXML`.

```sql
from raw_in
select map:createFromJSON(payload) as m           -- payload is a JSON string
insert into map_stream;

from map_stream
select map:get(m, 'deviceId') as deviceId, map:toJSON(m) as json
insert into map_out;
```
> Verified live: `select map:createFromJSON('{"k":"v"}') as m` (output attr `m object`) compiles & starts.

### 7.6 `json:` — in-stream JSON (siddhi-execution-json 2.0.11)

**Use for:** reading/writing fields of JSON documents carried in the stream by JSONPath, without a full schema.

`getBool`, `getDouble`, `getFloat`, `getInt`, `getLong`, `getObject`, `getString`, `setElement`,
`toObject`, `toString`, `tokenize`, `tokenizeAsObject`, and the aggregates `group` / `groupAsObject`.

```sql
from raw_in
select json:getString(doc, '$.device.id')  as deviceId,
       json:getDouble(doc, '$.reading.temp') as tempC
insert into readings;
```
> Verified live: `select json:getDouble('{"p":1.5}', '$.p') as p` returns `1.5`.

### 7.7 `unique:` — unique windows / dedup (siddhi-execution-unique 5.0.5)

**Use for:** deduplication and "distinct latest" windows keyed by an attribute.

Windows: `unique:length`, `unique:lengthBatch`, `unique:time`, `unique:timeBatch`, `unique:ever`,
`unique:first`, `unique:firstLengthBatch`, `unique:firstTimeBatch`, `unique:externalTimeBatch`,
`unique:timeLengthBatch`. Stream processor: `unique:deduplicate`.

```sql
-- keep only the latest event per deviceId across the last 1000 distinct devices
from device_in#window.unique:length(deviceId, 1000)
select deviceId, status, value
insert into device_latest;

-- drop duplicate (deviceId,seq) within 10 s
from device_in#window.unique:deduplicate(str:concat(deviceId,'-',seq), 10 sec)
select *
insert into device_dedup;
```
> Verified live: `from IN#window.unique:length(symbol, 5) select symbol, price` compiles & starts.

### 7.8 `reorder:` — event reordering (siddhi-execution-reorder 5.0.3)

**Use for:** re-ordering out-of-order events by a timestamp attribute before windowing — important across a
mesh where events from different agents arrive skewed.

`reorder:kslack` (K-Slack buffer) and `reorder:akslack` (adaptive K-Slack).

```sql
-- reorder by event-time (ts, a long) with a 5 s max drift, then window
from event_in#reorder:kslack(ts, 5000L)
select id, ts, value
insert into event_ordered;
```
> Verified live: `from IN#reorder:kslack(ts) select symbol, price` (input has `ts long`) compiles & starts.

### 7.9 `@map(type='json')` — the JSON mapper (siddhi-map-json 5.2.5)

Every Cresco CEP uses this on both the auto-generated `@source` and `@sink`. Input dataplane text is parsed
as JSON into the input stream; output events are serialized as JSON `{"event": {...}}`. You do not write
`@map` yourself — Cresco adds it — but it is why input must be JSON and output is JSON.

---

## 8. Recipes for the stated intent

**Rolling average / rate (measurement)**
```sql
from metrics_in#window.time(30 sec)
select host, avg(cpu) as cpuAvg, max(cpu) as cpuMax, count()/30.0 as evtsPerSec
group by host
insert into metrics_rollup;
```

**Threshold alert with dedup**
```sql
from metrics_in[cpu > 90.0]#window.unique:time(host, 60 sec)
select host, cpu, time:currentTimestamp() as at
insert into cpu_alerts;
```

**Combine two dataplane streams (join / enrich)**
```sql
from orders_in#window.time(5 min) as o
  join inventory_in#window.length(1) as i on o.sku == i.sku
select o.sku as sku, o.qty as qty, i.onHand as onHand, (i.onHand - o.qty) as remaining
insert into fulfillment;
```

**Derived stream feeding another CEP** — set one rule's `output_stream` as another rule's `input_stream`
to build multi-stage pipelines entirely on the dataplane.

---

## 9. Verified results

Run `run/tests/cep_validation.sh` (creation) and `run/tests/cep_dataflow.sh` (end-to-end) against a live agent.

**Creation — 10/10** (`cep_validation.py`), 0 `ExtensionNotFound`:
`core_window_agg`, `core_filter_having`, `ext_math`, `ext_string`, `ext_time`, `ext_regex`, `ext_map`,
`ext_json`, `ext_unique`, `ext_reorder` — each returns a `cepid`.

**End-to-end data flow — 2/2** (`cep_dataflow.py`):

```
sqrt:      fed price=16,81,100  →  out {"symbol":"AAA","root":4.0}
                                   out {"symbol":"BBB","root":9.0}
                                   out {"symbol":"CCC","root":10.0}       (extension executed on live data)

windowsum: fed price=10,20,30   →  out {"symbol":"Z","total":10.0}
                                   out {"symbol":"Z","total":30.0}
                                   out {"symbol":"Z","total":60.0}        (aggregation over the stream)
```

---

## 10. Operational notes & limits

- **Where to run a CEP:** `cepadd` targets a specific `dst_region`/`dst_agent`. The rule executes on that
  agent; its input/output streams are reachable fabric-wide over the GLOBAL dataplane.
- **Sharding:** if the dataplane shard count is > 1, input and output may live on different shard-topics;
  publisher and subscriber both derive the shard from the stream name, so alignment is automatic.
- **Lifecycle:** `removeCEP(cepid)` tears a rule down (unsubscribes, shuts the Siddhi app runtime).
- **Schema discipline:** the `select` output attributes must match `output_stream_desc` exactly (names + types).
- **Heavy/unsupported extensions (deliberately not bundled):** `execution-streamingml` (ND4J native),
  `execution-tensorflow` (TF native libs), and the `io-*` transports (Kafka/MQTT/HTTP/…) — the Cresco
  dataplane *is* the transport, so io-* would bypass the bus. `store-rdbms`/`store-mongodb` can be added
  later if you need CEP tables backed by an external database.
