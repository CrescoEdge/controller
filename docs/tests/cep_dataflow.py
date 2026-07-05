#!/usr/bin/env python3
"""
CEP end-to-end DATA FLOW over the Cresco dataplane.

Proves the full loop the user cares about — "capture data on the dataplane and do something
with it": an external client feeds events into a CEP's input stream and reads the transformed
results from its output stream, entirely over the wsapi dataplane. This exercises the rewired
CEP I/O (GLOBAL sharded topic) and proves a Siddhi extension actually EXECUTES on live data
(not just that the query compiled).

  writer dp --(stream_name=IN)--> global.event shard --> CEPInstance listener
        --> Siddhi (math:sqrt / windowed sum) --> OutputSubscriber
        --(stream_name=OUT)--> global.event shard --> reader dp callback

Two scenarios:
  1. math:sqrt transform  — feed price, expect sqrt(price)   (extension runs on data)
  2. windowed sum aggregate — feed a burst, expect running totals (stream combining)
"""
import os
import sys
import json
import time
import uuid
import threading

sys.path.insert(0, "/Users/cody/code/cresco/code/pycrescolib")
from pycrescolib.clientlib import clientlib

HOST = os.environ.get("CRESCO_HOST", "localhost")
PORT = int(os.environ.get("CRESCO_PORT", "8282"))
KEY = os.environ.get("CRESCO_KEY", "test-service-key-0001")
REGION = os.environ.get("CEP_REGION", "global-region")
AGENT = os.environ.get("CEP_AGENT", "global-controller")


def cfg(ident):
    return json.dumps({"ident_key": "stream_name", "ident_id": ident,
                       "io_type_key": "type", "output_id": "output", "input_id": "input"})


def extract(msg):
    """Return the inner event dict from a Siddhi json-sink payload ({"event":{...}} or {...})."""
    try:
        o = json.loads(msg)
        return o.get("event", o) if isinstance(o, dict) else {}
    except Exception:
        return {}


def scenario(client, name, in_desc, out_desc, query_tmpl, feed, settle=3.0):
    sid = uuid.uuid4().hex[:8]
    in_s, out_s = f"cepflow_{name}_in_{sid}", f"cepflow_{name}_out_{sid}"
    query = query_tmpl.replace("{IN}", in_s).replace("{OUT}", out_s)

    reply = client.agents.cepadd(in_s, in_desc, out_s, out_desc, query, REGION, AGENT)
    if str((reply or {}).get("status_code")) != "10":
        print(f"RESULT: {name} FAIL (cepadd status={reply})")
        return False
    print(f"  {name}: CEP {reply['cepid']} created")

    got = []
    lock = threading.Lock()
    def on_out(m):
        with lock:
            got.append(m)

    rx = client.get_dataplane(cfg(out_s), on_out)
    if not rx.connect():
        print(f"RESULT: {name} FAIL (reader connect)"); return False
    tx = client.get_dataplane(cfg(in_s))
    if not tx.connect():
        print(f"RESULT: {name} FAIL (writer connect)"); return False

    time.sleep(2.0)  # let the CEP listener + reader listener register on the shard
    for ev in feed:
        tx.send(json.dumps(ev))
        time.sleep(0.25)
    time.sleep(settle)

    with lock:
        events = [extract(m) for m in got]
    print(f"  {name}: fed {len(feed)}, received {len(events)} output event(s)")
    for e in events:
        print(f"      out: {e}")
    return events


def main():
    client = clientlib(HOST, PORT, KEY)
    if not client.connect():
        print("RESULT: connect FAIL"); return 1
    print(f"connected {HOST}:{PORT}; CEP on {REGION}/{AGENT}")

    passed = 0

    # 1) math:sqrt transform — extension executes on live data
    out = scenario(
        client, "sqrt",
        "symbol string, price double", "symbol string, root double",
        "from {IN} select symbol, math:sqrt(price) as root insert into {OUT};",
        [{"symbol": "AAA", "price": 16.0},
         {"symbol": "BBB", "price": 81.0},
         {"symbol": "CCC", "price": 100.0}])
    if out:
        roots = sorted(round(e.get("root"), 3) for e in out if e.get("root") is not None)
        if roots == [4.0, 9.0, 10.0]:
            passed += 1; print("RESULT: sqrt PASS (roots == [4.0, 9.0, 10.0] — extension executed on data)")
        else:
            print(f"RESULT: sqrt FAIL (roots={roots}, expected [4.0, 9.0, 10.0])")

    # 2) windowed sum aggregate — stream combining
    out = scenario(
        client, "windowsum",
        "symbol string, price double", "symbol string, total double",
        "from {IN}#window.length(3) select symbol, sum(price) as total group by symbol insert into {OUT};",
        [{"symbol": "Z", "price": 10.0},
         {"symbol": "Z", "price": 20.0},
         {"symbol": "Z", "price": 30.0}])
    if out:
        totals = [e.get("total") for e in out if e.get("total") is not None]
        if totals and max(totals) == 60.0:
            passed += 1; print(f"RESULT: windowsum PASS (running totals {totals}, final == 60.0 — aggregation over the stream)")
        else:
            print(f"RESULT: windowsum FAIL (totals={totals}, expected final 60.0)")

    print(f"SUMMARY: {passed}/2 data-flow scenarios verified")
    try: client.close()
    except Exception: pass
    return 0 if passed == 2 else 2


if __name__ == "__main__":
    sys.exit(main())
