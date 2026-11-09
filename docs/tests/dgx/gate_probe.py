#!/usr/bin/env python3
"""Small client-side probes of the DGX gate (gate.sh), through a global's wsapi with pycrescolib.

  gate_probe.py ready --host H --port P [--key K] [--pin FP] --target region/agent ...   exit 0 once every target answers
  gate_probe.py count --host H --port P [--key K] [--pin FP] [--prefix s] [--require region/agent ...]
        agents listed (whose name has prefix); exit 1 unless every --require is listed
  gate_probe.py storm --host H --port P [--key K] [--pin FP] --target region/agent --until FILE
        one RPC after another to the target until FILE exists: {"n", "errors", "p50_ms", "p99_ms", "max_ms", "gap_max_s"}
        (gap_max_s: the longest time without an answer -- whether the controller kept answering)

A production fabric takes the client certificate from CRESCO_TLS_CLIENT_CERT / CRESCO_TLS_CLIENT_KEY_PASSWORD (--key "").
Prints one JSON line.
"""
import argparse
import json
import os
import sys
import time

from pycrescolib.clientlib import clientlib


def connect(a):
    kw = {"fingerprint": a.pin} if a.pin else {}
    c = clientlib(a.host, a.port, a.key or None, **kw)
    if not c.connect():
        raise RuntimeError("wsapi connect failed")
    return c


def call(c, target, timeout=10.0):
    region, agent = target.split("/", 1)
    r = c.messaging.global_agent_msgevent(True, "EXEC", {"action": "iscontrolleractive"}, region, agent, timeout=timeout)
    return bool(r) and str(r.get("is_controller_active", "")).lower() in ("true", "1")


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(p / 100.0 * (len(xs) - 1)))))] if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["ready", "count", "storm"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--key", default="")
    ap.add_argument("--pin", default="")
    ap.add_argument("--target", action="append", default=[])
    ap.add_argument("--prefix", default="")
    ap.add_argument("--require", action="append", default=[])
    ap.add_argument("--until", default="")
    a = ap.parse_args()
    try:
        c = connect(a)
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        return 1
    try:
        if a.what == "ready":
            ok = all(call(c, t, 5.0) for t in a.target)
            print(json.dumps({"ready": ok}))
            return 0 if ok else 1
        if a.what == "count":
            agents = c.globalcontroller.get_agent_list() or []
            names = [str(x.get("agent_id", x.get("agent", ""))) for x in agents]
            paths = {"%s/%s" % (x.get("region_id", x.get("region")), x.get("agent_id", x.get("agent"))) for x in agents}
            print(json.dumps({"listed": len([n for n in names if n.startswith(a.prefix)])}))
            return 0 if set(a.require) <= paths else 1
        lat, errors, gap, last_ok = [], 0, 0.0, time.time()
        while not os.path.exists(a.until):
            t = time.perf_counter()
            try:
                if call(c, a.target[0]):
                    lat.append((time.perf_counter() - t) * 1000.0)
                    now = time.time()
                    gap = max(gap, now - last_ok)
                    last_ok = now
                else:
                    errors += 1
            except Exception:
                errors += 1
                time.sleep(0.2)
            time.sleep(0.05)
        gap = max(gap, time.time() - last_ok)
        print(json.dumps({"n": len(lat), "errors": errors, "p50_ms": round(pct(lat, 50) or 0, 2), "p99_ms": round(pct(lat, 99) or 0, 2),
                          "max_ms": round(max(lat) if lat else 0, 2), "gap_max_s": round(gap, 2)}))
        return 0
    finally:
        try:
            c.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
