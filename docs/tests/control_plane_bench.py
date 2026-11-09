#!/usr/bin/env python3
"""Control-plane MsgEvent round trips through a live fabric, from a client of the global's wsapi (control_plane_bench.sh).

For each target node it measures serial round-trip latency (p50/p90/p99/max over --n calls) and, with --threads, the
aggregate rate of concurrent callers (each with its own API connection, for --seconds). A round trip is one EXEC RPC
(iscontrolleractive) addressed to the target's controller and its reply: through the global's own controller (target
glob/gc: in-JVM, the client/wsapi floor), to an agent attached to the global's broker ("direct": one broker), or to an agent
behind a region ("bridge": global -> bridge -> region -> agent and back).

Usage: control_plane_bench.py --port <wsapi> [--key K] [--pin FP] --target name=region/agent ... [--n 300]
                              [--threads 1,8,32] [--seconds 15]
Output: one JSON line {"targets": {name: {"serial": {...}, "concurrent": {threads: msg_per_s}}}}.
Needs pycrescolib on PYTHONPATH; production nodes take the client certificate from CRESCO_TLS_CLIENT_CERT / _KEY.
"""
import argparse
import json
import statistics
import sys
import threading
import time

from pycrescolib.clientlib import clientlib


def connect(a):
    kw = {}
    if a.pin:
        kw["fingerprint"] = a.pin
    c = clientlib(a.host, a.port, a.key or None, **kw)
    if not c.connect():
        raise RuntimeError("wsapi connect failed")
    return c


def call(c, region, agent):
    reply = c.messaging.global_agent_msgevent(True, "EXEC", {"action": "iscontrolleractive"}, region, agent)
    if not reply or not str(reply.get("is_controller_active", "")).lower() in ("true", "1"):
        raise RuntimeError("no or negative reply: %r" % (reply,))


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(p / 100.0 * (len(xs) - 1)))))]


def serial(c, region, agent, n):
    for _ in range(min(20, n)):          # warm up (connections, JIT, caches)
        call(c, region, agent)
    lat, errors = [], 0
    for _ in range(n):
        t = time.perf_counter()
        try:
            call(c, region, agent)
            lat.append((time.perf_counter() - t) * 1000.0)
        except Exception:
            errors += 1
    if not lat:
        return {"errors": errors}
    return {"n": len(lat), "errors": errors, "p50_ms": round(pct(lat, 50), 2), "p90_ms": round(pct(lat, 90), 2),
            "p99_ms": round(pct(lat, 99), 2), "max_ms": round(max(lat), 2), "mean_ms": round(statistics.mean(lat), 2)}


def concurrent(a, region, agent, threads, seconds):
    clients = [connect(a) for _ in range(threads)]
    done = [0] * threads
    errs = [0] * threads
    lats = [[] for _ in range(threads)]
    stop = time.time() + seconds

    def run(i):
        while time.time() < stop:
            t = time.perf_counter()
            try:
                call(clients[i], region, agent)
                lats[i].append((time.perf_counter() - t) * 1000.0)
                done[i] += 1
            except Exception:
                errs[i] += 1

    t0 = time.time()
    ts = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    el = time.time() - t0
    for c in clients:
        try:
            c.close()
        except Exception:
            pass
    lat = [x for l in lats for x in l]
    r = {"msg_per_s": round(sum(done) / el, 1), "errors": sum(errs)}
    if lat:
        r.update({"p50_ms": round(pct(lat, 50), 2), "p99_ms": round(pct(lat, 99), 2)})
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--key", default="")
    ap.add_argument("--pin", default="")
    ap.add_argument("--target", action="append", default=[], help="name=region/agent (repeatable)")
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--threads", default="1,8,32")
    ap.add_argument("--seconds", type=float, default=15.0)
    a = ap.parse_args()
    out = {"targets": {}}
    c = connect(a)
    try:
        for t in a.target:
            name, path = t.split("=", 1)
            region, agent = path.split("/", 1)
            r = {"serial": serial(c, region, agent, a.n), "concurrent": {}}
            for th in [int(x) for x in a.threads.split(",") if x.strip()]:
                r["concurrent"][str(th)] = concurrent(a, region, agent, th, a.seconds)
            out["targets"][name] = r
            print(json.dumps({name: r}), file=sys.stderr, flush=True)
    finally:
        try:
            c.close()
        except Exception:
            pass
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
