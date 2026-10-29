#!/usr/bin/env python3
"""
Live dataplane probe for peer identity + stream recipient binding (OUT-28), run by peer_identity_live_test.sh.

    sender  --ws--> global wsapi (global broker, vm://)
                       | broker bridge, mutual TLS, bound to the region controller's identity
                       v
    receiver --ws--> agent rgr/b1 wsapi (its connection to the region broker, bound to the region's identity)
    spy      --ws--> agent glob/a1 wsapi (its connection to the global broker)

1. an ordinary stream (stream_name=<random>) must arrive in full at the receiver;
2. a recipient-bound stream (cresco_to=<receiver agent path>, set through the wsapi stream config's ident_key) must
   arrive in full at the receiver and NOT at all at the spy, whose subscription the global broker refuses.

Prints one JSON line; exit 0 when both hold.
"""
import argparse
import json
import sys
import threading
import time
import uuid

from pycrescolib.clientlib import clientlib


def cfg(ident_key, ident_id):
    return json.dumps({"ident_key": ident_key, "ident_id": ident_id, "io_type_key": "type",
                       "output_id": "output", "input_id": "output"})


class Counter:
    def __init__(self):
        self.n = 0
        self.lock = threading.Lock()

    def text(self, _m):
        with self.lock:
            self.n += 1

    def binary(self, _d):
        with self.lock:
            self.n += 1


def stream(tx, rx_clients, config, messages, settle):
    counters = []
    dps = []
    for c in rx_clients:
        k = Counter()
        dp = c.get_dataplane(config, k.text, k.binary)
        ok = dp.connect()
        counters.append((k, ok))
        dps.append(dp)
    tx_dp = tx.get_dataplane(config)
    if not tx_dp.connect():
        return None
    time.sleep(3.0)                       # subscription demand crosses the bridge
    payload = b"x" * 4096
    for _ in range(messages):
        tx_dp.send_binary(payload)
    deadline = time.time() + settle
    while time.time() < deadline and counters[0][0].n < messages:
        time.sleep(0.1)
    time.sleep(1.0)                       # anything a spy would get arrives too
    return [(k.n, ok) for k, ok in counters]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--tx-port", type=int, required=True)
    ap.add_argument("--rx-port", type=int, required=True)
    ap.add_argument("--spy-port", type=int, required=True)
    ap.add_argument("--rx-path", required=True, help="receiver agent path <region>_<agent>")
    ap.add_argument("--key", required=True)
    ap.add_argument("--messages", type=int, default=200)
    ap.add_argument("--settle", type=float, default=30.0)
    a = ap.parse_args()

    tx = clientlib(a.host, a.tx_port, a.key)
    rx = clientlib(a.host, a.rx_port, a.key)
    spy = clientlib(a.host, a.spy_port, a.key)
    if not (tx.connect() and rx.connect() and spy.connect()):
        print(json.dumps({"error": "wsapi connect failed"}))
        return 2

    plain = stream(tx, [rx], cfg("stream_name", "pid-" + uuid.uuid4().hex[:8]), a.messages, a.settle)
    bound = stream(tx, [rx, spy], cfg("cresco_to", a.rx_path), a.messages, a.settle)
    out = {
        "messages": a.messages,
        "plain_received": plain[0][0] if plain else None,
        "bound_received": bound[0][0] if bound else None,
        "bound_spy_received": bound[1][0] if bound else None,
        "bound_spy_dataplane_connect": bound[1][1] if bound else None,
    }
    ok = (plain and plain[0][0] == a.messages and bound and bound[0][0] == a.messages and bound[1][0] == 0)
    out["ok"] = bool(ok)
    print(json.dumps(out))
    for c in (tx, rx, spy):
        try:
            c.close()
        except Exception:
            pass
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
