#!/usr/bin/env python3
"""
Live dataplane probe for peer identity + stream recipient binding (OUT-28), run by peer_identity_live_test.sh.

    sender  --ws--> global wsapi (global broker, vm://)
                       | broker bridge, mutual TLS, bound to the region controller's identity
                       v
    receiver --ws--> agent rgr/b1 wsapi (its connection to the region broker, bound to the region's identity)
    spy      --ws--> agent glob/a1 wsapi
    spy2     --ws--> the global's own wsapi (the controller's vm:// connection)

1. an ordinary stream (stream_name=<random>) must arrive in full at the receiver;
2. a recipient-bound stream is written by PeerIdentityRecipientProbe.java (on a node's own broker connection, the way a
   plugin's dataplane writes one: wsapi refuses cresco_to from API clients by design); while it runs, spy2 -- a client of
   the global's own wsapi, i.e. the global controller's vm:// connection -- subscribes to that stream by name and must get
   none of it (--bound-stream, --ready-file, --done-file);
3. an API client that asks wsapi for a stream by its recipient (ident_key cresco_to) is refused by wsapi.

Prints one JSON line; exit 0 when all hold.
"""
import argparse
import json
import sys
import threading
import time
import uuid

from pycrescolib.clientlib import clientlib

try:
    from pycrescolib.dataplane import DataplaneSubscriptionRefused
except ImportError:                     # a client without typed refusals: a refused spy just receives nothing
    class DataplaneSubscriptionRefused(Exception):
        pass


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
        try:
            ok = dp.connect()
            refused = None
        except DataplaneSubscriptionRefused as e:
            ok, refused = False, str(e)
        counters.append((k, ok, refused))
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
    return [(k.n, ok, refused) for k, ok, refused in counters]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--tx-port", type=int, required=True)
    ap.add_argument("--rx-port", type=int, required=True)
    ap.add_argument("--spy-port", type=int, required=True)
    ap.add_argument("--spy2-port", type=int, default=0, help="a second spy, e.g. the global's own wsapi")
    ap.add_argument("--rx-path", required=True, help="receiver agent path <region>_<agent>")
    ap.add_argument("--key", required=True)
    ap.add_argument("--messages", type=int, default=200)
    ap.add_argument("--settle", type=float, default=30.0)
    ap.add_argument("--pin", action="append", default=[], help="<port>=<wsapi certificate fingerprint> (repeatable)")
    ap.add_argument("--bound-stream", default="", help="the recipient-bound stream PeerIdentityRecipientProbe writes")
    ap.add_argument("--ready-file", default="", help="created once spy2 listens and the plain stream is done")
    ap.add_argument("--done-file", default="", help="waited for (at most --listen s) before spy2 stops listening")
    ap.add_argument("--listen", type=float, default=90.0)
    a = ap.parse_args()
    pins = dict(p.split("=", 1) for p in a.pin if "=" in p and p.split("=", 1)[1])

    def client(port):
        # pin the wsapi certificate when its fingerprint is known (clients verify it by default)
        pin = pins.get(str(port))
        return clientlib(a.host, port, a.key, fingerprint=pin) if pin else clientlib(a.host, port, a.key)

    tx = client(a.tx_port)
    rx = client(a.rx_port)
    spy = client(a.spy_port)
    spy2 = client(a.spy2_port) if a.spy2_port else None
    if not (tx.connect() and rx.connect() and spy.connect() and (spy2 is None or spy2.connect())):
        print(json.dumps({"error": "wsapi connect failed"}))
        return 2

    # spy2 listens to the recipient-bound stream by name for the whole run (the global controller's vm:// identity)
    spy2_counter, spy2_dp, spy2_refused = Counter(), None, None
    if spy2 and a.bound_stream:
        spy2_dp = spy2.get_dataplane(cfg("stream_name", a.bound_stream), spy2_counter.text, spy2_counter.binary)
        try:
            spy2_dp.connect()
        except DataplaneSubscriptionRefused as e:
            spy2_refused = str(e)
    # an API client may not name a recipient: wsapi refuses ident_key cresco_to (only infrastructure binds recipients)
    named_ok = None
    try:
        named = spy.get_dataplane(cfg("cresco_to", a.rx_path), Counter().text, Counter().binary)
        named_ok = bool(named.connect())
    except DataplaneSubscriptionRefused:
        named_ok = False
    except Exception:
        named_ok = False

    plain = stream(tx, [rx], cfg("stream_name", "pid-" + uuid.uuid4().hex[:8]), a.messages, a.settle)
    if a.ready_file:
        open(a.ready_file, "w").close()
    if a.done_file:
        deadline = time.time() + a.listen
        while time.time() < deadline:
            try:
                open(a.done_file).close()
                break
            except OSError:
                time.sleep(0.5)
        time.sleep(1.5)
    out = {
        "messages": a.messages,
        "plain_received": plain[0][0] if plain else None,
        "bound_spy2_received": spy2_counter.n if spy2_dp is not None else None,
        "bound_spy2_refused": spy2_refused,
        "wsapi_recipient_key_accepted": named_ok,
    }
    ok = bool(plain and plain[0][0] == a.messages and (spy2_dp is None or spy2_counter.n == 0) and named_ok is False)
    out["ok"] = ok
    print(json.dumps(out))
    for c in (tx, rx, spy) + ((spy2,) if spy2 else ()):
        try:
            c.close()
        except Exception:
            pass
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
