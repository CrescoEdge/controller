#!/usr/bin/env python3
"""Where the time of a join goes, from the logs of one gate build's scale phase.

Usage: joinstages.py <rundir>/logs "<steps>" [--rate] [--steps-file <rundir>/steps.txt]

Per light agent sN (step k holds the agents after step k-1 up to step k) the stages a log line stamps:
  jvm     the JVM started (the launcher's first line; whole seconds)
  svc     the agent service's first line (the controller bundle is running)
  auth    its controller logged the authenticated discovery exchange (identity bound or certified)
  tls     its controller's broker bound the first TLS connection to its identity
  ok      its controller's first health answer from it (it is registered and answering)
and how often its discovery went unanswered, and the ERROR lines of the step (its agents' logs, and the controllers'
lines stamped inside the step). An agent that logs at INFO (gate.sh SCALE_TRACE_EVERY) also stamps its own stages:
  sm      its state machine started        ident   its identity (key and certificate) is made: discovery starts
  disc    its discovery was answered and authenticated     conn   its broker connections are up     reg   registered
Times are seconds from the step's start (steps.txt: when gate.sh released the step; else its first JVM start). A JSON
list (one object per step) on stdout, a table on stderr; with --rate also the joins each controller completed per 5 s
against those already joined.

joinstages.py <rundir>/logs <agents> --since <epoch seconds> --controller rc|gc: a controller was restarted at that
time (gate.sh SCALE_RESTART); one JSON object: how many of its agents are back (their first health answer after it),
how long after the stop and after the controller's first answer, the discoveries that went unanswered meanwhile.
"""
import datetime
import json
import os
import re
import sys

LOG4J = re.compile(r"^(\d{1,2} \w{3} \d{4} \d{2}:\d{2}:\d{2}),(\d{3}) +(\w+) ")
JUL = re.compile(r"^(\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2} [AP]M) ")
AUTH = re.compile(r"authenticated AGENT exchange with (\w+)/(s\d+) ")
TLS = re.compile(r"mTLS identity bound from certificate: CrescoIdentity\{tenant=\w+, region=(\w+), agent=(s\d+),")
OK = re.compile(r"mesh: child (\w+)_(s\d+) health OK")
UNANSWERED = re.compile(r"Discovery Failed|No agents discovered|certDiscovery\.size\(\) == 0|Discovery: .*; attempt \d+, trying again in")
OWN = (("sm", re.compile(r"is_agent /w regional_controller_host|is_agent : 2")), ("ident", re.compile(r"CertificateManager Init:")),
       ("disc", re.compile(r"authenticated AGENT exchange with")), ("conn", re.compile(r"initIOChannels Success")),
       ("reg", re.compile(r"registered with Region")))
REFUSED = re.compile(r"Discovery: refused a connection|Discovery: busy: told")


def t4j(m):
    return datetime.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S").timestamp() + int(m.group(2)) / 1000.0


def pct(xs, p):
    xs = sorted(xs)
    return None if not xs else round(xs[min(len(xs) - 1, int(p * len(xs)))], 1)


def rejoin(d, since, controller):
    """A controller was restarted at `since`: when its agents were back, from its log and theirs, as one JSON object."""
    first = {}  # stage -> {agent: time}
    for l in open(os.path.join(d, controller + ".log"), errors="replace"):
        m = LOG4J.match(l)
        if not m or t4j(m) < since:
            continue
        for key, rx in (("auth", AUTH), ("tls", TLS), ("ok", OK)):
            x = rx.search(l)
            if x:
                first.setdefault(key, {}).setdefault(x.group(2), t4j(m))
    mine = set(first.get("auth", {})) | set(first.get("ok", {}))
    unanswered = errors = agents_unanswered = 0
    for n in mine:
        u = 0
        try:
            for l in open(os.path.join(d, n + ".log"), errors="replace"):
                m = LOG4J.match(l)
                if not m or t4j(m) < since:
                    continue
                errors += m.group(3) == "ERROR"
                u += bool(UNANSWERED.search(l))
        except OSError:
            pass
        unanswered += u
        agents_unanswered += u > 0
    ok = sorted(first.get("ok", {}).values())
    auth = sorted(first.get("auth", {}).values())
    up = auth[0] if auth else None

    def span(xs, base):
        return None if not xs or base is None else {"p50": pct([x - base for x in xs], 0.5), "p90": pct([x - base for x in xs], 0.9),
                                                    "max": round(xs[-1] - base, 1)}
    print(json.dumps({"controller": controller, "back": len(ok), "authenticated": len(auth),
                      "first_answer_after_s": None if up is None else round(up - since, 1),
                      "back_after_stop_s": span(ok, since), "back_after_first_answer_s": span(ok, up),
                      "authenticated_after_first_answer_s": span(auth, up),
                      "connected_after_first_answer_s": span(sorted(first.get("tls", {}).values()), up),
                      "agents_unanswered": agents_unanswered, "unanswered_lines": unanswered, "agent_error_lines": errors}))


def main():
    if "--since" in sys.argv:
        rejoin(sys.argv[1], float(sys.argv[sys.argv.index("--since") + 1]), sys.argv[sys.argv.index("--controller") + 1])
        return
    d, steps = sys.argv[1], [int(x) for x in sys.argv[2].split()]
    a = {}  # name -> stages
    for f in os.listdir(d):
        if not re.fullmatch(r"s\d+\.log", f):
            continue
        st = {"fails": 0, "errors": 0, "fail_at": [], "error_at": []}
        for l in open(os.path.join(d, f), errors="replace"):
            if "jvm" not in st:
                m = JUL.match(l)
                if m:
                    st["jvm"] = datetime.datetime.strptime(m.group(1), "%b %d, %Y %I:%M:%S %p").timestamp()
                    continue
            m = LOG4J.match(l)
            if not m:
                continue
            if "svc" not in st:
                st["svc"] = t4j(m)
            if m.group(3) == "ERROR":
                st["errors"] += 1
                st["error_at"].append(t4j(m))
            for key, rx in OWN:
                if key not in st and rx.search(l):
                    st[key] = t4j(m)
            if UNANSWERED.search(l):
                st["fails"] += 1
                st["fail_at"].append(t4j(m))
                st.setdefault("first_fail", t4j(m))
                st["last_fail"] = t4j(m)
        a[f[:-4]] = st
    ctl = {}  # controller -> [(t, name)] of authenticated exchanges
    refused = {}
    ctl_errors = []
    for c in ("gc", "rc"):
        p = os.path.join(d, c + ".log")
        if not os.path.exists(p):
            continue
        for l in open(p, errors="replace"):
            m = LOG4J.match(l)
            if not m:
                continue
            for key, rx in (("auth", AUTH), ("tls", TLS), ("ok", OK)):
                x = rx.search(l)
                if x and x.group(2) in a:
                    a[x.group(2)].setdefault(key, t4j(m))
                    if key == "auth":
                        ctl.setdefault(c, []).append((t4j(m), x.group(2)))
            if m.group(3) == "ERROR":
                ctl_errors.append(t4j(m))
            if REFUSED.search(l):
                refused[c] = refused.get(c, 0) + 1
    prev, out = 0, []
    released = {}
    after = float("inf")       # when what follows the steps began (a controller restart): its lines are not a step's
    if "--steps-file" in sys.argv:
        try:
            for l in open(sys.argv[sys.argv.index("--steps-file") + 1]):
                if l.split()[0] == "restart":
                    after = float(l.split()[1])
                else:
                    released[int(l.split()[0])] = float(l.split()[1])
        except (OSError, ValueError, IndexError):
            pass
    starts = []
    for s in steps:
        names = ["s%d" % i for i in range(prev + 1, s + 1) if "s%d" % i in a]
        prev = s
        jv = [a[n]["jvm"] for n in names if "jvm" in a[n]]
        if not jv:
            continue
        t0 = released.get(s, min(jv))
        starts.append(t0)

        def col(k, base=None):
            xs = [a[n][k] - (a[n][base] if base else t0) for n in names if k in a[n] and (base is None or base in a[n])]
            return {"p50": pct(xs, 0.5), "p90": pct(xs, 0.9), "max": pct(xs, 1.0), "n": len(xs)}
        r = {"agents": s, "new": len(names), "jvm": col("jvm"), "svc": col("svc"), "auth": col("auth"), "tls": col("tls"),
             "ok": col("ok"), "svc_to_auth": col("auth", "svc"), "auth_to_tls": col("tls", "auth"), "tls_to_ok": col("ok", "tls"),
             "t0": t0, "names": names,
             "traced": {k: col(k) for k, _ in OWN}, "traced_ident_to_disc": col("disc", "ident"),
             "traced_disc_to_conn": col("conn", "disc"), "traced_conn_to_reg": col("reg", "conn")}
        out.append(r)
    for i, r in enumerate(out):
        # a step's lines: those of its own agents and of the controllers, stamped before the next step (or a restart)
        end = starts[i + 1] if i + 1 < len(starts) else after
        names = r.pop("names")
        r["agents_unanswered"] = sum(1 for n in names if any(r["t0"] <= t < end for t in a[n]["fail_at"]))
        r["unanswered_lines"] = sum(sum(1 for t in a[n]["fail_at"] if r["t0"] <= t < end) for n in names)
        r["agent_error_lines"] = sum(sum(1 for t in a[n]["error_at"] if r["t0"] <= t < end) for n in names)
        r["controller_error_lines"] = sum(1 for t in ctl_errors if r["t0"] <= t < end)
        r["error_lines"] = r["agent_error_lines"] + r["controller_error_lines"]
    print(json.dumps(out))
    print("%6s %5s | %-17s %-17s %-17s %-17s | %-17s %-15s %-15s | unanswered" % (
        "agents", "new", "jvm p50/p90/max", "svc", "auth", "ok", "svc->auth", "auth->tls", "tls->ok"), file=sys.stderr)
    for r in out:
        def f(k):
            return "%s/%s/%s" % (r[k]["p50"], r[k]["p90"], r[k]["max"])
        print("%6d %5d | %-17s %-17s %-17s %-17s | %-17s %-15s %-15s | %d agents, %d lines" % (
            r["agents"], r["new"], f("jvm"), f("svc"), f("auth"), f("ok"), f("svc_to_auth"), f("auth_to_tls"), f("tls_to_ok"),
            r["agents_unanswered"], r["unanswered_lines"]), file=sys.stderr)
    print("%6s | %-17s %-17s %-17s %-17s %-17s | %-15s %-15s %-15s | ERROR lines" % (
        "agents", "traced: sm", "ident", "disc", "conn", "reg", "ident->disc", "disc->conn", "conn->reg"), file=sys.stderr)
    for r in out:
        def g(v):
            return "%s/%s/%s" % (v["p50"], v["p90"], v["max"])
        t = r["traced"]
        print("%6d | %-17s %-17s %-17s %-17s %-17s | %-15s %-15s %-15s | %d agents' + %d controllers'" % (
            r["agents"], g(t["sm"]), g(t["ident"]), g(t["disc"]), g(t["conn"]), g(t["reg"]), g(r["traced_ident_to_disc"]),
            g(r["traced_disc_to_conn"]), g(r["traced_conn_to_reg"]), r["agent_error_lines"], r["controller_error_lines"]), file=sys.stderr)
    if refused:
        print("connections the discovery listener refused (lines, rate limited): %s" % refused, file=sys.stderr)
    if "--rate" in sys.argv:
        for c, xs in sorted(ctl.items()):
            xs.sort()
            t0, joined, i = xs[0][0], 0, 0
            print("%s: joins completed per 5 s (already joined -> joins)" % c, file=sys.stderr)
            row = []
            while i < len(xs):
                n = 0
                while i < len(xs) and xs[i][0] < t0 + 5:
                    n += 1
                    i += 1
                if n:
                    row.append("%d->%d" % (joined, n))
                joined += n
                t0 += 5
            print("  " + " ".join(row), file=sys.stderr)


if __name__ == "__main__":
    main()
