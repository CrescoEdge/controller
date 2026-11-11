#!/usr/bin/env python3
"""The gate table from gate.sh's results: one column per build (old, oldprod, std, prod), one row per metric, PASS/FAIL
per the gate.

Gate (each candidate against the earlier build in the SAME security profile, same harness: std against old, prod against
oldprod): throughput no more than 10% lower, p99 no more than 20% higher, every frame delivered whole and in order,
every agent registered, no refusal or loss line, and the per-op cost flat as agents grow (serial p50 at the largest
step within 1.25x of the smallest, 8-caller rate within 0.8x).

Without an oldprod run, prod is judged against old (the standard profile) and the report says so: a FAIL of a prod row
may then be the profile and not the build.

Usage: report.py <results dir> [builds...] [--rescan]   (default: the builds the directory holds of old oldprod std prod)
  --rescan reads the logs the run kept again (logscan.py) instead of the counts stored with the results.
"""
import datetime
import json
import os
import statistics
import sys


ALL_BUILDS = ["old", "oldprod", "std", "prod"]
EARLIER = ("old", "oldprod")      # the earlier build, in the standard and in the production profile


def load(d, b):
    p = os.path.join(d, b + ".json")
    return json.load(open(p)) if os.path.exists(p) else None


def get(d, *path):
    for k in path:
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        return ("%.1f" % v) if v >= 10 else ("%.2f" % v)
    return str(v)


def main():
    args = [a for a in sys.argv[1:] if a != "--rescan"]
    d = args[0]
    builds = args[1:] or [b for b in ALL_BUILDS if os.path.exists(os.path.join(d, b + ".json"))] or ["old", "std", "prod"]
    R = {b: load(d, b) for b in builds}

    def baseline(b):
        """The earlier build a candidate is judged against: the one in its own profile when the run has it."""
        return "oldprod" if b == "prod" and R.get("oldprod") is not None else "old"
    if "--rescan" in sys.argv:
        # judge the logs a finished run kept (<results dir>/<build>/logs) by today's rule (logscan.py)
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import logscan
        for b in builds:
            logs = os.path.join(d, b, "logs")
            if R[b] is not None and os.path.isdir(logs):
                # the nodes were stopped when <build>/stop was written: what they logged while stopping is not judged
                stop = os.path.join(d, b, "stop")
                until = datetime.datetime.fromtimestamp(os.path.getmtime(stop)) if os.path.exists(stop) else None
                R[b]["logs"] = logscan.scan(logs, until)
    rows, fails = [], []

    def row(name, vals, kind=None):
        """kind: 'tput' (higher better, -10%), 'p99' (lower better, +20%), None (informational)."""
        verdicts = []
        for b in builds:
            if b in EARLIER or kind is None:
                continue
            v, o = vals.get(b), vals.get(baseline(b))
            if v is None or o is None or o == 0:
                continue
            ok = (v >= 0.9 * o) if kind == "tput" else (v <= 1.2 * o)
            verdicts.append("%s %s" % (b, "PASS" if ok else "FAIL"))
            if not ok:
                fails.append("%s: %s %s vs %s %s" % (name, b, fmt(v), baseline(b), fmt(o)))
        rows.append([name] + [fmt(vals.get(b)) for b in builds] + [", ".join(verdicts)])

    for tgt in ("global", "region", "agent"):
        cp = {b: get(R[b], "control_plane", "targets", tgt) for b in builds}
        row("CP %s serial p50 ms" % tgt, {b: get(cp[b], "serial", "p50_ms") for b in builds})
        row("CP %s serial p99 ms" % tgt, {b: get(cp[b], "serial", "p99_ms") for b in builds}, "p99")
        for th in ("8", "32"):
            row("CP %s %s callers msg/s" % (tgt, th), {b: get(cp[b], "concurrent", th, "msg_per_s") for b in builds}, "tput")
            row("CP %s %s callers p99 ms" % (tgt, th), {b: get(cp[b], "concurrent", th, "p99_ms") for b in builds}, "p99")
    for size, label in (("262144", "256KiB"), ("4194304", "4MiB")):
        for flows in ("1", "4"):
            def reps(b, mode="fire"):
                dp = get(R[b], "dataplane") or {}
                return [v for k, v in dp.items() if k.startswith("%s_%sf_%s_" % (size, flows, mode)) and v]
            def med(b, key, mode="fire"):
                rs = reps(b, mode)
                return statistics.median([r[key] for r in rs]) if rs else None
            row("DP %s %s flow(s) MiB/s" % (label, flows), {b: med(b, "mib_s") for b in builds}, "tput")
            row("DP %s %s flow(s) p50 ms, paced" % (label, flows), {b: med(b, "p50_ms", "paced") for b in builds})
            row("DP %s %s flow(s) p99 ms, paced" % (label, flows), {b: med(b, "p99_ms", "paced") for b in builds}, "p99")
            row("DP %s %s flow(s) p99 ms, flat out (queue depth)" % (label, flows), {b: med(b, "p99_ms") for b in builds})
            lost = {}
            for b in builds:
                rs = reps(b) + reps(b, "paced")
                lost[b] = sum(r["sent"] - r["received"] + r["out_of_order"] + r["bad_size"] + r["dups"] for r in rs) if rs else None
                if b not in EARLIER and rs and lost[b]:
                    fails.append("DP %s %s flow(s): %s lost/damaged/reordered %d" % (label, flows, b, lost[b]))
            row("DP %s %s flow(s) lost+damaged+reordered" % (label, flows), lost)
    steps = sorted({int(s) for b in builds for s in (get(R[b], "scale") or {}).keys()})
    for s in steps:
        sc = {b: get(R[b], "scale", str(s)) for b in builds}
        row("SCALE %d listed" % s, {b: get(sc[b], "listed") for b in builds})
        row("SCALE %d all registered after s" % s, {b: get(sc[b], "all_listed_after_s") for b in builds})
        for n in ("global", "region"):
            row("SCALE %d %s CPU %% steady" % (s, n), {b: get(sc[b], n, "cpu_pct_steady") for b in builds})
            row("SCALE %d %s heap MB / threads" % (s, n),
                {b: "%s / %s" % (get(sc[b], n, "heap_used_mb"), get(sc[b], n, "threads")) for b in builds})
        row("SCALE %d global probe in storm p99 ms" % s, {b: get(sc[b], "global_probe_during_storm", "p99_ms") for b in builds})
        row("SCALE %d global probe longest gap s / errors" % s,
            {b: "%s / %s" % (get(sc[b], "global_probe_during_storm", "gap_max_s"), get(sc[b], "global_probe_during_storm", "errors"))
             for b in builds})
        row("SCALE %d CP global serial p50 ms" % s, {b: get(sc[b], "control_plane", "targets", "global", "serial", "p50_ms") for b in builds})
        row("SCALE %d CP agent 8 callers msg/s" % s,
            {b: get(sc[b], "control_plane", "targets", "agent", "concurrent", "8", "msg_per_s") for b in builds})
        for b in builds:
            if b not in EARLIER and sc[b] and (sc[b].get("listed") or 0) < s:
                fails.append("SCALE %d: %s listed only %s" % (s, b, sc[b].get("listed")))
    if len(steps) >= 2:
        lo, hi = str(steps[0]), str(steps[-1])
        for b in builds:
            if b in EARLIER:
                continue
            p_lo = get(R[b], "scale", lo, "control_plane", "targets", "global", "serial", "p50_ms")
            p_hi = get(R[b], "scale", hi, "control_plane", "targets", "global", "serial", "p50_ms")
            r_lo = get(R[b], "scale", lo, "control_plane", "targets", "agent", "concurrent", "8", "msg_per_s")
            r_hi = get(R[b], "scale", hi, "control_plane", "targets", "agent", "concurrent", "8", "msg_per_s")
            if None not in (p_lo, p_hi, r_lo, r_hi):
                flat = p_hi <= 1.25 * p_lo and r_hi >= 0.8 * r_lo
                rows.append(["per-op cost flat %s->%s (%s)" % (lo, hi, b)] + [""] * len(builds) + ["PASS" if flat else "FAIL"])
                if not flat:
                    fails.append("per-op cost %s: p50 %s->%s ms, rate %s->%s" % (b, p_lo, p_hi, r_lo, r_hi))
    row("log ERROR lines", {b: get(R[b], "logs", "error_lines") for b in builds})
    row("log refusal/loss lines", {b: get(R[b], "logs", "refusal_or_loss_lines") for b in builds})
    row("log calls authority would refuse", {b: get(R[b], "logs", "would_refuse_lines") for b in builds})
    row("log dropped messages (by cause)", {b: (lambda d: None if d is None else
                                               (" ".join("%s=%s" % kv for kv in sorted(d.items())) or "0"))(get(R[b], "logs", "dropped"))
                                            for b in builds})
    for b in builds:
        if b not in EARLIER and (get(R[b], "logs", "refusal_or_loss_lines") or 0) > 0:
            fails.append("%s: %s refusal/loss line(s): %s" % (b, get(R[b], "logs", "refusal_or_loss_lines"),
                                                              [k for k, _ in (get(R[b], "logs", "top") or []) if k.startswith("REFUSAL")][:3]))
        if b not in EARLIER and (get(R[b], "logs", "would_refuse_lines") or 0) > 0:
            # the gate's fabric manages every node from the global's gateway: nothing in it may be out of place
            fails.append("%s: %s call(s) authority_direction=enforce would refuse: %s" % (
                b, get(R[b], "logs", "would_refuse_lines"),
                [k for k, _ in (get(R[b], "logs", "top") or []) if k.startswith("WOULD REFUSE")][:3]))
    w = [max(len(str(r[i])) for r in rows + [["metric"] + builds + ["gate"]]) for i in range(len(builds) + 2)]
    hdr = ["metric"] + builds + ["gate"]
    print("  ".join(h.ljust(w[i]) for i, h in enumerate(hdr)))
    for r in rows:
        print("  ".join(str(c).ljust(w[i]) for i, c in enumerate(r)))
    print()
    for b in builds:
        if b not in EARLIER and R.get(b) is not None:
            if R.get(baseline(b)) is None:
                print("NOTE %s has no earlier build to be judged against in this run: its rate and p99 rows are not judged" % b)
            elif b == "prod" and baseline(b) == "old":
                print("NOTE prod is judged against old, which ran the standard profile (no oldprod in this run): "
                      "a FAIL of a prod row may be the profile, not the build")
            else:
                print("%s is judged against %s" % (b, baseline(b)))
    print("GATE " + ("PASS" if not fails else "FAIL"))
    for f in fails:
        print("  FAIL " + f)
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
