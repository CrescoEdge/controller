#!/usr/bin/env python3
"""What a JFR recording of a controller says about where its time went: a text summary on stdout.

Usage: jfrtop.py <jfr tool> <file.jfr> [bucket seconds]

  CPU      the JVM's CPU load over the recording (jdk.CPULoad) and the threads that used it (jdk.ThreadCPULoad)
  samples  jdk.ExecutionSample: the top leaf frames, the io.cresco frames a sample passes through (each counted once
           per sample: an inclusive share), the same per thread kind, and per time bucket the share of the frames of
           the join path, so that the cost of a join can be read against the joins already made
  blocked  jdk.JavaMonitorEnter: time threads waited for a monitor, by the monitor's class and the first io.cresco frame
  disk     jdk.FileForce and jdk.FileWrite: time and count by the first io.cresco frame

It reads `jfr print` as text, one event at a time (a recording of a storm holds some 10^5 samples).
"""
import collections
import re
import subprocess
import sys

JOIN = ("DiscoveryProcessor.serverResult", "DiscoveryProcessor.serverChallenge", "ActiveBroker.updateTrustManager",
        "CertificateManager.getTrustManagers", "CertificateManager.replaceTrustStoreEntries",
        "CertificateManager.saveKeyAndTrustStore", "PeerTrustRegistry.pin", "PeerTrustRegistry.save",
        "SSLContextImpl.engineInit", "BoundKeyTrustManager.checkClientTrusted", "CrescoAuthorizationBroker.addConnection",
        "TransportHandshaker", "SSLEngineImpl.wrap", "SSLEngineImpl.unwrap", "DiscoveryAuth.sign", "DiscoveryAuth.verify")
DUR = re.compile(r"^([0-9.,]+) (ns|us|µs|ms|s|m)$")
UNIT = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0}


def events(tool, path, names):
    p = subprocess.Popen([tool, "print", "--events", ",".join(names), "--stack-depth", "64", path],
                         stdout=subprocess.PIPE, text=True, errors="replace")
    ev = None
    in_stack = False
    for line in p.stdout:
        line = line.rstrip("\n")
        if ev is None:
            if line.startswith("jdk.") and line.endswith("{"):
                ev = {"type": line[:-2].strip(), "stack": []}
            continue
        if line == "}":
            yield ev
            ev, in_stack = None, False
            continue
        s = line.strip()
        if in_stack:
            if s == "]":
                in_stack = False
            elif s and s != "...":
                ev["stack"].append(re.sub(r"\s+line: \d+$", "", s))
            continue
        if s.startswith("stackTrace = ["):
            in_stack = True
            continue
        if " = " in s:
            k, v = s.split(" = ", 1)
            ev[k] = v
    p.wait()


def secs(v):
    m = DUR.match(v or "")
    return float(m.group(1).replace(",", "")) * UNIT[m.group(2)] if m else 0.0


def short(frame):
    f = re.sub(r"\(.*$", "", frame)
    parts = f.split(".")
    return ".".join(parts[-2:]) if len(parts) > 2 else f


def kind(thread):
    t = re.sub(r"\s*\(javaThreadId = \d+.*$", "", thread or "?").strip('"')
    return re.sub(r"[0-9]+", "N", t)[:48]


def tod(v):
    m = re.match(r"(\d+):(\d+):(\d+)", v or "")
    return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3)) if m else 0


def first_cresco(stack):
    for f in stack:
        if f.startswith("io.cresco."):
            return short(f)
    return short(stack[0]) if stack else "?"


def main():
    tool, path = sys.argv[1], sys.argv[2]
    bucket = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    leaf, incl, by_kind = collections.Counter(), collections.Counter(), collections.Counter()
    kind_incl = collections.defaultdict(collections.Counter)
    buckets = collections.defaultdict(collections.Counter)
    n = 0
    for e in events(tool, path, ["jdk.ExecutionSample"]):
        st = e["stack"]
        if not st:
            continue
        n += 1
        leaf[short(st[0])] += 1
        k = kind(e.get("sampledThread"))
        by_kind[k] += 1
        seen = set()
        for f in st:
            if f.startswith("io.cresco."):
                seen.add(short(f))
            else:
                for j in JOIN:
                    if j in f:
                        seen.add(j)
        for s in seen:
            incl[s] += 1
            kind_incl[k][s] += 1
        b = tod(e.get("startTime")) // bucket * bucket
        buckets[b]["all"] += 1
        for j in JOIN:
            if j in seen:
                buckets[b][j] += 1
    print("== samples: %d" % n)
    if n:
        print("-- top leaf frames")
        for f, c in leaf.most_common(25):
            print("  %5.1f %%  %s" % (100.0 * c / n, f))
        print("-- io.cresco frames and join-path frames a sample passes through (inclusive)")
        for f, c in incl.most_common(45):
            print("  %5.1f %%  %s" % (100.0 * c / n, f))
        print("-- by thread kind (share of samples; its top inclusive frames)")
        for k, c in by_kind.most_common(12):
            print("  %5.1f %%  %s : %s" % (100.0 * c / n, k, ", ".join("%s %.0f%%" % (f, 100.0 * x / c)
                                                                    for f, x in kind_incl[k].most_common(4))))
        print("-- per %d s: samples, and the share inside each join-path frame" % bucket)
        cols = [j for j in JOIN if any(buckets[b][j] for b in buckets)]
        print("  time      samples  " + "  ".join(c.split(".")[-1][:14] for c in cols))
        for b in sorted(buckets):
            print("  %02d:%02d:%02d  %7d  %s" % (b // 3600, b % 3600 // 60, b % 60, buckets[b]["all"],
                                             "  ".join("%13.1f%%" % (100.0 * buckets[b][c] / buckets[b]["all"]) for c in cols)))
    cpu, thr = [], collections.Counter()
    for e in events(tool, path, ["jdk.CPULoad", "jdk.ThreadCPULoad"]):
        def pc(k):
            try:
                return float(e.get(k, "0").rstrip("%").replace(",", ""))
            except ValueError:
                return 0.0
        if e["type"] == "jdk.CPULoad":
            cpu.append((pc("jvmUser") + pc("jvmSystem"), pc("machineTotal")))
        else:
            thr[kind(e.get("eventThread"))] += pc("user") + pc("system")
    if cpu:
        j = sorted(x[0] for x in cpu)
        m = sorted(x[1] for x in cpu)
        print("== CPU (%% of all the host's cpus): JVM mean %.1f p90 %.1f max %.1f; machine mean %.1f p90 %.1f max %.1f" % (
            sum(j) / len(j), j[int(0.9 * len(j))], j[-1], sum(m) / len(m), m[int(0.9 * len(m))], m[-1]))
        tot = sum(thr.values()) or 1.0
        print("-- thread kinds by CPU (share of the JVM's thread CPU)")
        for k, c in thr.most_common(12):
            print("  %5.1f %%  %s" % (100.0 * c / tot, k))
    blocked, bn = collections.Counter(), collections.Counter()
    for e in events(tool, path, ["jdk.JavaMonitorEnter"]):
        k = "%s @ %s" % (re.sub(r"\s*\(classLoader.*$", "", e.get("monitorClass", "?")), first_cresco(e["stack"]))
        blocked[k] += secs(e.get("duration"))
        bn[k] += 1
    print("== blocked on a monitor (waits of 10 ms and more): %.1f s in %d waits" % (sum(blocked.values()), sum(bn.values())))
    for k, c in blocked.most_common(12):
        print("  %8.1f s  %6d  %s" % (c, bn[k], k))
    disk, dn = collections.Counter(), collections.Counter()
    for e in events(tool, path, ["jdk.FileForce", "jdk.FileWrite"]):
        k = "%s @ %s" % (e["type"][4:], first_cresco(e["stack"]))
        disk[k] += secs(e.get("duration"))
        dn[k] += 1
    print("== disk (writes and syncs of 10 ms and more): %.1f s in %d" % (sum(disk.values()), sum(dn.values())))
    for k, c in disk.most_common(10):
        print("  %8.1f s  %6d  %s" % (c, dn[k], k))


if __name__ == "__main__":
    main()
