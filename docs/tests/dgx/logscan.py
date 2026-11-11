#!/usr/bin/env python3
"""What the nodes of one gate build logged: ERROR lines, refusal and loss lines, the calls authority_direction would
refuse, and dropped messages by cause. Usage: logscan.py <logs dir> [<stop file>]  -> one JSON object on stdout (lines later than the stop file's time are not read).

A refusal or loss line is a line that reports something refused, discarded or lost. Not one: the line a node writes at
start to say which authority_direction it runs (it names what enforce WOULD refuse; counted as a refusal by the rule
before 2026-11-11, one per node, which failed the gate of agent release 1.3.377 on nothing). The calls the rule would
refuse (AUTHORITY: WOULD REFUSE <TYPE> ...) are counted on their own, and so are dropped messages (MESSAGE DROPPED),
by cause: nodes that are stopped at the end of a build drop what they cannot send any more (send_failed,
controller_stopping, node_stopping), in the earlier build and the candidate alike.
"""
import datetime
import json
import os
import re
import sys

REFUSAL = re.compile(r"REFUS|does not verify|SIGNATURE: REFUSED|Discovery REFUSED|DISCARDED|overloaded|dropped [0-9]+ frame")
MODE_NOTICE = re.compile(r"authority_direction=(log|off|enforce)\b.*\b(default|ALLOWED|not looked at|Set authority_direction)")
WOULD_REFUSE = re.compile(r"AUTHORITY: WOULD REFUSE [A-Z]")
DROPPED = re.compile(r"MESSAGE DROPPED: .* of this kind, ([a-z_]+);")
HEAD = re.compile(r"^\S+ \S+ \S+ +\w+ +\[[^]]*\] - ")


STAMP = re.compile(r"^(\d{1,2} \w{3} \d{4} \d{2}:\d{2}:\d{2}),")


def scan(d, until=None):
    """until (a datetime in the nodes' local time): lines stamped later are not read. gate.sh scans before it stops the
    nodes; a scan made afterwards would count what they log while they are being stopped."""
    errs, refusals, lines, dropped = {}, {}, {}, {}
    would = 0
    for f in sorted(os.listdir(d)):
        n = f[:-4]
        late = False
        for l in open(os.path.join(d, f), errors="replace"):
            if until is not None:
                m = STAMP.match(l)
                if m:
                    try:
                        late = datetime.datetime.strptime(m.group(1), "%d %b %Y %H:%M:%S") > until
                    except ValueError:
                        pass
                if late:                                    # and its continuation lines (a stack trace)
                    continue
            if " ERROR " in l:
                errs[n] = errs.get(n, 0) + 1
                k = HEAD.sub("", l.strip())[:160]
                lines[k] = lines.get(k, 0) + 1
            m = DROPPED.search(l)
            if m:
                dropped[m.group(1)] = dropped.get(m.group(1), 0) + 1
            if WOULD_REFUSE.search(l):
                would += 1
                k = "WOULD REFUSE " + re.sub(r"^.*AUTHORITY: WOULD REFUSE ", "", l.strip())[:160]
                lines[k] = lines.get(k, 0) + 1
            elif REFUSAL.search(l) and not MODE_NOTICE.search(l):
                refusals[n] = refusals.get(n, 0) + 1
                k = "REFUSAL/LOSS " + HEAD.sub("", l.strip())[:160]
                lines[k] = lines.get(k, 0) + 1
    return {"error_lines": sum(errs.values()), "refusal_or_loss_lines": sum(refusals.values()), "would_refuse_lines": would,
            "dropped": dropped, "by_node_errors": errs, "by_node_refusals": refusals,
            "top": sorted(lines.items(), key=lambda x: -x[1])[:15]}


if __name__ == "__main__":
    # logscan.py <logs dir> [<file whose modification time ends the scan>]
    stop = sys.argv[2] if len(sys.argv) > 2 and os.path.exists(sys.argv[2]) else None
    print(json.dumps(scan(sys.argv[1], datetime.datetime.fromtimestamp(os.path.getmtime(stop)) if stop else None)))
