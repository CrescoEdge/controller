#!/usr/bin/env python3
"""
CEP validation — proves Siddhi 5.1.33 + every bundled execution/map extension loads and
compiles inside the live agent.

Each case issues a `cepadd` (CONFIG -> AgentExecutor.cepAdd -> DataPlaneService.createCEP ->
SiddhiManager.createSiddhiAppRuntime). A returned cepid (status_code 10) means Siddhi PARSED
the query, RESOLVED every extension namespace it references, and STARTED the runtime — i.e.
that extension is present in the uber-bundle. status_code 9 carries Siddhi's own error text,
which distinguishes "no extension named X" from a mere query-syntax problem.

Emits RESULT: lines for the shell harness to grep.
"""
import os
import sys
import uuid

sys.path.insert(0, "/Users/cody/code/cresco/code/pycrescolib")
from pycrescolib.clientlib import clientlib

HOST = os.environ.get("CRESCO_HOST", "localhost")
PORT = int(os.environ.get("CRESCO_PORT", "8282"))
KEY = os.environ.get("CRESCO_KEY", "test-service-key-0001")
DST_REGION = os.environ.get("CEP_REGION", "global-region")
DST_AGENT = os.environ.get("CEP_AGENT", "global-controller")

# (name, input_desc, output_desc, query-template with {IN}/{OUT})
CASES = [
    ("core_window_agg",
     "symbol string, price double, volume int",
     "symbol string, avgPrice double, cnt long",
     "from {IN}#window.length(3) select symbol, avg(price) as avgPrice, count() as cnt "
     "group by symbol insert into {OUT};"),

    ("core_filter_having",
     "symbol string, price double",
     "symbol string, price double",
     "from {IN}[price > 100.0] select symbol, price insert into {OUT};"),

    ("ext_math",
     "symbol string, price double",
     "symbol string, root double",
     "from {IN} select symbol, math:sqrt(price) as root insert into {OUT};"),

    ("ext_string",
     "symbol string, price double",
     "tag string",
     "from {IN} select str:concat(symbol, '_ok') as tag insert into {OUT};"),

    ("ext_time",
     "symbol string, price double",
     "symbol string, ts long",
     "from {IN} select symbol, time:timestampInMilliseconds() as ts insert into {OUT};"),

    ("ext_regex",
     "symbol string, price double",
     "matched bool",
     "from {IN} select regex:find('[A-Z]+', symbol) as matched insert into {OUT};"),

    ("ext_map",
     "symbol string, price double",
     "m object",
     "from {IN} select map:createFromJSON('{\"k\":\"v\"}') as m insert into {OUT};"),

    ("ext_json",
     "symbol string, price double",
     "p double",
     "from {IN} select json:getDouble('{\"p\":1.5}', '$.p') as p insert into {OUT};"),

    ("ext_unique",
     "symbol string, price double",
     "symbol string, price double",
     "from {IN}#window.unique:length(symbol, 5) select symbol, price insert into {OUT};"),

    ("ext_reorder",
     "symbol string, price double, ts long",
     "symbol string, price double",
     "from {IN}#reorder:kslack(ts) select symbol, price insert into {OUT};"),
]


def run_case(name, in_desc, out_desc, qt):
    """Fresh client per case so RPC replies cannot misalign under rapid fire."""
    sid = uuid.uuid4().hex[:8]
    in_s, out_s = f"In_{name}_{sid}", f"Out_{name}_{sid}"
    query = qt.replace("{IN}", in_s).replace("{OUT}", out_s)  # .replace, not .format: queries embed JSON braces
    client = clientlib(HOST, PORT, KEY)
    if not client.connect():
        return (name, "CONNFAIL", "", query)
    try:
        reply = client.agents.cepadd(in_s, in_desc, out_s, out_desc, query,
                                     DST_REGION, DST_AGENT)
    except Exception as e:
        try: client.close()
        except Exception: pass
        return (name, "EXC", str(e), query)
    try: client.close()
    except Exception: pass
    code = str((reply or {}).get("status_code", ""))
    cepid = (reply or {}).get("cepid", "")
    if code == "10" and cepid:
        return (name, "PASS", cepid, query)
    err = ((reply or {}).get("status_desc", "") + " | " + (reply or {}).get("error", "")).strip()
    return (name, "FAIL", f"code={code} :: {err}", query)


def main():
    import time
    print(f"target {DST_REGION}/{DST_AGENT}")
    import traceback
    passed = 0
    for case in CASES:
        try:
            name, status, detail, query = run_case(*case)
        except Exception:
            print(f"RESULT: {case[0]} CRASH")
            traceback.print_exc()
            time.sleep(1.0)
            continue
        if status == "PASS":
            passed += 1
            print(f"RESULT: {name} PASS cepid={detail}")
        else:
            print(f"RESULT: {name} {status} {detail}")
            print(f"        query: {query}")
        time.sleep(1.0)
    print(f"SUMMARY: {passed}/{len(CASES)} CEP cases created")
    return 0 if passed == len(CASES) else 2


if __name__ == "__main__":
    sys.exit(main())
