#!/usr/bin/env python3
"""Capability drift check: for each annotated executor, assert its switch cases (the real dispatch) and
its @CrescoAction names (the self-description) match. Catches an action added to the switch but not
described, or described but not dispatched -- so the LLM tool catalog can never silently lie.

Source-scans the executor .java files (no build needed). Run standalone: `python3 capability_drift.py`.
"""
import re
import sys
import os

CODE = os.environ.get("CRESCO_CODE", "/Users/cody/code/cresco/code")

# executor source files
FILES = [
    "controller/src/main/java/io/cresco/agent/controller/agentcontroller/AgentExecutor.java",
    "controller/src/main/java/io/cresco/agent/controller/regionalcontroller/RegionalExecutor.java",
    "controller/src/main/java/io/cresco/agent/controller/globalcontroller/GlobalExecutor.java",
    "stunnel/src/main/java/io/cresco/stunnel/PluginExecutor.java",
    "sysinfo/src/main/java/io/cresco/sysinfo/ExecutorImpl.java",
    "wsapi/src/main/java/io/cresco/wsapi/PluginExecutor.java",
    "repo/src/main/java/io/cresco/repo/ExecutorImpl.java",
]

# switch cases that are NOT message actions (MsgEvent.Type routing inside RegionalExecutor, etc.)
IGNORE_CASES = {"CONFIG", "DISCOVER", "ERROR", "INFO", "EXEC", "WATCHDOG", "KPI", "GC", "LOG"}

CASE_RE = re.compile(r'case\s+"([^"]+)"\s*:')
# some handlers dispatch via an if instead of a switch case, e.g. "nettuning".equals(getParam("action"))
IF_RE = re.compile(r'"([^"]+)"\.equals\((?:incoming|ce|msg)\.getParam\("action"\)\)')
ACTION_RE = re.compile(r'@CrescoAction\(\s*name\s*=\s*"([^"]+)"')


def scan(path):
    with open(path) as f:
        src = f.read()
    cases = {c for c in CASE_RE.findall(src) if c not in IGNORE_CASES}
    cases |= set(IF_RE.findall(src))   # include if-style dispatch
    actions = set(ACTION_RE.findall(src))
    return cases, actions


def main():
    total_drift = 0
    for rel in FILES:
        path = os.path.join(CODE, rel)
        if not os.path.exists(path):
            print(f"SKIP  {rel} (not found)")
            continue
        cases, actions = scan(path)
        missing_annotation = sorted(cases - actions)   # dispatched but not described
        missing_case = sorted(actions - cases)         # described but not dispatched
        name = rel.split("/")[-1]
        if not missing_annotation and not missing_case:
            print(f"PASS  {name}: {len(cases)} actions, switch<->annotation aligned")
        else:
            total_drift += len(missing_annotation) + len(missing_case)
            print(f"FAIL  {name}: dispatched-but-undescribed={missing_annotation} described-but-undispatched={missing_case}")
    print("===================================================")
    if total_drift == 0:
        print("  CAPABILITY DRIFT: PASS (all switch cases match annotations)")
        return 0
    print(f"  CAPABILITY DRIFT: FAIL ({total_drift} mismatches)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
