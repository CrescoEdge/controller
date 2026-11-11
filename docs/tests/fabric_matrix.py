#!/usr/bin/env python3
"""Fabric matrix: one cell (a topology under a security profile) of a live Cresco fabric, through lifecycle events.

WHAT IT IS
  The defects the unit suite misses live in a combination: a topology, a security profile and a lifecycle event. This
  starts a real fabric (one JVM per node, all from one agent jar, on this host), runs the events in a fixed order and,
  after each one and a settle time, checks three invariants:
    (a) the global's registry lists exactly the nodes that should be up, each in its region;
    (b) one RPC (iscontrolleractive) from here through the global's wsapi to every node's controller is answered (a node
        that moved regions is also asked at its former address);
    (c) nothing a node logged since the event began is outside the KNOWN list below: `MESSAGE SIGNATURE: REFUSED`,
        `MESSAGE DROPPED`, `STRIPPED a forged caller`, `WITHHELD from a bridge`, `DISCARDED a message a bridge`, ERROR
        lines, `timer task ... failed`, `REFUSING TO START`.
  (c) is the point: a fabric can answer every question it is asked while its nodes refuse hundreds of messages. A cell
  fails on anything that is not a known item, or on a known item over its bound. It prints one line per event, a
  PASS/FAIL table, the known items it saw with their counts, and exits 0 only when every event passed.

HOW TO RUN IT
  PYTHONPATH=<pycrescolib checkout> python3 fabric_matrix.py --jar agent-1.3-SNAPSHOT.jar --work /tmp/fm
      [--topology tree] [--profile standard] [--events start,restart-region,...] [--old-jar earlier-agent.jar]
      [--base-port 41000] [--settle 30] [--seed 1] [--keep] [--json out.json]
  Needs a JDK 21 `java` on PATH, Python 3.12 with pycrescolib, and (production profile) OpenSSL 3 for tools/cresco-pki.sh.
  One cell per run; about 12 minutes for the 10 JVMs of `tree` with every default event (4.5 of them settle time), ~4 GB
  of RAM and 1.5 GB of disk, which is removed at the end unless --keep (logs stay in <work>/logs). A cell whose fabric
  does not form at `start` stops there. It starts and stops only
  its own JVMs, by PID (also when it fails or is interrupted; a run that was killed outright is cleaned up by the next
  run in the same --work), uses only ports base .. base+400, makes its secrets per run in owner-only files and never
  prints them, and refuses to run with less than 12 GB of free disk.

  Topologies  single     a global and one agent
              tree       a global, 3 regions bridged to the global only, 2 agents per region
              mesh       the tree, every region also bridged to the others (regional_peers)
              tenants    two tenants, each its own region and 2 agents, under a global in the superuser tenant
                         (regions are given the global's key in controller_trust_anchors; standard profile only).
                         The release of 2026-11-10 does not form it: a tenant's region never registers with the global.
              alias      the tree, plus one agent that starts in the global's region and is moved to a region with
                         region_alias (it must keep answering at its former address through every event)
              regions30  a global and 30 regions, no agents, no plugins: a scale smoke for a bigger machine
                         (regions<N> for another count); never part of a default run
  Profiles    standard   secure defaults: nothing set but the discovery secrets, the service key and names
              production pre-provisioned identities (tools/cresco-pki.sh), security_profile=production, no shared secret
              upgrade    standard, the upgrade window open on the controllers, one agent on --old-jar
  Events, in this order (--events picks some; `start` always runs):
              start          every node, in an order shuffled by --seed (agents before their controllers too)
              restart-agent  kill -9 and start of one agent
              restart-region kill -9 and start of one region controller; its agents come back by themselves
              restart-global kill -9, 20 s, start
              global-outage  the global down 60 s; meanwhile one agent starts under a region and one is killed
              rekey-region   a region controller is stopped, loses its identity (security/node-identity.p12) and starts
                             with a new key; the operator's part is done exactly where a refusing node's log asks for
                             it ("authorise ONE rebind on this node: add the line '<region>/<agent>=<key>' to <its
                             security/peer-trust.json.rebind>"), with the key the re-keyed node printed. Its agents
                             must come back by themselves, and no message may be refused: they sign for their
                             controller with their certificate, drop the attestation its former key issued when they
                             see its new key, and are attested anew on their first ping (ReKeyedParentTest). The
                             release of 2026-11-10 failed it: the agents signed that ping with the former key's
                             attestation, which the re-keyed controller refused ("the attestation's signature does not
                             verify") for ever, until they were restarted; it ran only when named until that was fixed.
                             Nodes that still do not answer are restarted after the event is judged, so later events
                             run on a whole fabric. Production: the node is re-issued (cresco-pki.sh node), no rebind;
                             the controller is back within the agents' reconnect time, so their pings stay unsigned on
                             the direct link and are accepted first hand (the release passes that; the signed ping of
                             an agent that had to join again is what it refused).
              move-agent     an agent of the global's region is stopped and started in a region with region_alias
              cut-bridge     one region controller is SIGSTOPped for 40 s
              clock-skew     LEFT OUT: nothing supported gives one JVM another clock (the event reports that)
              revoke         one agent's key is revoked at the global (standard: its fingerprint in the global's
                             security/peer-trust.json.revoke; production: cresco-pki.sh revoke + crl). Its controller
                             must close it within REVOKE_CLOSED_S, the global must drop it within REVOKE_UNLISTED_S,
                             and the same key must be refused under another controller.

WHAT IT CANNOT SEE
  A node logs a refusal, a drop or a strip at most every 10 s, with a running count: the harness judges the reason of
  every logged line and the count the line accounts for, so the tail of a burst is counted only by the next line of
  that node, and a second reason inside the same 10 s is not named (builds from controller 983407f on log each reason
  on its own and say "K of this kind", which is used when present). Those builds also expose the counts in the metric
  inventory (msg.refused.<reason>, msg.dropped.<cause>, broker.caller.stripped, ...): observe_counters reads them with
  one RPC, and what they gained per event and the conservation of messages (originated = delivered + refused + dropped,
  summed over the nodes) are REPORTED, not judged -- they show more than the log does (12 refusals of one burst where
  the log has one line). Every observation is one function (observe_*): moving the judgement from the log to the
  counters is replacing those. An earlier-build agent (upgrade profile) keeps its membership and nothing more: it must
  be listed, it is not asked an RPC, and its own log is not judged; nor is a revoked node's after its revocation.

ON THE RELEASE OF 2026-11-10 (agent 1.3, controller 29ef93d), 2026-11-11
  single, tree (standard, production, upgrade) pass. mesh fails restart-global and global-outage (after the global
  restarts, messages for controller inboxes and agent.event arrive over another region's bridge and lose their caller
  stamp); alias fails restart-region (the moved agent's failover dials the global's broker, which its region's broker
  advertised, and logs PEER IDENTITY MISMATCH: refused, as it must be); tenants does not form; rekey-region fails. None
  of these has a known-item entry: each is for its owner to fix or to list. The release before it (tree) fails six of
  eight events: credentials that do not verify at start, a minute of unsigned-message refusals at a moved agent.
  SINCE THEN: rekey-region is fixed in the controller (message signing: see the event above) and is part of the default
  order; tree passes it in standard and in production with no message refused at any node, and the agents hold an
  attestation of the new key 0.1 s after the operator's rebind is taken ("attested by <controller> (its key ...)").

HOW TO ADD
  A topology    a function that adds nodes to the fabric (the global first: fab.add / fab.add_agent), listed in TOPOLOGIES.
  An event      a function that does one thing to the fabric and returns a sentence (raise Skip when it does not apply;
                append to fab.event_failures what it found wrong itself), listed in EVENTS in the place it should run.
  A known item  ONE Known(...) entry in KNOWN: the kind, a regular expression, the events it may follow, an upper
                bound per node per event (per minute of the event for a rate) and the reason in one line. Measure the
                bound over two runs at least; say so when you widen one. An entry is for something understood that a
                plan removes later, not for making a run pass.
"""
from __future__ import annotations

import argparse
import atexit
import dataclasses
import datetime
import json
import logging
import os
import random
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PKI_TOOL = HERE.parent.parent / "tools" / "cresco-pki.sh"
HOST = "127.0.0.1"
MIN_FREE_GB = 12            # refuse to start (or to add a node) with less free disk than this in the work directory
SLOT = 4                    # ports per node: +0 discovery, +1 broker, +2 wsapi
MAX_SLOTS = 100             # the port block is base .. base + SLOT * MAX_SLOTS
COMMON = ["-Djava.net.preferIPv4Stack=true", "-Droot_log_level=INFO", "-Denable_console=false", "-Denable_dashboard=false"]
LIGHT = ["-Denable_stunnel=false", "-Denable_sysinfo=false", "-Denable_executor=false", "-Denable_filerepo=false",
         "-Denable_repo=false"]


# ====================================================================================================================
# Known items: what is non-zero today for an understood reason. ONE entry each. Anything a node logs in a watched kind
# that matches no entry (for the event it followed), or exceeds an entry's bound, fails the cell.
#   kind     what was observed: refused | stripped | withheld | discarded | error | timer | refused-start
#   pattern  a regular expression searched in the log line
#   events   the events it may follow ("*": any)
#   bound    per node, per event: refusals / strips the lines account for (kinds with a counter), or lines (the others)
#   reason   one line
# ====================================================================================================================
@dataclasses.dataclass(frozen=True)
class Known:
    id: str
    kind: str
    pattern: str
    events: tuple
    bound: int
    reason: str
    per_minute: bool = False     # the bound is a rate: it scales with the length of the event's window


# Events during which some node has lost, or is finding again, the controller above it.
PARENT_AWAY = ("start", "restart-region", "restart-global", "global-outage", "rekey-region", "cut-bridge")
RESTARTS = ("restart-agent", "restart-region", "restart-global", "global-outage", "rekey-region", "move-agent", "cut-bridge", "revoke")

KNOWN: list[Known] = [
    # ---- messages refused
    Known("refused-lost-session", "refused", r"a message under a session key this node does not hold", RESTARTS, 3,
          "after a node restarts, its peers' messages under the session it lost are refused until they notice and sign again"),
    Known("refused-stale-unsigned", "refused", r"an unsigned message \(from null, not delivered first hand\)",
          ("restart-region", "restart-global", "global-outage", "cut-bridge"), 2,
          "one stale unsigned ping or WATCHDOG reaches a parent that restarted"),
    # ---- messages dropped (logged by builds from controller 983407f on; the cause is the last word before the ';')
    Known("dropped-no-sender", "dropped", r"of this kind, no_sender;", ("*",), 2,
          "a node asked for an answer in its first moments (or while it joins again) has no sender yet"),
    Known("dropped-no-global", "dropped", r"of this kind, no_global;", ("restart-global", "global-outage"), 4,
          "a region's ping for the global is dropped while it has joined none"),
    Known("dropped-send-failed", "dropped", r"of this kind, send_failed;", PARENT_AWAY, 5,
          "an agent's watchdog or ping cannot be sent while its controller is away"),
    Known("dropped-controller-stopping", "dropped", r"of this kind, controller_stopping;", ("rekey-region", "move-agent", "revoke"), 3,
          "a node the event stops drops what it was about to send (seen by the counters' author at shutdown, not in these runs)"),
    Known("dropped-plugin-inactive", "dropped", r"of this kind, plugin_inactive;", ("*",), 2,
          "a message reaches a node whose plugin is still loading (expected by the counters' author, not seen in these runs)"),
    # ---- caller stamps stripped
    Known("stripped-mesh-adverts", "stripped",
          r"global\.event' .* a node of region \S+ over the bridge of region (?!glob\b)\S+ \(outside its subtree\)", ("*",), 200,
          "in a region mesh, a controller's link-state advert that arrives over a second bridge loses its caller stamp", per_minute=True),
    Known("stripped-after-global-restart", "stripped",
          r"global\.event' .* a node of region \S+ over the bridge of region glob \(outside its subtree\)",
          ("restart-global", "global-outage"), 3,
          "right after the global restarts, a region strips another region's advert the global carried before its new bundle arrived"),
    # ---- ERROR lines that are the report of the event itself
    Known("error-parent-discovery", "error", r"Discovery Failed: Connection refused|No agents discovered\.", PARENT_AWAY, 25,
          "an agent retries discovery (two lines every ~3 s) while its controller's port does not answer", per_minute=True),
    Known("error-global-discovery", "error", r"exchangeKeyWithBroker\(\) Connection refused|No global controller discovered\.",
          PARENT_AWAY, 25, "a region retries discovery (two lines every ~3 s) while the global's port does not answer", per_minute=True),
    Known("error-peer-discovery", "error", r"Could not discover or connect to peer", ("start", "restart-region", "rekey-region", "cut-bridge"), 3,
          "in a region mesh, a region reports a configured peer (regional_peers) that does not answer while the event has it down"),
    Known("error-global-lost", "error", r"GLOBAL CONTROLLER LOST", ("restart-global", "global-outage", "cut-bridge"), 2,
          "a region reports that it lost the global"),
    Known("error-region-lost", "error", r"REGIONAL CONTROLLER LOST|HC bridge: link:parent CRITICAL in AGENT -> regionalControllerLost",
          PARENT_AWAY, 4, "an agent reports that it lost its controller"),
    Known("error-send-failed", "error", r"Control-plane send failed to \[[^\]]+\] for (WATCHDOG|EXEC) message", PARENT_AWAY, 5,
          "an agent's ping or watchdog cannot be sent while its controller is away"),
    Known("error-dataplane-restore", "error", r"DataPlaneServiceImpl\] (Restoring DataPlane|Removing old listenerId)", PARENT_AWAY, 4,
          "an agent that found its controller again rebuilds its dataplane listeners (logged at ERROR)"),
    Known("error-producer-null", "error", r"AgentProducer is null\. Message not sent", ("*",), 2,
          "a node asked for an answer in its first moments (or while it joins again) has no sender yet"),
    Known("error-inactive-plugin", "error", r"MESSAGE SENT TO INACTIVE PLUGIN", ("*",), 2,
          "a message reaches a node whose plugin is still loading (on the owner's list of known lines, not seen in these runs)"),
    Known("error-agent-lost", "error", r"Agent NodeID: \S+ (is STALE\. Setting to LOST|lost\. Setting Plugin NodeID: \S+ to LOST)",
          ("global-outage", "revoke"), 7, "a controller reports an agent the event killed or revoked as lost (one line, and one per plugin)"),
    Known("error-upgrade-window", "error", r"UPGRADE WINDOW OPEN until", ("*",), 3,
          "a controller with the upgrade window open says so at ERROR at every start, and hourly (upgrade profile)"),
    # ---- revoke: the report of the revocation
    Known("error-revoked", "error", r"Key [0-9a-f]+ (of \S+ )?REVOKED \(operator file|Merged the revocation list of \S+: \d+ key\(s\) newly REVOKED",
          ("revoke",), 2, "every node reports the key the operator revoked (at the global) or that reached it in a list"),
    Known("error-revoked-refused", "error",
          r"Discovery REFUSED \S+: the key [0-9a-f]+ is REVOKED|REFUSED the hello of \S+ from \S+ the certificate is REVOKED", ("revoke",), 15,
          "a controller refuses the revoked node each time it tries to join (every ~3 s)", per_minute=True),
    # ---- rekey-region: the report of the re-key and of the operator's rebind
    Known("error-rekey", "error", r"Discovery REFUSED \S+: the name is bound to another key|Identity \S+ REBOUND from key"
          r"|discovery v2: could not record|REFUSED this node: refused by|certDiscovery\.size\(\) == 0|CERT DISCOVERY != 1",
          ("rekey-region",), 12, "a re-keyed controller is refused until the operator's rebind, and the rebind is reported"),
    Known("error-rekey-connect", "error", r"Failed to connect to \[nio\+ssl://[^\]]+\] after: \d+ attempt\(s\)|createDedicatedSession error for URI",
          ("rekey-region",), 2, "an agent's connection gives up (5 attempts, 25 s) on its controller, down and then back with a key the agent"
          " refuses until the operator's rebind: one line of the transport, one of the client (both at one agent in 3 runs of 4, none in the 4th)"),
]


# ====================================================================================================================
# The fabric: nodes, their JVMs, the API client
# ====================================================================================================================
class Skip(Exception):
    """An event that does not apply to this cell (the reason is reported)."""


class Fatal(Exception):
    """The harness cannot go on (a port in use, a tool that failed, no disk)."""


@dataclasses.dataclass
class Node:
    name: str                     # the agent name, unique in the fabric
    role: str                     # global | region | agent
    region: str
    slot: int
    parent: "Node | None" = None
    tenant: str = ""              # "": the profile's tenant
    jar: str = ""
    alias: str = ""               # region_alias: the region it moved from
    extra: list = dataclasses.field(default_factory=list)
    old_build: bool = False
    up: bool = False              # the harness expects it up (listed by the global, answering)
    proc: "subprocess.Popen | None" = None
    launches: int = 0
    mark: int = 0                 # log offset where the current event's window starts
    last: dict = dataclasses.field(default_factory=dict)   # kind -> the last counter value seen (this JVM)
    unjudged: str = ""            # why this node's log is not judged any more (a revoked node)

    @property
    def path(self) -> str:
        return "%s/%s" % (self.region, self.name)


class Fabric:
    def __init__(self, a, profile):
        self.a = a
        self.work = Path(a.work).resolve()
        self.profile = profile
        self.nodes: dict[str, Node] = {}
        self.rng = random.Random(a.seed)
        self.slots = 0
        self.infer = True             # regions may self-organize direct bridges (the shipped default)
        self.mesh = False
        self.anchor_global = False    # regions get the global's key in controller_trust_anchors (start: the global first)
        self.setup_events: list[str] = []   # events a topology runs right after `start`, whatever --events says
        self._client = None
        self.seq = 0                  # names of nodes added by events
        self.after_check = None       # set by an event: called with its result once the invariants are judged
        self.event_failures: list[str] = []   # what an event itself found wrong (beside the invariants)
        self.prev_counters: dict = {}  # the metric inventory's counters at the event before, and the JVMs they were of
        self.prev_launches: dict = {}
        self.revoked = None           # what the revoke event measured

    # ---- layout ----
    def disc(self, n): return self.a.base_port + SLOT * n.slot
    def broker(self, n): return self.a.base_port + SLOT * n.slot + 1
    def ws(self, n): return self.a.base_port + SLOT * n.slot + 2
    def dir(self, n) -> Path: return self.work / "nodes" / n.name
    def data(self, n) -> Path: return self.dir(n) / "cresco-data"
    def log(self, n) -> Path: return self.work / "logs" / (n.name + ".log")

    @property
    def glob(self) -> Node:
        return next(n for n in self.nodes.values() if n.role == "global")

    def regions(self): return [n for n in self.nodes.values() if n.role == "region"]
    def agents(self, parent=None, up=True):
        return [n for n in self.nodes.values() if n.role == "agent" and (parent is None or n.parent is parent)
                and (not up or n.up)]

    def add(self, name, role, region, parent=None, tenant="", **kw) -> Node:
        if name in self.nodes:
            raise Fatal("node name %s used twice" % name)
        if self.slots >= MAX_SLOTS:
            raise Fatal("out of ports in the block")
        n = Node(name=name, role=role, region=region, slot=self.slots, parent=parent, tenant=tenant, jar=self.a.jar, **kw)
        self.slots += 1
        self.nodes[name] = n
        return n

    def add_agent(self, name, parent, **kw) -> Node:
        return self.add(name, "agent", parent.region, parent=parent, tenant=parent.tenant, **kw)

    # ---- JVMs ----
    def flags(self, n) -> list:
        f = ["-Dregionname=" + n.region, "-Dagentname=" + n.name, "-Dnetdiscoveryport=%d" % self.disc(n)]
        if n.role == "global":
            f += ["-Dis_global=true", "-Dbroker_port=%d" % self.broker(n), "-Ddiscovery_port=%d" % self.broker(n),
                  "-Denable_wsapi=true", "-Dwsapi_port=%d" % self.ws(n)]
        elif n.role == "region":
            g = self.glob
            f += ["-Dis_region=true", "-Dglobal_controller_host=" + HOST, "-Dglobal_controller_port=%d" % self.disc(g),
                  "-Dbroker_port=%d" % self.broker(n), "-Ddiscovery_port_remote=%d" % self.broker(g), "-Denable_wsapi=false"]
            if self.mesh:
                f.append("-Dregional_peers=" + ",".join("%s:%d" % (HOST, self.disc(r)) for r in self.regions() if r is not n))
            if not self.infer:
                f.append("-Dnet_infer_connections=false")
        else:
            f += ["-Dis_agent=true", "-Dregional_controller_host=" + HOST, "-Dregional_controller_port=%d" % self.disc(n.parent),
                  "-Denable_wsapi=false"]
            if n.alias:
                f.append("-Dregion_alias=" + n.alias)
        if self.a.light:
            f += LIGHT
        return f

    def launch(self, n):
        free_disk(self.work)
        d = self.dir(n)
        d.mkdir(parents=True, exist_ok=True)
        heap = self.a.agent_heap if n.role == "agent" else self.a.heap
        cmd = ["java"] + COMMON + ["-Xmx" + heap] + self.profile.flags(self, n) + self.flags(n) + n.extra + ["-jar", n.jar]
        n.launches += 1
        with open(self.log(n), "ab") as f:
            f.write(("### fabric_matrix launch %d of %s at %s\n" % (n.launches, n.path, now_iso())).encode())
            f.flush()
            n.proc = subprocess.Popen(cmd, cwd=d, stdout=f, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        n.up = True
        with open(self.work / "pids", "a") as f:
            f.write("%d %s\n" % (n.proc.pid, n.name))

    def alive(self, n) -> bool:
        return n.proc is not None and n.proc.poll() is None

    def kill(self, n):
        """kill -9 (by PID)."""
        if self.alive(n):
            n.proc.send_signal(signal.SIGKILL)
            n.proc.wait()
        n.up = False
        if n.role == "global":
            self.drop_client()

    def stop(self, n, grace=25):
        """SIGTERM, then SIGKILL after `grace` seconds (by PID)."""
        if self.alive(n):
            for s in (signal.SIGCONT, signal.SIGTERM):
                try:
                    n.proc.send_signal(s)
                except ProcessLookupError:
                    pass
            try:
                n.proc.wait(grace)
            except subprocess.TimeoutExpired:
                n.proc.send_signal(signal.SIGKILL)
                n.proc.wait()
        n.up = False
        if n.role == "global":
            self.drop_client()

    def stop_all(self):
        self.drop_client()
        live = [n for n in self.nodes.values() if self.alive(n)]
        for n in live:
            for s in (signal.SIGCONT, signal.SIGTERM):
                try:
                    n.proc.send_signal(s)
                except ProcessLookupError:
                    pass
        end = time.time() + 12
        for n in live:
            try:
                n.proc.wait(max(0.1, end - time.time()))
            except subprocess.TimeoutExpired:
                pass
        for n in live:
            if n.proc.poll() is None:
                n.proc.send_signal(signal.SIGKILL)
                n.proc.wait()

    # ---- the API client (the global's wsapi) ----
    def drop_client(self):
        c, self._client = self._client, None
        if c is not None:
            try:
                c.close()
            except Exception:
                pass

    def ws_pin(self) -> str:
        for f in (self.data(self.glob) / "plugin-data").glob("*/ws.fingerprint"):
            try:
                return f.read_text().split()[0].strip()
            except (OSError, IndexError):
                pass
        return ""

    def api(self):
        """A connected client of the global's wsapi, or None while it cannot be reached."""
        if self._client is not None:
            return self._client
        g = self.glob
        if not self.alive(g) or not port_open(self.ws(g)):
            return None
        pin = self.ws_pin()
        if not pin:
            return None
        from pycrescolib.clientlib import clientlib
        try:
            c = clientlib(HOST, self.ws(g), fingerprint=pin, **self.profile.client_kwargs(self))
            if c.connect():
                self._client = c
                return c
            c.close()
        except Exception:
            pass
        return None

    def listed(self):
        """The nodes the global's registry lists, as region/agent; None when the global cannot be asked."""
        c = self.api()
        if c is None:
            return None
        try:
            return {"%s/%s" % (x.get("region_id"), x.get("agent_id")) for x in c.globalcontroller.get_agent_list()}
        except Exception:
            self.drop_client()
            return None

    def rpc(self, region, agent, timeout=10.0):
        """One RPC to a node's controller through the global's wsapi: (answered, milliseconds, error)."""
        c = self.api()
        if c is None:
            return False, 0.0, "the global's wsapi cannot be reached"
        t = time.perf_counter()
        try:
            r = c.messaging.global_agent_msgevent(True, "EXEC", {"action": "iscontrolleractive"}, region, agent, timeout=timeout)
            ms = (time.perf_counter() - t) * 1000.0
            if str((r or {}).get("is_controller_active", "")).lower() in ("true", "1"):
                return True, ms, ""
            return False, ms, "answer without is_controller_active=true"
        except Exception as e:
            self.drop_client()
            return False, (time.perf_counter() - t) * 1000.0, type(e).__name__

    def expected(self) -> set:
        return {n.path for n in self.nodes.values() if n.up}

    def wait_listed(self, timeout) -> "float | None":
        """Seconds until the global lists exactly the nodes that should be up, or None."""
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.listed() == self.expected():
                return time.time() - t0
            time.sleep(3)
        return None

    def targets(self) -> list:
        """(region, agent) of every node that should answer; a moved node also at its former address. An earlier build
        in the upgrade window keeps its membership and nothing more (docs/upgrade-1.3-secure-defaults.md section 3): it is
        listed, and an API client's RPC is not for it."""
        up = [n for n in self.nodes.values() if n.up and not n.old_build]
        return [(n.region, n.name) for n in up] + [(n.alias, n.name) for n in up if n.alias]

    def wait_converged(self, timeout) -> "float | None":
        """Seconds until the listing is right and every node that should be up answers an RPC, or None."""
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.listed() == self.expected() and all(self.rpc(r, a, timeout=5.0)[0] for r, a in self.targets()):
                return time.time() - t0
            time.sleep(3)
        return None

    # ---- logs ----
    def window(self, n) -> list:
        """The lines node n logged since its mark; the mark moves to what was read."""
        try:
            with open(self.log(n), "rb") as f:
                f.seek(n.mark)
                data = f.read()
        except OSError:
            return []
        cut = data.rfind(b"\n") + 1          # whole lines only
        n.mark += cut
        return data[:cut].decode("utf-8", "replace").splitlines()

    def peek(self, n, start=None) -> list:
        """The lines node n logged since `start` (default: its mark), without moving the mark."""
        try:
            with open(self.log(n), "rb") as f:
                f.seek(n.mark if start is None else start)
                return f.read().decode("utf-8", "replace").splitlines()
        except OSError:
            return []

    def identity_key(self, n) -> str:
        """The key fingerprint node n printed at its last start ("This node's identity key: ..."), or ""."""
        fp = ""
        for l in self.peek(n, 0):
            m = re.search(r"This node's identity key: ([0-9a-f]{64})", l)
            if m:
                fp = m.group(1)
        return fp

    def wait_node(self, n, timeout) -> "float | None":
        """Seconds until the global lists node n and it answers an RPC, or None."""
        t0 = time.time()
        while time.time() - t0 < timeout:
            if n.path in (self.listed() or ()) and self.rpc(n.region, n.name, timeout=5.0)[0]:
                return time.time() - t0
            time.sleep(2)
        return None

    def size(self, n) -> int:
        try:
            return self.log(n).stat().st_size
        except OSError:
            return 0

    def wait_log(self, n, regex, timeout, start=None):
        """The first line node n logged (since `start`, default its mark) matching regex, within timeout; or None."""
        rx = re.compile(regex)
        end = time.time() + timeout
        while True:
            for line in self.peek(n, start):
                if rx.search(line):
                    return line
            if time.time() >= end:
                return None
            time.sleep(1)


def now_iso() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def port_open(port) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=1):
            return True
    except OSError:
        return False


def free_disk(path):
    free = shutil.disk_usage(path).free / 2**30
    if free < MIN_FREE_GB:
        raise Fatal("only %.1f GB free under %s (the harness keeps %d GB free)" % (free, path, MIN_FREE_GB))


def write_secret(path: Path, value: str):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(value + "\n")
    os.chmod(path, 0o600)


def append_owner_only(path: Path, line: str):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a") as f:
        f.write(line + "\n")


def run_tool(cmd, log: Path, what: str):
    with open(log, "ab") as f:
        f.write(("### %s\n" % what).encode())
        f.flush()
        r = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    if r.returncode != 0:
        raise Fatal("%s failed (exit %d, see %s)" % (what, r.returncode, log))


# ====================================================================================================================
# Profiles: how a node is secured, how the API client authenticates, what the operator does to re-key and to revoke
# ====================================================================================================================
class Standard:
    """Secure defaults: nothing set but the discovery secrets, the service key and names."""
    name = "standard"
    SECRETS = ("discovery_secret_global", "discovery_secret_region", "discovery_secret_agent", "cresco_service_key")

    def prepare(self, fab):
        self.sec = fab.work / "secrets"
        self.sec.mkdir(mode=0o700, exist_ok=True)
        os.chmod(self.sec, 0o700)
        for k in self.SECRETS:
            write_secret(self.sec / k, secrets.token_hex(16))

    def provision(self, fab, n):
        pass

    def flags(self, fab, n) -> list:
        f = ["-D%s_file=%s" % (k, self.sec / k) for k in self.SECRETS[:3]]
        if n.role == "global":
            f.append("-Dcresco_service_key_file=%s" % (self.sec / "cresco_service_key"))
        if n.tenant:
            f.append("-Dtenant_id=" + n.tenant)
        return f

    def client_kwargs(self, fab) -> dict:
        return {"service_key": (self.sec / "cresco_service_key").read_text().strip()}

    def rekey(self, fab, n):
        """The node (stopped) loses its identity: it makes a new key at its next start."""
        for f in ("node-identity.p12", "node-identity.p12.pass"):
            (fab.data(n) / "security" / f).unlink(missing_ok=True)

    def revoke(self, fab, n) -> str:
        """The operator revokes a node at the global: its key fingerprint (the node prints it at start; the global holds no
        name for an agent of another region) in the global's revocation file, which is re-read every 10 s."""
        fp = fab.identity_key(n)
        if not fp:
            raise Fatal("%s printed no identity key" % n.path)
        append_owner_only(fab.data(fab.glob) / "security" / "peer-trust.json.revoke", fp)
        return "its key %s... in the global's security/peer-trust.json.revoke" % fp[:12]


class Upgrade(Standard):
    """Standard, with the upgrade window open on the controllers and one agent on an earlier build."""
    name = "upgrade"

    def prepare(self, fab):
        super().prepare(fab)
        if not fab.a.old_jar:
            raise Fatal("the upgrade profile needs --old-jar")
        until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=6)
        self.window = ["-Ddiscovery_auth=compat", "-Ddiscovery_compat_until=" + until.strftime("%Y-%m-%dT%H:%M:%SZ")]
        # the earlier build: the first agent of the second region when there is one (events restart, re-key and revoke others)
        regions = fab.regions()
        pool = fab.agents(regions[1] if len(regions) > 1 else None, up=False)
        if not pool:
            raise Fatal("the upgrade profile needs an agent")
        pool[0].jar, pool[0].old_build = fab.a.old_jar, True
        pool[0].unjudged = "an earlier build: its own log is not the release's (what the release's nodes log about it is judged)"

    def flags(self, fab, n) -> list:
        f = super().flags(fab, n)
        if n.role != "agent" and not n.old_build:
            f += self.window
        return f


class Production:
    """Pre-provisioned identities (tools/cresco-pki.sh), security_profile=production (FIPS), no shared secret."""
    name = "production"
    tenant = "t1"

    def prepare(self, fab):
        self.pki = fab.work / "pki"
        self.toollog = fab.work / "logs" / "pki-tool.log"
        if self.pki.exists():
            shutil.rmtree(self.pki)
        self.tool(fab, "init", str(self.pki), "--days", "30")
        for r in fab.regions():
            self.tool(fab, "region", str(self.pki), self.tenant, r.region, "--days", "30")
        for n in fab.nodes.values():
            self.provision(fab, n)
        self.tool(fab, "api", str(self.pki), self.tenant, "probe", "admin", "--days", "10")
        self.tool(fab, "crl", str(self.pki))
        # plugin signing is mandatory in production: the anchors network-installed jars are signed to (none is installed)
        self.codesign = fab.work / "pki-codesign.pem"
        key = fab.work / "pki-codesign.key"
        run_tool(["openssl", "req", "-x509", "-newkey", "rsa:3072", "-sha384", "-days", "10", "-nodes", "-keyout", str(key),
                  "-out", str(self.codesign), "-subj", "/O=%s/CN=fabric matrix code signing (made at test time)" % self.tenant],
                 self.toollog, "openssl req (code signing anchor)")
        os.chmod(key, 0o600)
        os.chmod(self.codesign, 0o600)

    def tool(self, fab, *args):
        run_tool(["bash", str(PKI_TOOL)] + list(args), self.toollog, "cresco-pki.sh " + " ".join(args[:1] + args[2:]))

    def ndir(self, n) -> Path:
        return self.pki / "nodes" / ("%s_%s" % (n.region, n.name))

    def provision(self, fab, n):
        """Issue (or re-issue, with a new key) the node's identity; production keystores are BCFKS (converted offline)."""
        role = {"global": ["--global"], "region": ["--controller"], "agent": []}[n.role]
        self.tool(fab, "node", str(self.pki), self.tenant, n.region, n.name, *role, "--days", "10")
        d = self.ndir(n)
        new = d / "node.bcfks.new"
        run_tool(["java", "-jar", fab.a.jar, "--crypto", "convert-keystore", str(d / "node.p12"), str(d / "node.p12.pass"), str(new)],
                 self.toollog, "convert-keystore for " + n.path)
        os.replace(new, d / "node.bcfks")

    def flags(self, fab, n) -> list:
        d = self.ndir(n)
        return ["-Dsecurity_profile=production", "-Dbroker_security_enabled=true", "-Dtenant_id=" + self.tenant,
                "-Dpeer_identity_mode=enforce", "-Didentity_keystore=%s" % (d / "node.bcfks"),
                "-Didentity_keystore_password_file=%s" % (d / "node.p12.pass"), "-Dtrust_anchors=%s" % (d / "trust-anchors.pem"),
                "-Drevocation_file=%s" % (self.pki / "revocations.pem"), "-Didentity_keystore_reload_seconds=5",
                "-Dplugin_signing_truststore=%s" % self.codesign, "-Dwsapi_client_ca=%s" % (d / "trust-anchors.pem")]

    def client_kwargs(self, fab) -> dict:
        d = self.pki / "api" / ("%s_probe" % self.tenant)
        return {"client_cert": str(d / "api.p12"), "client_key_password": (d / "api.p12.pass").read_text().strip()}

    def rekey(self, fab, n):
        """A re-keyed production node is re-provisioned: the operator issues its name a new certificate (a new key)."""
        self.provision(fab, n)

    def revoke(self, fab, n) -> str:
        self.tool(fab, "revoke", str(self.pki), str(self.ndir(n) / "node.pem"))
        self.tool(fab, "crl", str(self.pki))
        return "cresco-pki.sh revoke + crl (every node reads the shared revocation_file)"


PROFILES = {"standard": Standard, "production": Production, "upgrade": Upgrade}


# ====================================================================================================================
# Topologies: each adds its nodes to the fabric (the global first). To add one: write a function, add it to TOPOLOGIES.
# ====================================================================================================================
def topo_single(fab):
    g = fab.add("gc", "global", "glob")
    fab.add_agent("a1", g)


def topo_tree(fab, regions=3, agents=2):
    g = fab.add("gc", "global", "glob")
    fab.infer = False            # bridged to the global only: no self-organized region-to-region bridge
    for i in range(1, regions + 1):
        r = fab.add("rc%d" % i, "region", "r%d" % i, parent=g)
        for j in range(1, agents + 1):
            fab.add_agent("a%d%d" % (i, j), r)


def topo_mesh(fab):
    topo_tree(fab)
    fab.mesh = True              # every region also bridged to the others (regional_peers)


def topo_alias(fab):
    topo_tree(fab)
    fab.add_agent("m0", fab.glob)            # starts in the global's region ...
    fab.setup_events = ["move-agent"]        # ... and is moved to a region, keeping its former address (region_alias)


def topo_tenants(fab):
    g = fab.add("gc", "global", "glob", tenant="cresco-system")     # manages two tenants' regions: a superuser tenant
    fab.infer = False
    fab.anchor_global = True
    for t in ("ta", "tb"):
        r = fab.add("rc" + t, "region", "r" + t, parent=g, tenant=t)
        for j in (1, 2):
            fab.add("a%s%d" % (t, j), "agent", r.region, parent=r, tenant=t)


def topo_regions30(fab, regions=30):
    """A scale smoke for a bigger machine: a global and 30 regions, no agents, without the default plugins. The inference
    of region-to-region bridges is left at its default, on (a 4-region trial on one host inferred none). regions<N> runs N."""
    topo_tree(fab, regions=regions, agents=0)
    fab.infer = True
    fab.a.light = True


TOPOLOGIES = {"single": topo_single, "tree": topo_tree, "mesh": topo_mesh, "tenants": topo_tenants, "alias": topo_alias,
              "regions30": topo_regions30}


# ====================================================================================================================
# Events. Each takes the fabric, does one thing to it and returns a short description. Raise Skip when it does not apply.
# To add one: write a function, add it to EVENTS (the order there is the order they run in).
# ====================================================================================================================
def pick_agent(fab, parent=None):
    """An agent that is up (of `parent` when given), never the earlier-build one or a moved one; the last one added."""
    pool = [n for n in fab.agents(parent) if not n.old_build and not n.alias]
    if not pool:
        raise Skip("no agent to pick")
    return pool[-1]


def ev_start(fab):
    g = fab.glob
    order = list(fab.nodes.values())
    fab.rng.shuffle(order)
    if fab.anchor_global:
        # regions join only a global whose key the operator gave them: the global starts first and prints its key
        fab.launch(g)
        line = fab.wait_log(g, r"This node's identity key: [0-9a-f]{64}", 120, start=0)
        if not line:
            raise Fatal("the global did not print its identity key")
        fp = re.search(r"identity key: ([0-9a-f]{64})", line).group(1)
        for r in fab.regions():
            r.extra.append("-Dcontroller_trust_anchors=" + fp)
        order.remove(g)
    for n in order:
        fab.launch(n)
        time.sleep(fab.a.stagger)
    return "%d nodes started in the order %s%s" % (len(fab.nodes), "gc (for its key), then " if fab.anchor_global else "",
                                                   " ".join(n.name for n in order))


def ev_restart_agent(fab):
    regions = fab.regions()
    n = pick_agent(fab, regions[0] if regions else None)
    fab.kill(n)
    fab.launch(n)
    return "kill -9 and start of %s" % n.path


def ev_restart_region(fab):
    regions = fab.regions()
    if not regions:
        raise Skip("no region in this topology")
    r = regions[0]
    fab.kill(r)
    fab.launch(r)
    return "kill -9 and start of %s (its agents: %s)" % (r.path, " ".join(n.name for n in fab.agents(r)))


def ev_restart_global(fab):
    g = fab.glob
    fab.kill(g)
    time.sleep(20)
    fab.launch(g)
    return "kill -9 of %s, 20 s, start" % g.path


def ev_global_outage(fab):
    g = fab.glob
    regions = fab.regions()
    parent = regions[1] if len(regions) > 1 else (regions[0] if regions else g)
    victim = pick_agent(fab, parent)
    t0 = time.time()
    fab.kill(g)
    time.sleep(5)
    fab.seq += 1
    new = fab.add_agent("n%d" % fab.seq, parent)
    fab.profile.provision(fab, new)
    fab.launch(new)
    time.sleep(5)
    mark = fab.size(parent)
    fab.kill(victim)
    t_kill = time.time()
    time.sleep(max(0.0, 60 - (time.time() - t0)))
    fab.launch(g)
    # the event ends when the dead agent's controller has declared it lost (its report belongs to this event)
    lost = fab.wait_log(parent, r"Agent NodeID: %s is STALE\. Setting to LOST" % victim.name, 200, start=mark)
    return "%s down 60 s; meanwhile %s started and %s killed (%s declared it lost after %s)" % (
        g.path, new.path, victim.path, parent.path, secs(time.time() - t_kill if lost else None))


def ev_rekey_region(fab):
    regions = fab.regions()
    if not regions:
        raise Skip("no region in this topology")
    r = regions[-1]
    start = {n.name: fab.size(n) for n in fab.nodes.values()}
    fab.stop(r)
    fab.profile.rekey(fab, r)
    fab.launch(r)
    line = fab.wait_log(r, r"This node's identity key: [0-9a-f]{64}|PRE-PROVISIONED identity", 120, start=start[r.name])
    if not line:
        raise Fatal("%s did not come up with an identity" % r.path)
    if fab.profile.name == "production":
        return "%s stopped, re-provisioned with a new key (cresco-pki.sh node) and started; nothing else to do" % r.path
    fp = re.search(r"identity key: ([0-9a-f]{64})", line).group(1)
    # The operator's part, exactly as each refusing node's log asks for it: "authorise ONE rebind on this node: add the line
    # '<region>/<agent>=<key>' (its new key) to <file> (no restart)". The key is the one the re-keyed node printed itself.
    ask = re.compile(r"authorise ONE rebind on this node: add the line '(%s=%s)' \(its new key\) to (\S+) \(no restart\)"
                     % (re.escape(r.path), fp))
    done = {}
    end = time.time() + 150
    quiet_since = time.time()
    while time.time() < end:
        for n in fab.nodes.values():
            if n.name in done or not fab.alive(n):
                continue
            for l in fab.peek(n, start[n.name]):
                m = ask.search(l)
                if m and Path(m.group(2)).resolve().is_relative_to(fab.data(n).resolve()):
                    append_owner_only(Path(m.group(2)), m.group(1))
                    done[n.name] = time.time()
                    quiet_since = time.time()
                    break
        if fab.listed() == fab.expected() and time.time() - quiet_since > 20:
            break
        time.sleep(2)
    def recover(res):
        """After the invariants are judged: nodes of the region that still do not answer are restarted, so that the events
        after this one run on a whole fabric. What the restart logs is read past, not judged; the event stays failed."""
        stuck = [n for n in [r] + fab.agents(r) if not fab.rpc(n.region, n.name, timeout=5.0)[0]]
        if not stuck:
            return
        for n in stuck:
            fab.kill(n)
            fab.launch(n)
        back = fab.wait_converged(180)
        time.sleep(fab.a.settle)
        for n in fab.nodes.values():
            fab.window(n)
        res["recovered"] = "%s did not answer and %s restarted by the harness: the fabric was whole again after %s" % (
            " ".join(n.name for n in stuck), "was" if len(stuck) == 1 else "were", secs(back))
        res["failures"].append("recovery: " + res["recovered"])

    fab.after_check = recover
    return "%s stopped, identity removed, started with the new key %s...; rebind line written where asked: %s" % (
        r.path, fp[:12], " ".join(sorted(done)) or "nowhere")


def ev_move_agent(fab):
    g = fab.glob
    regions = fab.regions()
    if not regions:
        raise Skip("no region to move an agent to")
    if fab.anchor_global:
        raise Skip("no agent can start in the global's region here: it is in a superuser tenant, which agents cannot claim")
    pool = [n for n in fab.agents(g) if not n.old_build]
    made = ""
    if pool:
        m = pool[0]
    else:
        fab.seq += 1
        m = fab.add_agent("m%d" % fab.seq, g)
        fab.profile.provision(fab, m)
        fab.launch(m)
        made = " (started in %s first, listed and answering after %s)" % (g.region, secs(fab.wait_node(m, 180)))
        time.sleep(15)
    fab.stop(m)
    former = m.region
    m.parent, m.region, m.alias, m.tenant = regions[0], regions[0].region, former, regions[0].tenant
    fab.profile.provision(fab, m)
    fab.launch(m)
    return "%s stopped in %s and started in %s with region_alias=%s%s" % (m.name, former, m.region, former, made)


def ev_cut_bridge(fab):
    regions = fab.regions()
    if not regions:
        raise Skip("no region in this topology")
    r = regions[1] if len(regions) > 1 else regions[0]
    r.proc.send_signal(signal.SIGSTOP)
    try:
        time.sleep(40)
    finally:
        r.proc.send_signal(signal.SIGCONT)
    return "%s stopped (SIGSTOP) for 40 s, then continued" % r.path


def ev_clock_skew(fab):
    raise Skip("left out: there is no supported way to give one JVM another clock (the controller reads the system clock; "
               "no setting moves it, and injecting libfaketime into a JVM is not a supported configuration)")


def ev_revoke(fab):
    regions = fab.regions()
    victim = pick_agent(fab, regions[-1] if regions else None)
    parent = victim.parent
    start = {n.name: fab.size(n) for n in fab.nodes.values()}
    t0 = time.time()
    how = fab.profile.revoke(fab, victim)
    victim.up = False                       # the global must not list it any more
    closed = fab.wait_log(parent, r"CLOSING the connection of CrescoIdentity\{[^}]*region=%s, agent=%s\b" % (victim.region, victim.name),
                          120, start=start[parent.name])
    t_closed = time.time() - t0 if closed else None
    t_unlisted = fab.wait_listed(180)
    t_unlisted = (time.time() - t0) if t_unlisted is not None else None
    # refused everywhere: the same key under another controller (another region when there is one, else its own again)
    fab.stop(victim)
    # (a production certificate names its region, and a tenant's node joins only its own tenant's region)
    others = [r for r in regions if r is not parent and r.tenant == parent.tenant] if fab.profile.name != "production" else []
    elsewhere = others[0] if others else parent
    was = victim.path
    victim.unjudged = "revoked: what it logs is the report of being refused"
    mark = fab.size(elsewhere)
    victim.parent, victim.region = elsewhere, elsewhere.region
    fab.launch(victim)
    victim.up = False
    time.sleep(45)
    readmitted = [l for l in fab.peek(elsewhere, mark) if re.search(r"mTLS identity bound from certificate: CrescoIdentity\{[^}]*agent=%s\b" % victim.name, l)]
    fab.stop(victim)
    fab.revoked = {"node": was, "how": how, "closed_after_s": t_closed, "unlisted_after_s": t_unlisted,
                   "tried_again_under": elsewhere.path, "readmitted": bool(readmitted)}
    if t_closed is None or t_closed > REVOKE_CLOSED_S:
        fab.event_failures.append("revoke: %s did not close %s within %d s" % (parent.path, was, REVOKE_CLOSED_S))
    if t_unlisted is None or t_unlisted > REVOKE_UNLISTED_S:
        fab.event_failures.append("revoke: the global still listed %s after %d s" % (was, REVOKE_UNLISTED_S))
    if readmitted:
        fab.event_failures.append("revoke: %s admitted the revoked key again (%s)" % (elsewhere.path, readmitted[0][:200]))
    return "%s revoked (%s): its controller closed it after %s, the global dropped it after %s; started again under %s: %s" % (
        was, how, secs(t_closed), secs(t_unlisted), elsewhere.path, "ADMITTED" if readmitted else "refused")


# Bounds of the revoke event (seconds from the operator's action), a little above what was measured (see the top).
REVOKE_CLOSED_S = 30
REVOKE_UNLISTED_S = 150


def secs(t):
    return "a TIMEOUT" if t is None else "%.0f s" % t


# Events that run only when named in --events, and why (none today: rekey-region was one until the controller passed it).
OPT_IN = {}
EVENTS = {"start": ev_start, "restart-agent": ev_restart_agent, "restart-region": ev_restart_region,
          "restart-global": ev_restart_global, "global-outage": ev_global_outage, "rekey-region": ev_rekey_region,
          "move-agent": ev_move_agent, "cut-bridge": ev_cut_bridge, "clock-skew": ev_clock_skew, "revoke": ev_revoke}


# ====================================================================================================================
# Observations: ONE function per thing observed, each from a node's log lines since the event began. A later version can
# answer the same question from the node's metric inventory (msg.refused{reason}, msg.dropped{cause}, ...) by replacing
# the function; the judge only sees what they return: a list of (kind, line, count).
# The controller logs a refusal (a strip, a withheld message) at most every 10 s, with a running count: the count of a
# logged line, minus the count before it, is attributed to that line's reason.
# ====================================================================================================================
LAUNCH = re.compile(r"^### fabric_matrix launch ")
OF_KIND = r"(?:; (?P<k>\d+) of this kind, (?P<why>[^;)]+))?"      # builds with per-reason counters say which and how many
RX = {
    "refused-start": re.compile(r"REFUSING TO START"),
    "timer": re.compile(r"timer task .* failed"),
    "refused": re.compile(r"MESSAGE SIGNATURE: REFUSED .*\((?P<n>\d+) refused so far" + OF_KIND),
    "dropped": re.compile(r"MESSAGE DROPPED: .*\((?P<n>\d+) dropped so far" + OF_KIND),
    "stripped": re.compile(r"STRIPPED a forged caller .*\((?P<n>\d+) stripped so far"),
    "withheld": re.compile(r"WITHHELD from a bridge .*\((?P<n>\d+) so far\)"),
    "discarded": re.compile(r"DISCARDED a message a bridge .*\((?P<n>\d+) so far\)"),
    "error": re.compile(r"^\d{1,2} \w{3} \d{4} [\d:,]+\s+(?:ERROR|FATAL)\s|^SEVERE: "),
}
COUNTED = ("refused", "dropped", "stripped", "withheld", "discarded")


def _counted(node, lines, kind):
    """Lines of a kind that carries a running count: (kind, line, the increase the line accounts for). A line that names
    its own reason's count ("K of this kind, <reason>") accounts for that reason's increase; one that does not, for the
    increase of the node's total since the line before it."""
    out = []
    for l in lines:
        if LAUNCH.match(l):
            for key in [k for k in node.last if k == kind or k[0] == kind]:
                del node.last[key]       # a new JVM counts from zero
            continue
        m = RX[kind].search(l)
        if m:
            d = m.groupdict()
            key, n = ((kind, d["why"].strip()), int(d["k"])) if d.get("k") else (kind, int(m.group("n")))
            last = node.last.get(key, 0)
            out.append((kind, l, max(0, n - last)))     # (two threads may log their lines out of order: never a reset)
            node.last[key] = max(last, n)
    return out


def _plain(lines, kind, also_not=()):
    return [(kind, l, 1) for l in lines if RX[kind].search(l) and not any(RX[k].search(l) for k in also_not)]


def observe_refused(node, lines): return _counted(node, lines, "refused")
def observe_dropped(node, lines): return _counted(node, lines, "dropped")
def observe_stripped(node, lines): return _counted(node, lines, "stripped")
def observe_withheld(node, lines): return _counted(node, lines, "withheld")
def observe_discarded(node, lines): return _counted(node, lines, "discarded")
def observe_timer_failures(node, lines): return _plain(lines, "timer")
def observe_refused_start(node, lines): return _plain(lines, "refused-start")
def observe_errors(node, lines): return _plain(lines, "error", also_not=("refused-start", "timer") + COUNTED)


OBSERVATIONS = [observe_refused, observe_dropped, observe_stripped, observe_withheld, observe_discarded, observe_timer_failures,
                observe_refused_start, observe_errors]


COUNTER_NAMES = ("msg.", "broker.caller.", "broker.bridge.")


def observe_counters(fab):
    """Every node's message counters from its metric inventory, {node: {metric: value}}, when the build exposes them
    (msg.originated / relayed / delivered / held, msg.refused[.<reason>], msg.dropped[.<cause>], broker.caller.stripped,
    broker.bridge.withheld / discarded: controller main since 983407f); {} when it does not. One RPC to the global, which
    asks the fabric. Counters start at zero with each JVM."""
    c = fab.api()
    if c is None:
        return {}
    try:
        inv = c.globalcontroller.get_metric_inventory(scope="global", include_plugins=False, include_resource=False, timeout=30.0)
    except Exception:
        return {}
    out = {}

    def walk(x, node):
        if isinstance(x, dict):
            name = x.get("name")
            if isinstance(name, str) and name.startswith(COUNTER_NAMES) and node:
                try:
                    out.setdefault(node, {})[name] = float(x.get("count", x.get("value")))
                except (TypeError, ValueError):
                    pass
            for k, v in x.items():
                walk(v, k.split(":", 1)[0] if k.count(":") == 1 and "_" in k.split(":", 1)[0] else node)
        elif isinstance(x, list):
            for v in x:
                walk(v, node)

    walk(inv, "")
    return out


def counter_gains(fab, counters) -> dict:
    """REPORTED, not judged: what each node's refusal, drop and broker counters gained since the reading after the event
    before, {metric: {node: gain}} (a JVM started since then counts from zero; what a killed JVM counted is gone). The
    log shows a reason at most every 10 s, so a burst's tail is in these counts and not in the log's."""
    out = {}
    launches = {"%s_%s" % (n.region, n.name): n.launches for n in fab.nodes.values()}
    for node, m in counters.items():
        base = fab.prev_counters.get(node, {}) if fab.prev_launches.get(node) == launches.get(node) else {}
        for k, v in m.items():
            if k.startswith(("msg.refused.", "msg.dropped.", "broker.")):
                gain = int(v - base.get(k, 0.0))
                if gain > 0:
                    out.setdefault(k, {})[node] = gain
    if counters:
        fab.prev_counters, fab.prev_launches = counters, launches
    return out


def conservation(counters) -> dict:
    """REPORTED, not judged: on a settled fabric every message a node originated was delivered, refused or dropped
    somewhere, so the sums over the nodes should balance (counters of a JVM that was killed are gone: after such an event
    the difference says little)."""
    if not counters:
        return {}
    total = lambda name: sum(v.get(name, 0.0) for v in counters.values())
    t = {k: int(total("msg." + k)) for k in ("originated", "delivered", "refused", "dropped", "relayed", "held")}
    t["nodes"] = len(counters)
    t["originated_minus_accounted"] = t["originated"] - t["delivered"] - t["refused"] - t["dropped"]
    return t


# ====================================================================================================================
# Invariants and the judge
# ====================================================================================================================
def known_for(kind, line, event):
    for k in KNOWN:
        if k.kind == kind and ("*" in k.events or event in k.events) and re.search(k.pattern, line):
            return k
    return None


def check(fab, event, began, counts_as=None):
    """The invariants after an event (and its settle time): the listing, an RPC to every node, every node's log."""
    judged = counts_as or event
    res = {"event": event, "failures": [], "known": {}, "unknown": [], "unjudged": {}}
    # (a) the global lists exactly the nodes that should be up, each in its region
    want, got = fab.expected(), fab.listed()
    res["listed"] = sorted(got) if got is not None else None
    if got is None:
        res["failures"].append("listing: the global could not be asked")
    elif got != want:
        res["failures"].append("listing: missing %s; not expected %s" % (sorted(want - got) or "none", sorted(got - want) or "none"))
    # (b) an RPC through the global's wsapi to every node's controller (a moved node also at its former address)
    rpc = {}
    for region, agent in fab.targets():
        ok, ms, err = fab.rpc(region, agent)
        rpc["%s/%s" % (region, agent)] = round(ms, 1) if ok else err
        if not ok:
            res["failures"].append("rpc: %s/%s did not answer (%s)" % (region, agent, err))
    res["rpc"] = rpc
    # (c) every node's log since the event began
    minutes = max(1.0, (time.time() - began) / 60.0)
    totals = {}
    for n in fab.nodes.values():
        lines = fab.window(n)
        per = {}
        for obs in OBSERVATIONS:
            for kind, line, count in obs(n, lines):
                totals[kind] = totals.get(kind, 0) + count
                k = known_for(kind, line, judged)
                if n.unjudged:
                    res["unjudged"][n.name] = res["unjudged"].get(n.name, 0) + count
                elif k is None:
                    res["unknown"].append({"node": n.name, "kind": kind, "count": count, "line": line[:600]})
                else:
                    per[k.id] = per.get(k.id, 0) + count
        for kid, c in per.items():
            k = next(x for x in KNOWN if x.id == kid)
            bound = int(k.bound * minutes + 0.999) if k.per_minute else k.bound
            res["known"].setdefault(kid, {})[n.name] = c
            if c > bound:
                res["failures"].append("known item %s at %s: %d, over its bound of %d" % (kid, n.name, c, bound))
    res["totals"] = totals
    if res["unknown"]:
        kinds = {}
        for u in res["unknown"]:
            kinds[u["kind"]] = kinds.get(u["kind"], 0) + u["count"]
        res["failures"].append("log: not a known item: " + ", ".join("%d %s" % (c, k) for k, c in sorted(kinds.items())))
    res["counters"] = observe_counters(fab)
    res["conservation"] = conservation(res["counters"])
    res["counter_gains"] = counter_gains(fab, res["counters"])
    res["failures"] += fab.event_failures
    fab.event_failures = []
    return res


def one_line(res) -> str:
    rpc = res.get("rpc", {})
    ok = [v for v in rpc.values() if not isinstance(v, str)]
    t = res.get("totals", {})
    known = sum(sum(v.values()) for v in res["known"].values())
    unknown = sum(u["count"] for u in res["unknown"])
    c = res.get("conservation") or {}
    line = "listing %s, converged after %s | rpc %d/%d%s | refused %d dropped %d stripped %d withheld %d discarded %d errors %d timer %d refused-start %d | known %d, NOT KNOWN %d%s%s" % (
        "ok" if not any(f.startswith("listing") for f in res["failures"]) else "WRONG", secs(res.get("listed_after_s")),
        len(ok), len(rpc), (" (max %.0f ms)" % max(ok)) if ok else "",
        t.get("refused", 0), t.get("dropped", 0), t.get("stripped", 0), t.get("withheld", 0), t.get("discarded", 0), t.get("error", 0),
        t.get("timer", 0), t.get("refused-start", 0), known, unknown,
        (", not judged %d (%s)" % (sum(res["unjudged"].values()), " ".join(res["unjudged"]))) if res.get("unjudged") else "",
        (" | counters of %(nodes)d nodes: originated %(originated)d = delivered %(delivered)d + refused %(refused)d + dropped %(dropped)d %(originated_minus_accounted)+d" % c) if c else "")
    g = res.get("counter_gains") or {}
    if g:
        line += "; gained: " + ", ".join("%s +%d" % (k.replace("msg.", ""), sum(v.values())) for k, v in sorted(g.items()))
    return line


# ====================================================================================================================
# The run
# ====================================================================================================================
def reap_previous(work: Path):
    """JVMs a previous run in this work directory left behind (it was killed): stop them, by the PIDs it recorded, when
    the process is still one started from this work directory."""
    f = work / "pids"
    if not f.exists():
        return
    for line in f.read_text().splitlines():
        try:
            pid = int(line.split()[0])
            cmd = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True).stdout
            if cmd and str(work) in cmd and "java" in cmd:
                os.kill(pid, signal.SIGKILL)
                print("# stopped a JVM a previous run left behind (pid %d)" % pid)
        except (ValueError, IndexError, ProcessLookupError, PermissionError):
            pass
    f.unlink()


def main():
    ap = argparse.ArgumentParser(description="One cell of the Cresco fabric matrix (see the top of this file).")
    ap.add_argument("--jar", required=True, help="the agent jar under test")
    ap.add_argument("--old-jar", default="", help="an earlier-build agent jar (the upgrade profile)")
    ap.add_argument("--work", required=True, help="work directory (created; node directories, logs, secrets)")
    ap.add_argument("--topology", default="tree", help=" | ".join(TOPOLOGIES))
    ap.add_argument("--profile", default="standard", choices=sorted(PROFILES))
    ap.add_argument("--events", default="", help="comma separated, run in the fixed order; default: all (%s)" % ",".join(EVENTS))
    ap.add_argument("--base-port", type=int, default=41000, help="the port block is base .. base+%d" % (SLOT * MAX_SLOTS))
    ap.add_argument("--settle", type=float, default=30.0, help="seconds between an event's end and its invariants")
    ap.add_argument("--keep", action="store_true", help="keep the node directories and secrets")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--json", default="", help="write the full result here")
    ap.add_argument("--heap", default="512m", help="-Xmx of a controller")
    ap.add_argument("--agent-heap", default="320m", help="-Xmx of an agent")
    ap.add_argument("--stagger", type=float, default=1.0, help="seconds between two launches of the start event")
    ap.add_argument("--light", action="store_true", help="nodes without the default plugins (sysinfo, executor, repo, ...)")
    ap.add_argument("--converge", type=float, default=240.0, help="seconds an event may take until the listing is right")
    a = ap.parse_args()

    scale = re.fullmatch(r"regions(\d+)", a.topology)
    if scale:
        TOPOLOGIES[a.topology] = lambda fab: topo_regions30(fab, int(scale.group(1)))
    if a.topology not in TOPOLOGIES:
        ap.error("unknown topology %s (one of: %s)" % (a.topology, ", ".join(TOPOLOGIES)))
    names = [e.strip() for e in a.events.split(",") if e.strip()] or [e for e in EVENTS if e not in OPT_IN]
    for e in names:
        if e not in EVENTS:
            ap.error("unknown event %s (one of: %s)" % (e, ", ".join(EVENTS)))
    names = [e for e in EVENTS if e in names]
    if "start" not in names:
        names.insert(0, "start")
    a.jar = str(Path(a.jar).resolve())
    a.old_jar = str(Path(a.old_jar).resolve()) if a.old_jar else ""
    for j in (a.jar, a.old_jar):
        if j and not os.path.isfile(j):
            ap.error("no such jar: " + j)
    logging.disable(logging.CRITICAL)        # the client library's own logging: the harness reports what it judges

    work = Path(a.work).resolve()
    work.mkdir(parents=True, exist_ok=True)
    reap_previous(work)
    for sub in ("nodes", "logs", "secrets", "pki"):
        shutil.rmtree(work / sub, ignore_errors=True)
    (work / "logs").mkdir()
    os.chmod(work, 0o700)

    profile = PROFILES[a.profile]()
    fab = Fabric(a, profile)
    result = {"topology": a.topology, "profile": a.profile, "jar": a.jar, "seed": a.seed, "settle_s": a.settle,
              "started": now_iso(), "events": [], "passed": False}
    cell = "%s/%s" % (a.topology, a.profile)

    def cleanup(*_):
        fab.stop_all()

    def on_signal(sig, _frame):
        raise KeyboardInterrupt("signal %d" % sig)

    atexit.register(cleanup)
    for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, on_signal)

    code = 2
    try:
        free_disk(work)
        TOPOLOGIES[a.topology](fab)
        if getattr(profile, "tenant", "") and any(n.tenant for n in fab.nodes.values()):
            raise Fatal("the %s profile runs one tenant: topology %s is for the standard profile" % (a.profile, a.topology))
        if len(fab.nodes) + 8 > MAX_SLOTS:
            raise Fatal("topology %s needs more ports than the block has" % a.topology)
        busy = [p for p in range(a.base_port, a.base_port + SLOT * (len(fab.nodes) + 8)) if port_open(p)]
        if busy:
            raise Fatal("ports of the block are in use: %s" % busy[:8])
        profile.prepare(fab)
        plan = []
        for e in names:
            plan.append((e, e))
            if e == "start":
                plan += [(s, s + " (the topology's own)") for s in fab.setup_events]
        print("# cell %s, %d nodes, events: %s" % (cell, len(fab.nodes), ", ".join(label for _, label in plan)), flush=True)
        for e, why in OPT_IN.items():
            if e not in names:
                print("# not run: %s (only when named in --events: %s)" % (e, why), flush=True)
        for event, label in plan:
            began = time.time()
            try:
                what = EVENTS[event](fab)
            except Skip as s:
                result["events"].append({"event": label, "skipped": str(s)})
                print("%-8s %-16s %s" % ("SKIP", label, s), flush=True)
                continue
            listed_after = fab.wait_converged(a.converge)
            time.sleep(a.settle)
            res = check(fab, label, began, counts_as=event)
            res.update({"what": what, "listed_after_s": listed_after, "took_s": round(time.time() - began, 1),
                        "revoked": fab.revoked if event == "revoke" else None})
            if listed_after is None:
                res["failures"].insert(0, "listing: not right with every node answering within %d s of the event" % a.converge)
            hook, fab.after_check = fab.after_check, None
            if hook and res["failures"]:
                hook(res)
            res["passed"] = not res["failures"]
            result["events"].append(res)
            print("%-8s %-16s %s\n         %s" % ("PASS" if res["passed"] else "FAIL", label, what, one_line(res)), flush=True)
            for f in res["failures"]:
                print("         ! " + f, flush=True)
            if event == "start" and listed_after is None:
                result["fatal"] = "the fabric did not form: the other events were not run"
                break
        ran = [e for e in result["events"] if "skipped" not in e]
        result["passed"] = bool(ran) and all(e["passed"] for e in ran) and "fatal" not in result
        code = 0 if result["passed"] else 1
    except Fatal as e:
        result["fatal"] = str(e)
        print("FATAL: %s" % e, flush=True)
    except KeyboardInterrupt as e:
        result["fatal"] = "interrupted (%s)" % e
        print("INTERRUPTED: stopping the JVMs", flush=True)
    finally:
        fab.stop_all()
        result["ended"] = now_iso()
        report(cell, result)
        if a.json:
            Path(a.json).write_text(json.dumps(result, indent=1))
        if not a.keep:
            for sub in ("nodes", "secrets", "pki"):
                shutil.rmtree(work / sub, ignore_errors=True)
            for f in ("pki-codesign.key", "pki-codesign.pem", "pids"):
                (work / f).unlink(missing_ok=True)
    return code


def report(cell, result):
    print("\n=== %s on %s" % (cell, os.path.basename(os.path.dirname(result["jar"])) + "/" + os.path.basename(result["jar"])))
    print("%-30s %-8s %-8s %-6s %-6s %s" % ("event", "result", "listing", "rpc", "logs", "seconds"))
    known = {}
    for e in result["events"]:
        if "skipped" in e:
            print("%-30s %-8s %s" % (e["event"], "skipped", e["skipped"][:90]))
            continue
        f = e["failures"]
        col = lambda p: "FAIL" if any(x.startswith(p) for x in f) else "ok"
        logs = "FAIL" if any(x.startswith(("log", "known")) for x in f) else "ok"
        other = "  (and: %s)" % "; ".join(x for x in f if x.startswith(("revoke", "recovery"))) if any(x.startswith(("revoke", "recovery")) for x in f) else ""
        print("%-30s %-8s %-8s %-6s %-6s %s%s" % (e["event"], "PASS" if e["passed"] else "FAIL", col("listing"), col("rpc"), logs, e["took_s"], other))
        for kid, per in e["known"].items():
            k = known.setdefault(kid, {"events": {}, "max": 0})
            k["events"][e["event"]] = sum(per.values())
            k["max"] = max(k["max"], max(per.values()))
    if known:
        print("\nknown items seen (total per event; the highest at one node in one event, against the bound per node):")
        for kid, k in known.items():
            item = next(x for x in KNOWN if x.id == kid)
            print("  %-28s max %d (bound %d%s)  %s" % (kid, k["max"], item.bound, "/min" if item.per_minute else "",
                                                     ", ".join("%s: %d" % kv for kv in k["events"].items())))
            print("  %-28s %s" % ("", item.reason))
    ran = [e for e in result["events"] if "skipped" not in e]
    for e in ran:
        r = e.get("revoked")
        if r:
            print("\nrevoke: %s closed by its controller after %s (bound %d s), dropped by the global after %s (bound %d s), "
                  "started again under %s: %s" % (r["node"], secs(r["closed_after_s"]), REVOKE_CLOSED_S, secs(r["unlisted_after_s"]),
                                                  REVOKE_UNLISTED_S, r["tried_again_under"], "ADMITTED" if r["readmitted"] else "refused"))
    if any(e.get("conservation") for e in ran):
        print("\nmessage counters, summed over the nodes that answered (reported, not judged; a killed JVM's counts are gone):")
        print("  %-28s %5s %10s %10s %8s %8s %8s %6s  %s" % ("after", "nodes", "originated", "delivered", "refused", "dropped", "relayed", "held", "originated - (delivered + refused + dropped)"))
        for e in ran:
            c = e.get("conservation")
            if c:
                print("  %-28s %5d %10d %10d %8d %8d %8d %6d  %+d" % (e["event"], c["nodes"], c["originated"], c["delivered"], c["refused"],
                                                                 c["dropped"], c["relayed"], c["held"], c["originated_minus_accounted"]))
    if any(e.get("counter_gains") for e in ran):
        print("\nwhat the nodes' counters gained per event (reported, not judged; the highest at one node in brackets):")
        for e in ran:
            g = e.get("counter_gains") or {}
            if g:
                print("  %-28s %s" % (e["event"], ", ".join("%s +%d (%d)" % (k.replace("msg.", ""), sum(v.values()), max(v.values()))
                                                            for k, v in sorted(g.items()))))
    unknown = [(e["event"], u) for e in result["events"] if "skipped" not in e for u in e["unknown"]]
    if unknown:
        print("\nNOT a known item (%d lines; each fails its event):" % len(unknown))
        seen = {}
        for ev, u in unknown:
            key = (ev, u["node"], u["kind"], re.sub(r"\d+", "N", u["line"][30:200]))
            seen.setdefault(key, [0, u["line"]])[0] += u["count"]
        for (ev, node, kind, _), (count, line) in list(seen.items())[:60]:
            print("  [%s] %s %s x%d: %s" % (ev, node, kind, count, line[:330]))
        if len(seen) > 60:
            print("  ... and %d more (see --json)" % (len(seen) - 60))
    if "fatal" in result:
        print("\nFATAL: " + result["fatal"])
    print("\n%s  %s" % ("PASS" if result["passed"] else "FAIL", cell))


if __name__ == "__main__":
    sys.exit(main())
