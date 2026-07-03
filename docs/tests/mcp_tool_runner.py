#!/usr/bin/env python3
"""MCP-style tool-runner for the Cresco capability inventory — a TEST harness (not a plugin/library).

Given a connected pycrescolib client, it:
  1. pulls the fabric capability inventory (getcapabilityinventory),
  2. flattens every ActionDescriptor into an Anthropic-style tool spec (name/description/input_schema),
  3. exposes call_tool(name, args) which reads the descriptor's cresco_binding and turns the tool call
     back into the correct MsgEvent (global/regional/agent/plugin), sends it, and returns the reply.

This proves the catalog is directly usable for LLM tool-calling: an agent can discover every fabric
capability and invoke it purely from the self-description, with no hand-written per-action glue.
"""
import sys


def _tool_from_descriptor(d):
    """Build {name, description, input_schema} + keep the cresco_binding — same shape the Java serializer emits."""
    namespace = d.get("namespace", "")
    action = d.get("action", "")
    name = ("cresco_" + namespace + "_" + action)
    name = "".join(c if (c.isalnum() or c == "_") else "_" for c in name)

    desc = d.get("summary", "") or ""
    if d.get("why"):
        desc += "\n\nWhen/why: " + d["why"]

    props, required = {}, []
    for rp in d.get("routingParams", []) or []:
        props[rp] = {"type": "string", "description": f"Routing identity: target {rp} for this call."}
        required.append(rp)
    for p in d.get("params", []) or []:
        props[p["name"]] = {"type": p.get("type", "string")}
        if p.get("description"):
            props[p["name"]]["description"] = p["description"]
        if p.get("required"):
            required.append(p["name"])

    return {
        "name": name,
        "description": desc,
        "input_schema": {"type": "object", "properties": props, "required": required},
        "cresco_binding": {
            "msg_type": d.get("msgType", "EXEC"),
            "target": d.get("target", "plugin"),
            "action": action,
            "routing_params": d.get("routingParams", []) or [],
            "returns": d.get("returns", []) or [],
        },
    }


class CapabilityToolRunner:
    def __init__(self, client):
        self.client = client
        self.tools = []          # list of tool dicts (name/description/input_schema/cresco_binding)
        self._by_name = {}

    def load(self, inventory):
        """Flatten a getcapabilityinventory result into tools, deduped by tool name.

        ONLY MsgEvent actions (capabilities_by_source[*].actions) become callable tools — an external
        client reaches Cresco solely over the WebSocket message bus and cannot invoke OSGi services
        directly. The inventory's `osgi` section (Export-Package + registered service interfaces) is
        informational metadata about what a bundle exposes internally; it is deliberately NOT walked here
        and never surfaces as a tool.
        """
        seen = set()

        def walk(node):
            if not isinstance(node, dict):
                return
            for src, doc in (node.get("capabilities_by_source") or {}).items():
                if not isinstance(doc, dict):
                    continue
                for d in (doc.get("actions") or []):
                    t = _tool_from_descriptor(d)
                    if t["name"] in seen:
                        continue
                    seen.add(t["name"])
                    self.tools.append(t)
                    self._by_name[t["name"]] = t
            for _, child in (node.get("children") or {}).items():
                walk(child)

        walk(inventory)
        return self.tools

    def tool_specs(self):
        """The LLM-facing tool list (strip the cresco_binding)."""
        return [{k: v for k, v in t.items() if k != "cresco_binding"} for t in self.tools]

    def call_tool(self, name, args=None, timeout=15.0):
        """Invoke a described tool by building the MsgEvent from its binding and routing it correctly."""
        args = dict(args or {})
        t = self._by_name.get(name)
        if not t:
            raise KeyError(f"unknown tool: {name}")
        b = t["cresco_binding"]
        msg_type = b["msg_type"]
        target = b["target"]
        routing = b.get("routing_params", [])

        # separate routing identity from action params
        route = {r: args.pop(r) for r in routing if r in args}
        payload = {"action": b["action"]}
        payload.update(args)

        m = self.client.messaging
        if target == "global":
            reply = m.global_controller_msgevent(True, msg_type, payload, timeout)
        elif target == "regional":
            reply = m.global_agent_msgevent(True, msg_type, payload, route.get("region"), route.get("agent"), timeout)
        elif target == "agent":
            reply = m.global_agent_msgevent(True, msg_type, payload, route.get("region"), route.get("agent"), timeout)
        elif target == "plugin":
            reply = m.global_plugin_msgevent(True, msg_type, payload,
                                             route.get("region"), route.get("agent"), route.get("pluginid"), timeout)
        else:
            raise ValueError(f"unknown target tier: {target}")
        return reply
