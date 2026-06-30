# Cresco Controller

Control plane — manages agents, regions and the global hierarchy; embeds the ActiveMQ broker, Derby state store, discovery, and loads system plugins.

Part of the **[Cresco](https://github.com/CrescoEdge/agent)** edge-computing framework. See the
**[agent repository](https://github.com/CrescoEdge/agent)** for the full architecture, build, and run guide.

## Role

The heart of Cresco. It manages an agent's plugins, a region's agents, and the global controller's regions. It brings up an embedded ActiveMQ broker (agent-to-agent messaging), an embedded Apache Derby database (controller state), TCP/UDP discovery, the data plane, and loads the system plugins (`repo`, `sysinfo`, `wsapi`, `stunnel`, …).

## Build

```bash
mvn package bundle:bundle
```

Built with **JDK 21**. The `bundle:bundle` goal is required: it rewrites the jar into a
proper OSGi bundle (with `Bundle-SymbolicName`, `Export-Package`, and embedded
dependencies). A plain `mvn package`/`install` produces a non-bundle jar that the agent
cannot start. Output: `target/controller-1.2-SNAPSHOT.jar`.

Requires `io.cresco:library` in your local Maven repository — build [library](https://github.com/CrescoEdge/library) first.

## Cresco framework

| Component | Role |
|-----------|------|
| [Agent](https://github.com/CrescoEdge/agent) | OSGi runtime that boots the framework and bundles every component into one executable jar. |
| [Logger](https://github.com/CrescoEdge/logger) | Logging bundle (pax-logging) — the first service the agent starts. |
| [Library](https://github.com/CrescoEdge/library) | Shared `io.cresco.library` API + embedded dependencies (JMS, Siddhi, Jackson, …) used by every component and plugin. |
| [Core](https://github.com/CrescoEdge/core) | Core agent services (logging control, update management), loaded above the library. |
| **[Controller](https://github.com/CrescoEdge/controller)** — _this repo_ | Control plane — manages agents, regions and the global hierarchy; embeds the ActiveMQ broker, Derby state store, discovery, and loads system plugins. |
| [Repo](https://github.com/CrescoEdge/repo) | Plugin repository — stores, reports and deploys Cresco plugins. |
| [SysInfo](https://github.com/CrescoEdge/sysinfo) | Collects operating-environment and system metrics for an agent. |
| [WSAPI](https://github.com/CrescoEdge/wsapi) | WebSocket API plugin — the external client entrypoint (control, data plane, log streaming) over `wss://…:8282`. |
| [STunnel](https://github.com/CrescoEdge/stunnel) | Secure TCP tunnel plugin (Netty) — tunnels TCP across the fabric. |
| [Java Client (clientlib)](https://github.com/CrescoEdge/clientlib) | Java client library for driving Cresco through the wsapi. |
| [Python Client (pycrescolib)](https://github.com/CrescoEdge/pycrescolib) | Python client library for driving Cresco through the wsapi. |

## License

Apache License, Version 2.0.
