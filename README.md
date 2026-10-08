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
cannot start. Output: `target/controller-1.3-SNAPSHOT.jar`.

Requires `io.cresco:library` in your local Maven repository — build [library](https://github.com/CrescoEdge/library) first.

Tests: `mvn test -Djacoco.skip=true` (JaCoCo cannot instrument JDK 21 classes; always skip it).

## Release by hand (no GitHub Actions)

`scripts/release-agent.sh` builds and ships the agent from a workstation or the DGX:

```bash
scripts/release-agent.sh [--offline] [--skip-tests] [--publish] [--repo OWNER/NAME] [--tag TAG] <agent-checkout>
```

1. builds this bundle (`mvn package bundle:bundle -Djacoco.skip=true`, tests included unless
   `--skip-tests`) and checks it is an OSGi bundle;
2. copies `target/controller-<version>.jar` to `<agent-checkout>/src/main/resources/controller.jar`;
3. builds the agent (`mvn package -Dmaven.test.skip=true`) and checks the agent jar carries exactly
   the controller just built (sha256);
4. only with `--publish`: `gh release upload <tag> target/agent-<version>.jar --clobber` to the
   existing `CrescoEdge/agent` release `1.3-SNAPSHOT` (it never creates or deletes a release).

It does not run the agent's `prebuild.sh` (that re-downloads every component from the snapshot
repository and would overwrite the embedded controller) and it does not commit: the new
`controller.jar` is left in the agent checkout for you to commit with the release.

## Security-relevant configuration

| Parameter | Default | Effect |
|-----------|---------|--------|
| `controlplane_dedicated_vm` | `true` | An agent on its own broker (the global controller, vm://) gives the control-plane sender and its inbox their own vm:// connections instead of sharing the dataplane's pooled one. `false` restores the shared connection. Network URIs keep `controlplane_dedicated_connection` / `agentconsumer_dedicated_connection`. |
| `db_key_file` | unset | Encrypts the controller Derby database at rest (`AES/CBC/NoPadding`, 256-bit). The file holds one line: 64 hex characters (raw key) or a boot password of at least 16 characters. It must be a regular file owned by the agent user, mode 0600/0400, in a directory only that user (or root) can write; otherwise the controller refuses to start. A plaintext database is encrypted in place at the first boot with the key. Unset = no change. Keep the key: the database cannot be opened without it. |
| `<param>_file`, `CRESCO_<PARAM>` | — | Secret parameters (`keystorepwd`, `truststorepwd`, `db_password`, `broker_security_secret`, `discovery_secret_{agent,region,global}`, and any secret-looking name) are taken from an owner-only file named by `<param>_file` (as `-D`, `CRESCO_<PARAM>_FILE` or in agent.ini), then from the environment `CRESCO_<PARAM>`, before `-D<param>`. A `-D` copy that loses is cleared; a secret given only by `-D` still works but is logged as a warning. An unusable file refuses start. |

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
