package io.cresco.agent.controller.netdiscovery;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;

/** An in-process discovery v2 AGENT exchange between two engines, for tests outside this package. */
public final class DiscoveryTestHooks {

    private DiscoveryTestHooks() { }

    private static final Gson GSON = new Gson();

    private static DiscoveryAuth.Msg copy(DiscoveryAuth.Msg m) { return GSON.fromJson(GSON.toJson(m), DiscoveryAuth.Msg.class); }

    /** {@code client} joins {@code server} (an AGENT exchange); the node it learned, or null when refused. */
    public static DiscoveryNode join(ControllerEngine client, ControllerEngine server) {
        DiscoveryProcessor c = new DiscoveryProcessor(client), s = new DiscoveryProcessor(server);
        DiscoveryAuth.Msg hello = c.clientHello(DiscoveryType.AGENT);
        if (hello == null) return null;
        DiscoveryAuth.Msg ch = s.serverChallenge(copy(hello), "client");
        if (ch == null) return null;
        DiscoveryAuth.Msg proof = c.clientProof(hello, copy(ch), "server");
        if (proof == null) return null;
        DiscoveryAuth.Msg r = s.serverResult(copy(hello), ch, copy(proof), "client");
        return c.clientResult(hello, copy(ch), copy(r), "127.0.0.1", 32005);
    }
}
