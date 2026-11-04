package io.cresco.agent.core;

import io.cresco.library.security.Caller;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Who may send on behalf of an API user (docs/caller-authorization.md section 2). */
class OnBehalfPolicyTest {

    static final Set<String> ALLOWED = Set.of("io.cresco.wsapi"), SU = Set.of("cresco-system");
    static final Caller ALICE = Caller.api("t1", "alice", List.of("user"));

    @Test
    void onlyAListedBundleForItsOwnPluginForItsTenantsUsers() {
        assertNull(AgentServiceImpl.onBehalfRefusal("io.cresco.wsapi", ALLOWED, true, "p1", ALICE, "t1", SU));
        assertNotNull(AgentServiceImpl.onBehalfRefusal("io.cresco.evil", ALLOWED, true, "p1", ALICE, "t1", SU), "an unlisted bundle");
        assertNotNull(AgentServiceImpl.onBehalfRefusal("io.cresco.wsapi", ALLOWED, false, "p9", ALICE, "t1", SU), "another bundle's plugin");
        assertNotNull(AgentServiceImpl.onBehalfRefusal("io.cresco.wsapi", ALLOWED, true, "p1", Caller.controller("t1", "r", "a"), "t1", SU),
                "on behalf of a controller");
        assertNotNull(AgentServiceImpl.onBehalfRefusal("io.cresco.wsapi", ALLOWED, true, "p1", null, "t1", SU));
        assertNotNull(AgentServiceImpl.onBehalfRefusal("io.cresco.wsapi", ALLOWED, true, "p1",
                Caller.api("cresco-system", "op", List.of("admin")), "t1", SU), "a tenant's node mints no superuser admin");
        assertNull(AgentServiceImpl.onBehalfRefusal("io.cresco.wsapi", ALLOWED, true, "p1",
                Caller.api("t2", "bob", List.of("admin")), "cresco-system", SU), "a superuser tenant's node sends for any tenant");
    }
}
