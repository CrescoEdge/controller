package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.ActionAuthorizer;
import io.cresco.library.security.Caller;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review of Phase 3 (controller 0ff640d), 2026-11-04: registry binding and the "fabric mechanics"
 * any-tenant allowance. Each test asserts the promised outcome ("an agent registers, unregisters and keeps alive only
 * itself in its region, a region only itself and its agents at the global"; "infra crosses a tenant only for the fabric
 * mechanics ... nothing that reads or changes a tenant's data"); a failure is a finding. Review-only: never pushed.
 *
 * <p>Setting: region rgA is shared by tenants t1 and t2 (its controller is in the superuser tenant, as the contract requires
 * for a region hosting several tenants). The attacker is an ordinary agent of tenant t2 in rgA: {@code controller:t2/rgA/a5}
 * (any node whose operator is t2, or a compromised t2 node). Its target is t1's agent a6 in the same region.
 *
 * <p>How the deletes land: RegionalExecutor CONFIG agent_disable and GlobalExecutor CONFIG region_disable both call
 * {@code DBInterfaceImpl.removeNode(msg)} -> {@code DBEngine.removeNode(unregister_region_id, unregister_agent_id,
 * unregister_plugin_id)}: (r, null, null) deletes region r with all its agents and plugins; (null, a, null) resolves a's
 * region and deletes agent a; (null, null, p) runs {@code DELETE FROM pnode WHERE plugin_id = p} across every agent.
 */
class ReviewPhase3RegistryTest {

    static final String SU = "cresco-system";
    static final Caller T2_AGENT = Caller.controller("t2", "rgA", "a5");

    private static MsgEvent config(String action, Map<String, String> params) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, "rgA", "a5", null, "rgA", "rc", null, true, false);
        m.setParam("action", action);
        params.forEach(m::setParam);
        return m;
    }

    /** At the region: an agent sends agent_disable naming only its region -> removeNode(rgA, null, null) wipes the region. */
    @Test
    void anAgentCannotUnregisterItsWholeRegion() {
        MsgEvent m = config("agent_disable", Map.of("unregister_region_id", "rgA"));
        assertTrue(RegistryBinding.applies(m));
        assertNotNull(RegistryBinding.refusal(m, T2_AGENT, false, null),
                "controller:t2/rgA/a5 may send agent_disable{unregister_region_id=rgA} with no agent id: the region's"
                        + " DBEngine.removeNode(rgA,null,null) then deletes every agent of rgA, of every tenant");
    }

    /** At the region: unregister_plugin_id is never bound -> DELETE FROM pnode WHERE plugin_id = ? on every agent. */
    @Test
    void anAgentCannotUnregisterAnotherAgentsPlugins() {
        MsgEvent m = config("agent_disable", Map.of("unregister_plugin_id", "system-sysinfo"));
        assertNotNull(RegistryBinding.refusal(m, T2_AGENT, false, null),
                "controller:t2/rgA/a5 may send agent_disable{unregister_plugin_id=system-sysinfo}: removeNode(null,null,p)"
                        + " deletes that plugin's row on EVERY agent of the region (unregister_plugin_id is never checked)");
        MsgEvent own = config("agent_disable", Map.of("unregister_region_id", "rgA", "unregister_agent_id", "a6",
                "unregister_plugin_id", "p1"));
        assertNotNull(RegistryBinding.refusal(own, T2_AGENT, false, null), "control: another agent's plugin");
    }

    /** At the global: unregister_agent_id is not checked at all (only at the region) -> any agent, any region. */
    @Test
    void atTheGlobalANodeCannotUnregisterAnotherRegionsAgent() {
        MsgEvent m = config("region_disable", Map.of("unregister_agent_id", "a6"));
        assertNotNull(RegistryBinding.refusal(m, Caller.controller("t2", "rgB", "a5"), true, agent -> "rgA"),
                "controller:t2/rgB/a5 may send region_disable{unregister_agent_id=a6} to the global: removeNode(null,a6,null)"
                        + " resolves a6's region (rgA) and deletes it -- another region's, another tenant's agent");
        MsgEvent p = config("region_disable", Map.of("unregister_plugin_id", "system-sysinfo"));
        assertNotNull(RegistryBinding.refusal(p, Caller.controller("t2", "rgB", "a5"), true, null),
                "region_disable{unregister_plugin_id} at the global deletes that plugin id on every agent of the fabric");
    }

    /**
     * At the global the binding is by region NAME: any controller whose stamp names region rgA -- every agent of rgA, of any
     * tenant -- passes as "region rgA", so an ordinary agent unregisters its whole region at the global (or re-imports it).
     */
    @Test
    void atTheGlobalOnlyTheRegionsControllerSpeaksForTheRegion() {
        MsgEvent m = config("region_disable", Map.of("unregister_region_id", "rgA"));
        assertNotNull(RegistryBinding.refusal(m, T2_AGENT, true, null),
                "controller:t2/rgA/a5 (an agent, not rgA's controller) unregisters region rgA -- all its agents of every"
                        + " tenant -- at the global");
    }

    /**
     * setinodestatus is in the global tier's any-tenant-infra list, but no controller sends it (it is a client API) and it
     * rewrites any iNode's status_code/status_desc in the global DB -- the scheduling state of every tenant's pipelines.
     * RegistryBinding does not cover it. So any node of any tenant (infra) rewrites another tenant's pipeline state.
     */
    @Test
    void setInodeStatusIsNotAFabricMechanicForeignTenantsMayCall() {
        ActionAuthorizer global = ControllerActionRoles.globalTier();
        MsgEvent m = new MsgEvent(MsgEvent.Type.CONFIG, "rgB", "a5", null, "rgG", "gc", null, false, true);
        m.setParam("action", "setinodestatus");
        m.setParam("action_inodeid", "inode-of-a-t1-pipeline");
        m.setParam("action_statuscode", "10");
        m.setParam("action_statusdesc", "iNode scheduled for removal.");
        Caller.controller("t2", "rgB", "a5").stamp(m);
        ActionAuthorizer.Decision d = global.decide(m, SU, Set.of(SU));
        assertFalse(d.allowed, "controller:t2/rgB/a5 (an ordinary agent of tenant t2) may setinodestatus on any iNode at the"
                + " global (tenant " + SU + "): the any-tenant 'fabric mechanics' allowance covers an action that changes"
                + " tenants' data");
    }
}
