package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;
import io.cresco.library.security.TenantNamespace;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two tenants' regions under a global of the superuser tenant: which tenant's namespace a message travels in.
 *
 * <p>A message's {@code cresco_tenant} names the queues every hop puts it on, and a router drops a message of one tenant's
 * caller in another tenant's namespace. An answer is the request's message sent back, so a tenant's answer to the
 * global's request kept the global's stamp and was dropped by the tenant's own router ("route: refused ... travels in
 * tenant cresco-system's namespace"); and a message the global originated stayed in the global's namespace, which a
 * tenant's leaf does not read. A tenant's node now always sends in its own tenant's namespace; a superuser tenant's node
 * sends into the tenant of the region it addresses, as far as it knows that tenant first hand.
 */
class TenantsUnderASuperuserGlobalTest {

    static final String SU = "cresco-system";
    static final Set<String> SUS = Set.of(SU);
    private static KeyPair k1, k2, k3;

    @BeforeAll
    static void keys() throws Exception {
        TlsTestSupport.provider();
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(256);
        k1 = g.generateKeyPair();
        k2 = g.generateKeyPair();
        k3 = g.generateKeyPair();
    }

    private static MsgEvent to(String dstRegion, String dstAgent, String srcRegion, String srcAgent) {
        MsgEvent m = new MsgEvent(MsgEvent.Type.EXEC, srcRegion, srcAgent, null, dstRegion, dstAgent, null, true, true);
        m.setParam("action", "iscontrolleractive");
        return m;
    }

    private static String namespace(MsgEvent m) {
        return m.getParam(TenantNamespace.TENANT_PARAM);
    }

    private static ControllerEngine node(String tenant, String region, String agent) throws Exception {
        return TlsTestSupport.engine(region, agent, Map.of("tenant_id", tenant));
    }

    /** {@code region/agent} joined {@code at} as a controller of {@code tenant}: its name is bound and it is a bridge peer. */
    private static void joined(PeerTrustRegistry at, String tenant, String region, String agent, KeyPair key) {
        assertTrue(at.bind(tenant, region, agent, key.getPublic(), "test", PeerTrustRegistry.ROLE_CONTROLLER, false).ok);
        at.allowBridgePeer(region, agent);
    }

    // ---- on a node: what its stamping point does (the calls every send makes)

    @Test
    void aTenantsAnswerToTheSuperuserGlobalsRequestTravelsInItsOwnTenantsNamespace() throws Exception {
        ControllerEngine rc = node("ta", "rta", "rcta");
        // the global's request as it arrived, turned round by the executor: it still carries the global's namespace
        MsgEvent answer = to("glob", "gc", "rta", "rcta");
        answer.setParam(TenantNamespace.TENANT_PARAM, SU);
        rc.stampSelf(answer);
        assertEquals("ta", namespace(answer), "the answer stayed in the superuser tenant's namespace");
        assertNull(MsgRouter.tenantRefusal(answer, SUS), "the tenant's own router drops its answer");
    }

    @Test
    void aTenantsNodeNeverSendsIntoAnotherTenantsNamespace() throws Exception {
        ControllerEngine rc = node("ta", "rta", "rcta");
        MsgEvent m = to("rtb", "atb1", "rta", "rcta");
        m.setParam(TenantNamespace.TENANT_PARAM, "tb");               // whatever the message carried or a plugin wrote
        rc.stampSelf(m);
        assertEquals("ta", namespace(m));
        MsgEvent fresh = to("rtb", "atb1", "rta", "rcta");
        rc.stampSelf(fresh);
        assertEquals("ta", namespace(fresh), "a tenant's node resolves no other tenant: it sends in its own namespace");
    }

    @Test
    void theSuperuserGlobalSendsIntoTheTenantOfTheRegionItAddresses() throws Exception {
        ControllerEngine gc = node(SU, "glob", "gc");
        PeerTrustRegistry peers = gc.getCertificateManager().getPeerTrust();
        joined(peers, "ta", "rta", "rcta", k1);
        joined(peers, "tb", "rtb", "rctb", k2);

        MsgEvent toLeaf = to("rta", "ata1", "glob", "gc");
        gc.stampSelf(toLeaf);
        assertEquals("ta", namespace(toLeaf), "a tenant's leaf reads its own tenant's inbox only: the request never reached it");
        assertNull(MsgRouter.tenantRefusal(toLeaf, SUS));

        MsgEvent toOther = to("rtb", "rctb", "glob", "gc");
        gc.stampSelf(toOther);
        assertEquals("tb", namespace(toOther));

        MsgEvent own = to("glob", "gc", "glob", "gc");
        gc.stampSelf(own);
        assertEquals(SU, namespace(own), "its own region is its own tenant's");

        MsgEvent unknown = to("rtz", "x", "glob", "gc");
        gc.stampSelf(unknown);
        assertEquals(SU, namespace(unknown), "a region no controller joined from: nothing is guessed");

        // and what it carries is kept, as before: its answer goes back in the namespace the request came in (a region it
        // holds no record of yet included), and an operator's client may name the tenant it addresses
        MsgEvent pong = to("rtz", "rcz", "glob", "gc");
        pong.setParam(TenantNamespace.TENANT_PARAM, "tz");
        gc.stampSelf(pong);
        assertEquals("tz", namespace(pong));
        MsgEvent named = to("rta", "ata1", "glob", "gc");
        named.setParam(TenantNamespace.TENANT_PARAM, "tb");
        gc.stampSelf(named);
        assertEquals("tb", namespace(named));
    }

    // ---- the rule itself

    @Test
    void aMessageTravelsInItsCallersTenantUnlessItsCallerIsOfASuperuserTenant() {
        Caller tenant = Caller.controller("ta", "rta", "rcta"), user = Caller.api("ta", "alice", List.of("user"));
        Caller su = Caller.controller(SU, "glob", "gc");
        assertEquals("ta", MsgRouter.travelTenant(tenant, SU, "ta", SUS, () -> "tb"));
        assertEquals("ta", MsgRouter.travelTenant(tenant, null, "ta", SUS, () -> "tb"));
        assertEquals("ta", MsgRouter.travelTenant(user, null, SU, SUS, () -> "tb"), "a tenant's user at the operator's node");
        assertEquals("tb", MsgRouter.travelTenant(su, null, SU, SUS, () -> "tb"));
        assertEquals("ta", MsgRouter.travelTenant(su, "ta", SU, SUS, () -> "tb"), "the stamp a superuser's message carries is kept");
        assertEquals(SU, MsgRouter.travelTenant(su, null, SU, SUS, () -> null));
        assertEquals(SU, MsgRouter.travelTenant(su, "", SU, SUS, null));
        assertEquals("ta", MsgRouter.travelTenant(Caller.anonymous(), "ta", "t1", SUS, () -> "tb"), "security off: as before");
        assertEquals("t1", MsgRouter.travelTenant(null, null, "t1", SUS, () -> "tb"));
        // whatever it answers passes the router's own check for that caller
        for (Caller c : List.of(tenant, user, su)) {
            for (String carried : new String[]{null, "ta", "tb", SU}) {
                MsgEvent m = to("rtb", "atb1", "glob", "gc");
                c.stamp(m);
                m.setParam(TenantNamespace.TENANT_PARAM, MsgRouter.travelTenant(c, carried, SU, SUS, () -> "tb"));
                assertNull(MsgRouter.tenantRefusal(m, SUS), c + " carrying " + carried);
            }
        }
    }

    // ---- what a node knows of a region's tenant

    @Test
    void aRegionsTenantIsThatOfItsControllerThatJoinedThisNode() {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        peers.setSuperuserTenants(SUS);
        joined(peers, "ta", "rta", "rcta", k1);                       // standard profile: the bridge peer is recorded by name
        assertNull(peers.allowBridgePeer("tb", "rtb", "rctb"));       // pre-provisioned: recorded with its tenant
        assertTrue(peers.bind("ta", "rtc", "a1", k3.getPublic(), "test").ok);   // a bound name that is no bridge peer
        assertEquals("ta", peers.tenantOfRegion("rta"));
        assertEquals("tb", peers.tenantOfRegion("rtb"));
        assertNull(peers.tenantOfRegion("rtc"), "only a controller that completed discovery with this node speaks for its region");
        assertNull(peers.tenantOfRegion("nowhere"));
        assertNull(peers.tenantOfRegion(null));
    }

    @Test
    void aRegionTwoTenantsControllersClaimIsAddressedInNeithersNamespace() {
        PeerTrustRegistry peers = new PeerTrustRegistry();
        peers.setSuperuserTenants(SUS);
        joined(peers, "ta", "rta", "rcta", k1);
        joined(peers, "tb", "rta", "intruder", k2);                   // another tenant's controller takes a name in ta's region
        assertNull(peers.tenantOfRegion("rta"), "the second claimant would be handed the messages meant for the first");

        PeerTrustRegistry mixed = new PeerTrustRegistry();
        mixed.setSuperuserTenants(SUS);
        joined(mixed, "ta", "rta", "rcta", k1);
        joined(mixed, SU, "rta", "ops", k3);                          // the operator's own controller beside the tenant's
        assertEquals("ta", mixed.tenantOfRegion("rta"));
    }
}
