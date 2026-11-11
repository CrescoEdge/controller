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
        assertEquals("ta", MsgRouter.travelTenant(tenant, null, SU, "ta", SUS));
        assertEquals("ta", MsgRouter.travelTenant(tenant, "tb", null, "ta", SUS));
        assertEquals("ta", MsgRouter.travelTenant(user, null, null, SU, SUS), "a tenant's user at the operator's node");
        assertNull(MsgRouter.travelTenant(su, null, null, SU, SUS), "a superuser's new message: the tenant of the node it addresses");
        assertNull(MsgRouter.travelTenant(su, "", "", SU, SUS));
        assertEquals("ta", MsgRouter.travelTenant(su, null, "ta", SU, SUS), "the stamp a superuser's message carries is kept");
        assertEquals("ta", MsgRouter.travelTenant(su, SU, "ta", SU, SUS), "a superuser's request it answers: the stamp it carried");
        assertEquals("ta", MsgRouter.travelTenant(su, "ta", "tb", SU, SUS), "a tenant's request it answers: that tenant, not the one named");
        assertEquals("ta", MsgRouter.travelTenant(Caller.anonymous(), null, "ta", "t1", SUS), "security off: as before");
        assertEquals("t1", MsgRouter.travelTenant(null, null, null, "t1", SUS));
        // whatever it answers passes the router's own check for that caller
        for (Caller c : List.of(tenant, user, su)) {
            for (String carried : new String[]{null, "ta", "tb", SU}) {
                for (String answered : new String[]{null, "ta", SU}) {
                    MsgEvent m = to("rtb", "atb1", "glob", "gc");
                    c.stamp(m);
                    String t = MsgRouter.travelTenant(c, answered, carried, SU, SUS);
                    m.setParam(TenantNamespace.TENANT_PARAM, t == null ? "tb" : t);
                    assertNull(MsgRouter.tenantRefusal(m, SUS), c + " carrying " + carried + " answering " + answered);
                }
            }
        }
    }

    // ---- a message off the wire, where it is dispatched

    /** An RPC request as it arrived at a node, from {@code src}, verified as {@code caller}'s, travelling in {@code namespace}. */
    private static MsgEvent request(Caller caller, String namespace, String srcRegion, String srcAgent) {
        MsgEvent m = to("glob", "gc", srcRegion, srcAgent);
        m.setParam("is_rpc", "true");
        m.setParam("callId-" + srcRegion + "-" + srcAgent + "-null", "c1");
        caller.stamp(m);
        if (namespace != null) m.setParam(TenantNamespace.TENANT_PARAM, namespace);
        return m;
    }

    @Test
    void aRequestInAnotherTenantsNamespaceIsRefusedWhereItArrives() throws Exception {
        ControllerEngine gc = node(SU, "glob", "gc");
        Caller rcta = Caller.controller("ta", "rta", "rcta");
        // tenant ta's controller names tenant tb in the stamp and tb's leaf as the sender: the answer was to go there
        MsgEvent forged = request(rcta, "tb", "rtb", "atb1");
        assertTrue(MsgRouter.isRpcRequest(forged));
        assertFalse(gc.admitArrival(forged, rcta), "the global answered into tenant tb's namespace");
        assertEquals(1, gc.msgCounts().droppedCount(MsgCounts.Drop.CROSS_TENANT));
        assertTrue(gc.admitArrival(request(rcta, "ta", "rta", "rcta"), rcta), "a tenant's own request");
        Caller op = Caller.controller(SU, "rops", "ops");
        assertTrue(gc.admitArrival(request(op, "tb", "rops", "ops"), op), "a superuser tenant's caller sends into any tenant");
        // the same rule at a tenant's node
        ControllerEngine rc = node("ta", "rta", "rcta");
        Caller rctb = Caller.controller("tb", "rtb", "rctb");
        assertFalse(rc.admitArrival(request(rctb, "ta", "rtb", "rctb"), rctb));
        assertEquals(1, gc.msgCounts().droppedCount(), "each refusal is counted once");
    }

    @Test
    void anotherTenantsRequestIsServedOnlyForTheNodeThatVerifiedAsItsSender() throws Exception {
        ControllerEngine gc = node(SU, "glob", "gc");
        Caller rcta = Caller.controller("ta", "rta", "rcta");
        // in its own namespace, but the answer would go to another tenant's controller (which reads every namespace)
        MsgEvent elsewhere = request(rcta, "ta", "rtb", "rctb");
        assertFalse(gc.admitArrival(elsewhere, rcta), "the global's answer went to a node that never asked");
        assertEquals(1, gc.msgCounts().droppedCount(MsgCounts.Drop.SENDER_UNBOUND));
        assertTrue(gc.admitArrival(request(rcta, "ta", "rta", "rcta"), rcta));
        // a moved node asks from its former address (region_alias): the stamp its controller made says so
        MsgEvent moved = request(rcta, "ta", "rold", "rcta");
        rcta.stampRegionAlias(moved, "rold");
        assertTrue(gc.admitArrival(moved, rcta));
        // an API user speaks through the node that vouched for it
        Caller alice = Caller.api("ta", "alice", List.of("user"));
        assertNull(MsgRouter.senderRefusal(request(alice, "ta", "rta", "ata1"), alice, "ta/rta/ata1", SU, SUS));
        assertNotNull(MsgRouter.senderRefusal(request(alice, "ta", "rtb", "atb1"), alice, "ta/rta/ata1", SU, SUS));
        // not a request: nothing is answered, nothing to bind; within one tenant: as before
        MsgEvent notice = to("glob", "gc", "rtb", "rctb");
        rcta.stamp(notice);
        assertNull(MsgRouter.senderRefusal(notice, rcta, null, SU, SUS));
        assertNull(MsgRouter.senderRefusal(request(rcta, "ta", "rtx", "x"), rcta, null, "ta", SUS));
    }

    @Test
    void theGlobalsAnswerToATenantGoesIntoThatTenantsNamespaceWhateverTheRequestNamed() throws Exception {
        ControllerEngine gc = node(SU, "glob", "gc");
        // the request as the global holds it: verified as ta's controller's, naming tb in its stamp
        MsgEvent answer = request(Caller.controller("ta", "rta", "rcta"), "tb", "rta", "rcta");
        answer.setReturn();
        gc.stampSelf(answer);
        assertEquals("ta", namespace(answer), "the answer kept the tenant the request merely named");
        // a superuser's request: its answer goes back in the namespace it carried
        MsgEvent toOps = request(Caller.controller(SU, "rops", "ops"), "tb", "rops", "ops");
        toOps.setReturn();
        gc.stampSelf(toOps);
        assertEquals("tb", namespace(toOps));
    }

    // ---- a region whose tenant this node does not know

    @Test
    void aMessageForARegionOfUnknownOrDisputedTenantIsCountedAndLoggedWithTheRegion() throws Exception {
        ControllerEngine gc = node(SU, "glob", "gc");
        PeerTrustRegistry peers = gc.getCertificateManager().getPeerTrust();
        joined(peers, "ta", "rta", "rcta", k1);
        joined(peers, SU, "rta", "ops", k3);                           // a tenant's controller beside the operator's, in one region
        assertTrue(peers.bind("tb", "rgone", "atb9", k2.getPublic(), "test").ok);   // a node bound here; its region's controller is not
        List<String> lines = new java.util.concurrent.CopyOnWriteArrayList<>();
        gc.msgCounts().lineTap = lines::add;

        MsgEvent disputed = to("rta", "ata1", "glob", "gc");
        gc.stampSelf(disputed);
        assertEquals(SU, namespace(disputed), "neither tenant's namespace");
        assertEquals(1, gc.msgCounts().tenantUnresolvedCount());
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("region rta") && lines.get(0).contains(SU + ", ta"), lines.get(0));

        MsgEvent unknown = to("rtz", "x", "glob", "gc");
        gc.stampSelf(unknown);
        assertEquals(2, gc.msgCounts().tenantUnresolvedCount());
        assertEquals(1, lines.size(), "logged at most every 10 s");
        assertEquals(0, gc.msgCounts().droppedCount(), "it is sent, not ended here: no drop");

        MsgEvent toBound = to("rgone", "atb9", "glob", "gc");            // the very node is bound here: its tenant is known
        gc.stampSelf(toBound);
        assertEquals("tb", namespace(toBound));
        MsgEvent toController = to("rta", "rcta", "glob", "gc");         // a disputed region, but this very node proved tenant ta
        gc.stampSelf(toController);
        assertEquals("ta", namespace(toController));
        assertEquals(2, gc.msgCounts().tenantUnresolvedCount());
    }

    // ---- a node of a superuser tenant reads its inbox in every tenant's namespace

    @Test
    void aSuperuserTenantsNodeConsumesItsInboxInEveryTenantsNamespaceAndATenantsLeafItsOwn() {
        assertEquals("T.*.rops_ops1", MsgRouter.inboxQueue("rops_ops1", false, true, SU, SUS),
                "a tenant's answer (in the tenant's namespace) never reached an operator's leaf");
        assertEquals("T.ta.rta_ata1", MsgRouter.inboxQueue("rta_ata1", false, true, "ta", SUS));
        assertEquals("T.*.rta_rcta", MsgRouter.inboxQueue("rta_rcta", true, true, "ta", SUS), "a controller on its own broker, as before");
        assertEquals("rta_ata1", MsgRouter.inboxQueue("rta_ata1", false, false, "ta", SUS), "without namespacing: the raw name");
        // and what a tenant asks of it is judged by its verified caller as on any node: no privileged action
        io.cresco.library.security.ActionAuthorizer a = ControllerActionRoles.agentTier(new io.cresco.library.security.ActionAuthorizer(null));
        for (Caller c : List.of(Caller.controller("ta", "rta", "rcta"), Caller.api("ta", "root", List.of("admin")), Caller.plugin("ta", "rta", "ata1", "p"))) {
            for (String act : List.of("pluginadd", "killjvm", "stopcontroller", "setloglevel")) {
                MsgEvent m = to("rops", "ops1", "rta", "rcta");
                m.setMsgType(MsgEvent.Type.CONFIG);
                m.setParam("action", act);
                c.stamp(m);
                assertFalse(a.decide(m, SU, SUS).allowed, act + " asked of a superuser tenant's node by " + c);
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

        assertEquals("ta, tb", peers.tenantsRecordedFor("rta"));

        // a tenant's controller beside the operator's own: the tenant is not handed the operator's region either
        PeerTrustRegistry mixed = new PeerTrustRegistry();
        mixed.setSuperuserTenants(SUS);
        joined(mixed, SU, "rops", "ops", k3);
        joined(mixed, "ta", "rops", "squatter", k1);
        assertNull(mixed.tenantOfRegion("rops"), "a tenant that took a name in an operator's region received the global's messages for it");
        assertEquals(SU + ", ta", mixed.tenantsRecordedFor("rops"));
        assertNull(mixed.tenantsRecordedFor("nowhere"));
    }
}
