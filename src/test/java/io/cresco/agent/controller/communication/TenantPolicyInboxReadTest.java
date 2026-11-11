package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.communication.TenantPolicy.Access;
import io.cresco.agent.controller.communication.TenantPolicy.Decision;
import io.cresco.agent.controller.communication.TenantPolicy.Role;
import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A queue of a tenant's namespace is a node's inbox: every node of the tenant writes it, only the node it names reads it
 * (docs/tenant-isolation-design.md section 12a). Before, any node of the tenant read every queue of {@code T.<tenant>.}
 * -- its controller's inbox, a sibling's, a pattern over all of them -- and so took other nodes' control messages.
 */
class TenantPolicyInboxReadTest {

    private static final Set<String> SHARED = Collections.emptySet();
    private static final CrescoIdentity A1 = CrescoIdentity.of("t1", "rgA", "a1", null);

    private static Decision queue(CrescoIdentity who, String name, Access access, Role role) {
        return TenantPolicy.check(who, name, access, SHARED, role, null, null, true);
    }

    private static Decision nameOnly(CrescoIdentity who, String name, Access access, Role role) {
        return TenantPolicy.check(who, name, access, SHARED, role, null, null);
    }

    private static void readDenied(String name, String what) {
        Decision d = queue(A1, name, Access.READ, Role.TENANT);
        assertFalse(d.allowed, what + ": a1 may read the queue " + name + " (" + d.reason + ")");
    }

    @Test
    void aNodeReadsItsOwnInbox() {
        assertTrue(queue(A1, "T.t1.rgA_a1", Access.READ, Role.TENANT).allowed, "a node's own inbox");
        assertTrue(queue(A1, "T.t1.old_a1", Access.READ, Role.TENANT).allowed, "the inbox of its former address (region_alias)");
        assertTrue(queue(A1, "T.t1.site-7_a1", Access.READ, Role.TENANT).allowed, "a former region with a '-'");
        // names with dots: a region us.east, an agent a.b
        CrescoIdentity dotted = CrescoIdentity.of("t1", "us.east", "a.b", null);
        assertTrue(queue(dotted, "T.t1.us.east_a.b", Access.READ, Role.TENANT).allowed);
        assertFalse(queue(dotted, "T.t1.us.east_b", Access.READ, Role.TENANT).allowed, "agent b is not agent a.b");
        assertFalse(queue(CrescoIdentity.of("t1", "us.east", "b", null), "T.t1.us.east_a.b", Access.READ, Role.TENANT).allowed,
                "agent a.b's inbox ends in 'b' but is not agent b's");
        // a tenant's node in a region of another tenant's controller: its own tenant's namespace, its own inbox
        assertTrue(queue(CrescoIdentity.of("t2", "rgA", "b1", null), "T.t2.rgA_b1", Access.READ, Role.TENANT).allowed);
    }

    @Test
    void aNodeReadsNoOtherNodesInbox() {
        readDenied("T.t1.rgA_rc", "its controller's inbox");
        readDenied("T.t1.rgA_a2", "a sibling's inbox");
        readDenied("T.t1.rgG_gc", "the global's inbox");
        readDenied("T.t1.rgA_rc_plugin-0", "a plugin-suffixed form of its controller's inbox");
        readDenied("T.t1.rgA_a1_plugin-0", "a plugin-suffixed form of its own inbox (no node reads such a queue)");
        readDenied("T.t1.rgA_a1.plugin-0", "a plugin-suffixed form with a dot");
        readDenied("T.t1.rgA_a1.>", "everything below its own inbox's name");
        readDenied("T.t1.old_a2", "the alias inbox of another node");
        readDenied("T.t1.old_rc", "the alias form of its controller's name");
        readDenied("T.t1.rgA_xa1", "an agent whose name ends in its own");
        readDenied("T.t1.rgA_x-a1", "an agent whose name ends in -<its own>");
        readDenied("T.t1.rgA_x.a1", "an agent whose name ends in .<its own>");
        readDenied("T.t1.work", "a queue that is no inbox");
        readDenied("T.t1.a1", "a name without a region");
        readDenied("T.t1._a1", "an empty region");
    }

    @Test
    void theAliasFormIsNoWayAroundTheRule() {
        // the former region is a plain region name: nothing that could hold a second address, a pattern or a path
        readDenied("T.t1.rgA_rc_a1", "an alias that is another node's whole address");
        readDenied("T.t1.x.y_a1", "an alias with a dot");
        readDenied("T.t1.*_a1", "an alias that is a wildcard character");
        readDenied("T.t1.>_a1", "an alias that is a wildcard character");
        readDenied("T.t1.*.rgA_a1", "a wildcard segment before its own inbox's name");
        readDenied("T.t1.rgA_rc,T.t1.rgA_a1", "a composite that ends in its own inbox");
        readDenied("T.t1.old a_a1", "an alias with a space");
        assertFalse(TenantPolicy.isOwnInbox(A1, "T.t1.rgA_rc"));
        assertFalse(TenantPolicy.isOwnInbox(A1, "T.t2.rgA_a1"), "the same address in another tenant's namespace");
        assertFalse(TenantPolicy.isOwnInbox(A1, "T.t11.rgA_a1"), "a tenant whose name begins with its own");
        assertFalse(TenantPolicy.isOwnInbox(A1, "rgA_a1"), "the raw name is the un-namespaced rule's");
        assertFalse(TenantPolicy.isOwnInbox(null, "T.t1.rgA_a1"));
        assertFalse(TenantPolicy.isOwnInbox(A1, null));
        assertTrue(TenantPolicy.isOwnInbox(A1, "T.t1.rgA_a1"));
        // an identity whose names are not Cresco names (a certificate read with peer_identity_mode=off) has no inbox to read:
        // its "own inbox" could be a pattern
        CrescoIdentity odd = CrescoIdentity.of("t1", "rgA", "x.>", null);
        assertFalse(queue(odd, "T.t1.rgA_x.>", Access.READ, Role.TENANT).allowed, "an agent name that ends in a wildcard");
    }

    @Test
    void noPatternOrCompositeOverInboxes() {
        readDenied("T.t1.>", "every destination of the tenant");
        readDenied("T.t1.*", "every one-segment destination of the tenant");
        readDenied("T.t1.rgA_*", "a pattern-like name");
        readDenied("T.t1.*.>", "a pattern");
        readDenied("T.*.rgA_a1", "its inbox in every tenant's namespace (a superuser tenant's node reads that)");
        readDenied("T.>", "every tenant");
        readDenied("T.t1.rgA_a1,T.t1.rgA_rc", "a composite of its own inbox and its controller's");
    }

    @Test
    void writingIsAsBefore() {
        for (String name : List.of("T.t1.rgA_rc", "T.t1.rgA_a2", "T.t1.rgA_a1", "T.t1.old_a2", "T.t1.rgG_gc", "T.t1.work", "T.t1.>",
                "T.t2.rgA_a1", "T.*.rgA_a1", "rgA_rc", "rgA_a1", "t1.stream", "ActiveMQ.DLQ", "elsewhere")) {
            Decision before = nameOnly(A1, name, Access.WRITE, Role.TENANT), now = queue(A1, name, Access.WRITE, Role.TENANT);
            assertEquals(before.allowed, now.allowed, "WRITE of the queue " + name);
            assertEquals(before.reason, now.reason, "WRITE of the queue " + name);
        }
        assertTrue(queue(A1, "T.t1.rgA_rc", Access.WRITE, Role.TENANT).allowed, "a node writes its controller's inbox");
        assertTrue(queue(A1, "T.t1.rgA_a2", Access.WRITE, Role.TENANT).allowed, "a node writes a sibling's inbox");
    }

    @Test
    void aSuperuserTenantsNodeAndInternalReadAsBefore() {
        CrescoIdentity op = CrescoIdentity.of("cresco-system", "rgG", "op", null);
        for (Role role : List.of(Role.SUPERUSER, Role.INTERNAL)) {
            for (String name : List.of("T.*.rgG_op", "T.t1.rgA_rc", "T.t1.>", "T.cresco-system.rgG_gc", "T.>")) {
                assertTrue(queue(op, name, Access.READ, role).allowed, role + " reads the queue " + name);
            }
        }
    }

    @Test
    void topicsAreAsBefore() {
        for (String name : List.of("T.t1.agent.event", "T.t1.region.event", "T.t1.global.event", "T.t1.global.event.3", "T.t1.>", "T.t1.*",
                "T.t1.rgA_rc", "T.t2.agent.event", "T.*.agent.event", "agent.event", "t1.stream", "ActiveMQ.Advisory.Consumer.Queue.T.t1.rgA_rc")) {
            for (Access access : Access.values()) {
                Decision before = nameOnly(A1, name, access, Role.TENANT);
                Decision now = TenantPolicy.check(A1, name, access, SHARED, Role.TENANT, null, null, false);
                assertEquals(before.allowed, now.allowed, access + " of the topic " + name);
                assertEquals(before.reason, now.reason, access + " of the topic " + name);
            }
        }
        assertTrue(TenantPolicy.check(A1, "T.t1.agent.event", Access.READ, SHARED, Role.TENANT, null, null, false).allowed);
        assertTrue(TenantPolicy.check(A1, "T.t1.>", Access.READ, SHARED, Role.TENANT, null, null, false).allowed,
                "a tenant's node still reads every topic of its tenant");
    }

    @Test
    void whatLiesOutsideTheNamespacedBranchIsAsBefore() {
        // another tenant's namespace: refused, unless the operator named a sink that says read
        assertFalse(queue(A1, "T.t2.rgA_a1", Access.READ, Role.TENANT).allowed);
        List<TenantPolicy.CrossTenantSink> sinks = TenantPolicy.CrossTenantSink.parse("t1->T.t2.*:read");
        assertTrue(TenantPolicy.check(A1, "T.t2.rgB_b1", Access.READ, SHARED, Role.TENANT, sinks, null, true).allowed,
                "a named cross-tenant read sink is the operator's explicit grant, as before");
        // the un-namespaced names (an earlier build's node in the upgrade window reads its raw inbox)
        for (String name : List.of("rgA_a1", "rgA_rc", "rgA_a2", "t1.stream", "t1", "elsewhere", "ActiveMQ.DLQ", "ActiveMQ.Advisory.TempQueue")) {
            Decision before = nameOnly(A1, name, Access.READ, Role.TENANT), now = queue(A1, name, Access.READ, Role.TENANT);
            assertEquals(before.allowed, now.allowed, "READ of the queue " + name);
            assertEquals(before.reason, now.reason, "READ of the queue " + name);
        }
        assertTrue(queue(A1, "rgA_a1", Access.READ, Role.TENANT).allowed, "its raw inbox");
        assertFalse(queue(A1, "rgA_rc", Access.READ, Role.TENANT).allowed, "its controller's raw inbox");
    }
}
