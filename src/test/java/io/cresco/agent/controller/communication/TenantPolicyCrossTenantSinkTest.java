package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * W-GFS-1: named cross-tenant sinks are the only way across the tenant boundary (short of SUPERUSER),
 * they are explicit, and they never widen READ unless they say so.
 */
public class TenantPolicyCrossTenantSinkTest {

    private static final Set<String> SHARED = new HashSet<>(Collections.singletonList("agent.event"));
    private static final CrescoIdentity SITE_A = CrescoIdentity.of("siteA", "rgnA", "ctlA", "u1");
    private static final CrescoIdentity CORE = CrescoIdentity.of("gfs-federation", "global-region", "global-controller", "u2");

    private static TenantPolicy.Decision check(CrescoIdentity who, String dest, TenantPolicy.Access access, List<TenantPolicy.CrossTenantSink> sinks) {
        return TenantPolicy.check(who, dest, access, SHARED, TenantPolicy.Role.TENANT, sinks);
    }

    @Test
    public void withoutSinksTheBoundaryIsStrict() {
        assertTrue(check(SITE_A, "T.siteA.rgnA_ctlA", TenantPolicy.Access.WRITE, null).allowed);
        assertFalse(check(SITE_A, "T.gfs-federation.global-region_global-controller", TenantPolicy.Access.WRITE, null).allowed);
        assertFalse(check(SITE_A, "T.siteB.rgnB_ctlB", TenantPolicy.Access.WRITE, null).allowed);
        assertFalse(check(SITE_A, "T.siteB.rgnB_ctlB", TenantPolicy.Access.WRITE, Collections.emptyList()).allowed);
    }

    @Test
    public void gfsFlowsAreExpressibleAsNamedSinks() {
        List<TenantPolicy.CrossTenantSink> sinks = TenantPolicy.CrossTenantSink.parse(
                "*->T.gfs-federation.*:write ; gfs-federation->T.*:write ; *->T.*.global.event:write");
        assertEquals(3, sinks.size());
        // F1 site -> core inbox
        assertTrue(check(SITE_A, "T.gfs-federation.global-region_global-controller", TenantPolicy.Access.WRITE, sinks).allowed);
        // F2 core -> site inbox
        assertTrue(check(CORE, "T.siteB.rgnB_ctlB", TenantPolicy.Access.WRITE, sinks).allowed);
        // F3 site -> other site's dataplane topic (fragments)
        assertTrue(check(SITE_A, "T.siteB.global.event", TenantPolicy.Access.WRITE, sinks).allowed);
        // but NOT another site's inbox (no rule names it)
        assertFalse(check(SITE_A, "T.siteB.rgnB_ctlB", TenantPolicy.Access.WRITE, sinks).allowed);
        // and never READ across the boundary unless a rule says so
        assertFalse(check(SITE_A, "T.gfs-federation.global-region_global-controller", TenantPolicy.Access.READ, sinks).allowed);
        assertFalse(check(SITE_A, "T.siteB.global.event", TenantPolicy.Access.READ, sinks).allowed);
        assertFalse(check(CORE, "T.siteB.rgnB_ctlB", TenantPolicy.Access.READ, sinks).allowed);
    }

    @Test
    public void readAndAnyAccessAreExplicit() {
        List<TenantPolicy.CrossTenantSink> sinks = TenantPolicy.CrossTenantSink.parse("siteB->T.siteA.global.event:read, siteC->T.siteA.*:any");
        assertEquals(2, sinks.size());
        CrescoIdentity siteB = CrescoIdentity.of("siteB", "rgnB", "ctlB", "u3");
        CrescoIdentity siteC = CrescoIdentity.of("siteC", "rgnC", "ctlC", "u4");
        assertTrue(check(siteB, "T.siteA.global.event", TenantPolicy.Access.READ, sinks).allowed);
        assertFalse(check(siteB, "T.siteA.global.event", TenantPolicy.Access.WRITE, sinks).allowed);
        assertTrue(check(siteC, "T.siteA.rgnA_ctlA", TenantPolicy.Access.READ, sinks).allowed);
        assertTrue(check(siteC, "T.siteA.rgnA_ctlA", TenantPolicy.Access.WRITE, sinks).allowed);
        // a rule for siteC grants nothing to siteA elsewhere
        assertFalse(check(SITE_A, "T.siteC.rgnC_ctlC", TenantPolicy.Access.WRITE, sinks).allowed);
    }

    @Test
    public void malformedEntriesAreDroppedAndOwnSubtreeUnaffected() {
        List<TenantPolicy.CrossTenantSink> sinks = TenantPolicy.CrossTenantSink.parse("nonsense; ->T.x; a->; ;;");
        assertTrue(sinks.isEmpty());
        assertTrue(check(SITE_A, "T.siteA.anything.at.all", TenantPolicy.Access.READ, sinks).allowed);
        assertFalse(check(SITE_A, "T.siteB.anything", TenantPolicy.Access.WRITE, sinks).allowed);
    }
}
