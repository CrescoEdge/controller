package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** A tenant name has no '.': tenant t1's namespace T.t1. must not be a prefix of another tenant's (t1.x). */
class TenantNameTest {

    @Test
    void aTenantNameWithADotIsNeverAnIdentity() throws Exception {
        assertTrue(CrescoNames.isValidTenant("t1"));
        assertTrue(CrescoNames.isValidTenant("tenant-a"));
        assertFalse(CrescoNames.isValidTenant("t1.x"));
        assertTrue(CrescoNames.isValid("rg.a"), "regions and agents keep '.'");
        assertFalse(CrescoNames.isValid(CrescoIdentity.of("t1.x", "rgA", "b", null)));
        TlsTestSupport.provider();
        TlsTestSupport.Id dotted = TlsTestSupport.selfRooted("r", "t1.x", "rgA", "b");
        assertNull(CrescoNames.identityOf(dotted.chain[0]), "a certificate naming tenant t1.x was read as an identity");
        assertThrows(IllegalArgumentException.class, () -> CrescoNames.x500(CrescoIdentity.of("t1.x", "rgA", "b", null)));
        assertThrows(IllegalArgumentException.class, () -> RegionCA.generate("t1.x", "rgA", 2048, 1));
        assertFalse(new PeerTrustRegistry().bind("t1.x", "rgA", "b", TlsTestSupport.rsa().getPublic(), "test").ok);
        assertTrue(CrescoNames.whyInvalid("tenant_id", "t1.x").contains("no '.'"));
        TenantPolicy.Decision d = TenantPolicy.check(CrescoIdentity.of("t1.x", "rgA", "b", null), "T.t1.x.inbox",
                TenantPolicy.Access.WRITE, Set.of(), TenantPolicy.Role.TENANT);
        assertFalse(d.allowed, "a dotted tenant name was given a namespace");
    }
}
