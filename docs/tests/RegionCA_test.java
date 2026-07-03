import io.cresco.agent.controller.communication.RegionCA;
import io.cresco.library.security.*;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;
public class CATest {
  static int P=0,F=0; static void ck(String n,boolean ok){ if(ok){P++;System.out.println("PASS  "+n);} else {F++;System.out.println("FAIL  "+n);} }
  static KeyPair leafKey() throws Exception { KeyPairGenerator g=KeyPairGenerator.getInstance("RSA","BC"); g.initialize(2048); return g.generateKeyPair(); }
  public static void main(String[] x) throws Exception {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    // Region A CA (tenantA) issues a leaf for agentA
    RegionCA caA = RegionCA.generate("tenantA","region-a",2048,2);
    KeyPair aKey = leafKey();
    X509Certificate[] chainA = caA.issueLeaf(aKey.getPublic(), CrescoIdentity.of("tenantA","region-a","agentA",null), 1);

    // Region B CA (tenantB) issues a leaf for intruder
    RegionCA caB = RegionCA.generate("tenantB","region-b",2048,2);
    KeyPair bKey = leafKey();
    X509Certificate[] chainB = caB.issueLeaf(bKey.getPublic(), CrescoIdentity.of("tenantB","region-b","b1",null), 1);

    // 1. identity is bound in the region-issued leaf
    CrescoIdentity id = CrescoIdentity.fromCertificate(chainA[0]);
    ck("issued leaf carries identity tenantA/region-a/agentA",
        id!=null && "tenantA".equals(id.getTenant()) && "region-a".equals(id.getRegion()) && "agentA".equals(id.getAgent()));

    // 2. the leaf chains to region A's CA (the O(regions) trust anchor)
    Set<X509Certificate> trustA = new HashSet<>(Arrays.asList(caA.caCert()));
    ck("agentA leaf chains to region-a CA (trusted)", CertTrust.verifyChain(Arrays.asList(chainA), trustA));

    // 3. a leaf from region B does NOT chain to region A's CA -> cross-region rejected
    ck("region-b intruder leaf REJECTED against region-a CA", !CertTrust.verifyChain(Arrays.asList(chainB), trustA));

    // 4. one-call verified identity only for the trusted region
    CrescoIdentity vA = CertTrust.verifiedIdentity(Arrays.asList(chainA), trustA);
    ck("verifiedIdentity returns agentA for region-a-signed leaf", vA!=null && "agentA".equals(vA.getAgent()));
    ck("verifiedIdentity null for region-b leaf under region-a trust", CertTrust.verifiedIdentity(Arrays.asList(chainB), trustA)==null);

    // 5. a bundle holding BOTH region CAs (the global's distributed trust) accepts both regions
    Set<X509Certificate> bundle = new HashSet<>(Arrays.asList(caA.caCert(), caB.caCert()));
    ck("federated bundle (both region CAs) accepts region-a leaf", CertTrust.verifyChain(Arrays.asList(chainA), bundle));
    ck("federated bundle (both region CAs) accepts region-b leaf", CertTrust.verifyChain(Arrays.asList(chainB), bundle));

    System.out.println("\nRESULT: "+P+" passed, "+F+" failed"); if(F>0) System.exit(1);
  }
}
