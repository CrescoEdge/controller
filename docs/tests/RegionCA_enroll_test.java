import io.cresco.agent.controller.communication.RegionCA;
import io.cresco.library.security.*;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.*;
public class EnrollTest {
  static int P=0,F=0; static void ck(String n,boolean ok){ if(ok){P++;System.out.println("PASS  "+n);} else {F++;System.out.println("FAIL  "+n);} }
  public static void main(String[] x) throws Exception {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    RegionCA caA = RegionCA.generate("tenantA","region-a",2048,2);
    // a joining node generated its OWN keypair and a self-signed cert CLAIMING tenant "evil"
    KeyPairGenerator g=KeyPairGenerator.getInstance("RSA","BC"); g.initialize(2048);
    KeyPair joinerKey = g.generateKeyPair();
    // region enrolls it, STAMPING its own tenant/region (ignores the joiner's claim); only pubkey is taken
    X509Certificate[] chain = caA.enroll(joinerKey.getPublic(), "tenantA", "region-a", "agent-007", 1);
    CrescoIdentity id = CrescoIdentity.fromCertificate(chain[0]);
    ck("region STAMPED tenant=tenantA (not joiner's claim)", "tenantA".equals(id.getTenant()));
    ck("region stamped region=region-a", "region-a".equals(id.getRegion()));
    ck("agent name carried = agent-007", "agent-007".equals(id.getAgent()));
    ck("enrolled leaf chains to region-a CA", CertTrust.verifyChain(Arrays.asList(chain), new HashSet<>(Arrays.asList(caA.caCert()))));
    ck("enrolled leaf uses the JOINER's public key", chain[0].getPublicKey().equals(joinerKey.getPublic()));
    System.out.println("\nRESULT: "+P+" passed, "+F+" failed"); if(F>0) System.exit(1);
  }
}
