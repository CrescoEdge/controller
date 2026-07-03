#!/usr/bin/env bash
# Mutual-TLS cryptographic identity binding test (docs/distributed-identity-trust-design.md §3.3).
# Launches ONE secured broker with broker_require_client_auth=true and proves:
#   1. a client presenting NO certificate is rejected by the mutual-TLS handshake,
#   2. a client presenting a tenantB certificate but a SPOOFED "tenantA" username is authenticated
#      as tenantB (the certificate DN is authoritative and overrides the asserted username) and is
#      therefore DENIED tenantA's namespace while ALLOWED its own.
# This is the anti-spoofing proof: identity is bound to the cert (needs the private key), not the username.
set -uo pipefail
RUN="$(cd "$(dirname "$0")/.." && pwd)"; JARP="$RUN/agent-1.3-SNAPSHOT.jar"; source "$RUN/cresco.env"
PKI=/tmp/mtls-pki; CLI=/tmp/mtls-client; rm -rf "$PKI" "$CLI"; mkdir -p "$PKI" "$CLI"

echo "### provision a trusted tenantB client certificate (identity in DN)"
keytool -genkeypair -alias clientb -dname "CN=b1,OU=region-b,O=tenantB" -keyalg RSA -keysize 2048 -validity 2 \
  -keystore "$CLI/clientB.p12" -storetype PKCS12 -storepass changeit -keypass changeit >/dev/null 2>&1
keytool -exportcert -alias clientb -keystore "$CLI/clientB.p12" -storepass changeit -file "$PKI/tenantB_b1.cer" -rfc >/dev/null 2>&1

echo "### launch a mutual-TLS secured broker"
pkill -9 -f "agent-1.3-SNAPSHOT.jar" 2>/dev/null; sleep 2; rm -rf "$RUN/nodes" "$RUN/cresco-data" /tmp/mtls.log 2>/dev/null
SEC="-Ddiscovery_secret_global=$CRESCO_DISCOVERY_SECRET_GLOBAL -Ddiscovery_secret_region=$CRESCO_DISCOVERY_SECRET_REGION -Ddiscovery_secret_agent=$CRESCO_DISCOVERY_SECRET_AGENT -Dcresco_service_key=$CRESCO_SERVICE_KEY"
D="$RUN/nodes/mtls-global"; mkdir -p "$D"; ( cd "$D"; nohup java -Djava.net.preferIPv4Stack=true -Xmx2048M -Dport=8080 \
  -Dregionname=global-region -Dagentname=global-controller -Dis_global=true -Denable_wsapi=true \
  -Denable_console=false -Denable_dashboard=false -Droot_log_level=INFO \
  -Dbroker_security_enabled=true -Dbroker_require_client_auth=true -Dtenant_id=global \
  -Dpublic_key_directory="$PKI" $SEC -jar "$JARP" > /tmp/mtls.log 2>&1 </dev/null & disown ) 2>/dev/null
for i in $(seq 1 45); do grep -q "mutual-TLS ENABLED" /tmp/mtls.log 2>/dev/null && break; sleep 2; done
for i in $(seq 1 20); do nc -z localhost 32010 2>/dev/null && break; sleep 1; done

echo "### JMS mutual-TLS assertions"
CP=$(cat /tmp/jmscp.txt 2>/dev/null); [ -z "$CP" ] && ( cd "$RUN/../code/controller" && mvn -o -q dependency:build-classpath -Dmdep.outputFile=/tmp/jmscp.txt >/dev/null 2>&1 ) && CP=$(cat /tmp/jmscp.txt)
T=$(mktemp -d /tmp/mtlsjava.XXXX)
cat > "$T/IsoMtls.java" <<'EOF'
import org.apache.activemq.ActiveMQSslConnectionFactory;
import jakarta.jms.*; import javax.net.ssl.*; import java.io.*; import java.security.*; import java.security.cert.X509Certificate;
public class IsoMtls {
  static TrustManager[] TA={ new X509TrustManager(){ public void checkClientTrusted(X509Certificate[] c,String a){}
    public void checkServerTrusted(X509Certificate[] c,String a){} public X509Certificate[] getAcceptedIssuers(){return new X509Certificate[0];}}};
  static KeyManager[] km(String p12) throws Exception { KeyStore ks=KeyStore.getInstance("PKCS12"); ks.load(new FileInputStream(p12),"changeit".toCharArray());
    KeyManagerFactory f=KeyManagerFactory.getInstance("SunX509"); f.init(ks,"changeit".toCharArray()); return f.getKeyManagers(); }
  static Connection connect(KeyManager[] kms,String user) throws Exception { ActiveMQSslConnectionFactory f=new ActiveMQSslConnectionFactory("nio+ssl://localhost:32010?verifyHostName=false");
    f.setKeyAndTrustManagers(kms,TA,new SecureRandom()); if(user!=null){f.setUserName(user);f.setPassword("cresco");} Connection c=f.createConnection(); c.start(); return c; }
  static boolean canSub(Connection c,String t){ try{ Session s=c.createSession(false,1); s.createConsumer(s.createTopic(t)); return true;}catch(JMSException e){return false;} }
  static int P=0,F=0; static void ck(String n,boolean ok){ if(ok){P++;System.out.println("PASS  "+n);} else {F++;System.out.println("FAIL  "+n);} }
  public static void main(String[] x) throws Exception {
    boolean rejected=false; try{ connect(null,"tenantA|r1|a1"); }catch(Exception e){ rejected=true; }
    ck("client WITHOUT certificate REJECTED by mutual TLS", rejected);
    Connection cb=connect(km("/tmp/mtls-client/clientB.p12"),"tenantA|r1|a1"); // spoofed username
    ck("tenantB-cert client spoofing tenantA username DENIED tenantA.stream", !canSub(cb,"tenantA.stream"));
    ck("tenantB-cert client ALLOWED its real tenantB.stream",                  canSub(cb,"tenantB.stream"));
    cb.close(); System.out.println("RESULT "+P+" "+F); if(F>0) System.exit(1);
  }
}
EOF
javac -cp "$CP" -d "$T" "$T/IsoMtls.java" 2>/dev/null
OUT=$(java -cp "$CP:$T" IsoMtls 2>/dev/null); echo "$OUT" | grep -E "^PASS|^FAIL"
echo "### broker-side proof"; grep -E "mTLS identity bound|DENY READ 'tenantA" /tmp/mtls.log 2>/dev/null | tail -2
P=$(echo "$OUT"|awk '/RESULT/{print $2}'); F=$(echo "$OUT"|awk '/RESULT/{print $3}')
pkill -9 -f "agent-1.3-SNAPSHOT.jar" 2>/dev/null; rm -rf "$T"
echo "==================================================="; echo "  PASS=${P:-0}  FAIL=${F:-3}"
[ "${F:-3}" = "0" ] && echo "  mTLS IDENTITY BINDING: PASS" || echo "  mTLS IDENTITY BINDING: FAIL"
exit ${F:-3}
