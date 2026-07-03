#!/usr/bin/env bash
# Phase-0 distributed-security foundation test (see docs/distributed-identity-trust-design.md).
# Proves the io.cresco.library.security primitives against REAL keytool-issued certs:
#   - CrescoIdentity: parse tenant/region/agent/uid from a leaf cert DN (agents identify each other)
#   - MessageSigner:  sign/verify a payload; reject tampered bytes and wrong signer (end-to-end auth)
#   - CertTrust:      a leaf chains to a TRUSTED region CA is accepted; a leaf from an UNTRUSTED
#                     region CA is rejected (the distributed regional-CA trust boundary)
# Self-contained: regenerates certs, compiles the library if needed, runs the assertions.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
LIBDIR="$REPO/code/library"
CP="$LIBDIR/target/classes"
D="$(mktemp -d /tmp/cresco-sec.XXXX)"
trap 'rm -rf "$D"' EXIT

if [ ! -d "$CP/io/cresco/library/security" ]; then
  echo "Building library (security classes not found in target/classes)..."
  ( cd "$LIBDIR" && mvn -q -o package -DskipTests )
fi

KS="$D/ks.p12"
SP="-storepass changeit -keypass changeit -keyalg RSA -keysize 2048 -validity 2 -keystore $KS -storetype PKCS12"
# NB: ${=SP} forces zsh word-splitting; harmless in bash. Keep this test runnable under either shell.
keytool -genkeypair -alias rca   -dname "CN=regionCA-r1,OU=r1,O=t1"         -ext bc:c ${=SP} >/dev/null 2>&1 || keytool -genkeypair -alias rca   -dname "CN=regionCA-r1,OU=r1,O=t1"         -ext bc:c $SP >/dev/null 2>&1
keytool -genkeypair -alias leaf  -dname "CN=agentA,OU=r1,O=t1,UID=node-123" ${=SP} >/dev/null 2>&1 || keytool -genkeypair -alias leaf  -dname "CN=agentA,OU=r1,O=t1,UID=node-123" $SP >/dev/null 2>&1
keytool -genkeypair -alias oca   -dname "CN=regionCA-r2,OU=r2,O=t2"         -ext bc:c ${=SP} >/dev/null 2>&1 || keytool -genkeypair -alias oca   -dname "CN=regionCA-r2,OU=r2,O=t2"         -ext bc:c $SP >/dev/null 2>&1
keytool -genkeypair -alias leaf2 -dname "CN=intruder,OU=r2,O=t2"            ${=SP} >/dev/null 2>&1 || keytool -genkeypair -alias leaf2 -dname "CN=intruder,OU=r2,O=t2"            $SP >/dev/null 2>&1
keytool -certreq -alias leaf  -storepass changeit -keystore "$KS" -file "$D/leaf.csr"  >/dev/null 2>&1
keytool -gencert -alias rca -ext bc:0 -storepass changeit -keystore "$KS" -infile "$D/leaf.csr"  -outfile "$D/leaf.cer"  >/dev/null 2>&1
keytool -certreq -alias leaf2 -storepass changeit -keystore "$KS" -file "$D/leaf2.csr" >/dev/null 2>&1
keytool -gencert -alias oca -ext bc:0 -storepass changeit -keystore "$KS" -infile "$D/leaf2.csr" -outfile "$D/leaf2.cer" >/dev/null 2>&1
keytool -exportcert -alias rca -storepass changeit -keystore "$KS" -file "$D/rca.cer" >/dev/null 2>&1
keytool -exportcert -alias oca -storepass changeit -keystore "$KS" -file "$D/oca.cer" >/dev/null 2>&1

cat > "$D/SecTest.java" <<'EOF'
import io.cresco.library.security.*;
import java.io.*; import java.security.*; import java.security.cert.*; import java.util.*;
public class SecTest {
  static X509Certificate load(String p) throws Exception {
    try (InputStream in = new FileInputStream(p)) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
    }
  }
  static int PASS=0, FAILN=0;
  static void check(String n, boolean ok){ if(ok){PASS++;System.out.println("PASS  "+n);} else {FAILN++;System.out.println("FAIL  "+n);} }
  public static void main(String[] a) throws Exception {
    String d = a[0];
    X509Certificate leaf=load(d+"/leaf.cer"), rca=load(d+"/rca.cer"), leaf2=load(d+"/leaf2.cer"), oca=load(d+"/oca.cer");
    KeyStore ks=KeyStore.getInstance("PKCS12");
    try(InputStream in=new FileInputStream(d+"/ks.p12")){ ks.load(in,"changeit".toCharArray()); }
    PrivateKey leafKey=(PrivateKey)ks.getKey("leaf","changeit".toCharArray());

    CrescoIdentity id=CrescoIdentity.fromCertificate(leaf);
    check("identity tenant=t1", "t1".equals(id.getTenant()));
    check("identity region=r1", "r1".equals(id.getRegion()));
    check("identity agent=agentA", "agentA".equals(id.getAgent()));
    check("identity uid=node-123", "node-123".equals(id.getUid()));
    check("agentPath r1_agentA", "r1_agentA".equals(id.getAgentPath()));
    check("DN roundtrip", CrescoIdentity.fromDN(CrescoIdentity.of("t1","r1","agentA","node-123").toX500Name()).equals(id));

    byte[] msg="control-plane payload".getBytes("UTF-8");
    byte[] sig=MessageSigner.sign(msg,leafKey);
    check("sign produced signature", sig!=null && sig.length>0);
    check("verify accepts good sig", MessageSigner.verify(msg,sig,leaf));
    check("verify REJECTS tampered payload", !MessageSigner.verify("control-plane payloaX".getBytes("UTF-8"),sig,leaf));
    check("verify REJECTS wrong signer", !MessageSigner.verify(msg,sig,leaf2));
    check("base64 sign/verify round-trip", MessageSigner.verifyBase64(msg, MessageSigner.signBase64(msg,leafKey), leaf));

    Set<X509Certificate> trust=new HashSet<>(Arrays.asList(rca)); // trust ONLY region r1's CA
    check("leaf chains to trusted region CA", CertTrust.verifyChain(Arrays.asList(leaf,rca),trust));
    check("intruder leaf REJECTED (untrusted region CA)", !CertTrust.verifyChain(Arrays.asList(leaf2,oca),trust));
    CrescoIdentity vid=CertTrust.verifiedIdentity(Arrays.asList(leaf,rca),trust);
    check("verifiedIdentity returns agentA for trusted", vid!=null && "agentA".equals(vid.getAgent()));
    check("verifiedIdentity null for untrusted", CertTrust.verifiedIdentity(Arrays.asList(leaf2,oca),trust)==null);

    System.out.println("\nRESULT: "+PASS+" passed, "+FAILN+" failed");
    if(FAILN>0) System.exit(1);
  }
}
EOF
javac -cp "$CP" -d "$D" "$D/SecTest.java"
java -cp "$CP:$D" SecTest "$D"
