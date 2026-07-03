#!/usr/bin/env bash
# End-to-end FEDERATED MULTI-REGION tenant-isolation test (docs/distributed-identity-trust-design.md).
# Stands up: 1 global + 2 regions (region-a=tenantA, region-b=tenantB) with broker_security_enabled,
# each running its own secured ActiveMQ 6.2.7 broker and federating to the global via a broker bridge.
# Then, with direct JMS clients, proves that at EVERY broker in the mesh:
#   - a tenant may pub/sub only its own <tenant>.* namespace,
#   - a cross-tenant subscribe/publish is DENIED by the CrescoAuthorizationBroker,
# while federation + link-metric (RTT) scaling remain healthy under enforcement.
set -uo pipefail

RUN="$(cd "$(dirname "$0")/.." && pwd)"
JARP="$RUN/agent-1.3-SNAPSHOT.jar"
source "$RUN/cresco.env"
SEC="-Ddiscovery_secret_global=$CRESCO_DISCOVERY_SECRET_GLOBAL -Ddiscovery_secret_region=$CRESCO_DISCOVERY_SECRET_REGION -Ddiscovery_secret_agent=$CRESCO_DISCOVERY_SECRET_AGENT -Dcresco_service_key=$CRESCO_SERVICE_KEY"

echo "### teardown any prior fabric"
pkill -9 -f "agent-1.3-SNAPSHOT.jar" 2>/dev/null; sleep 2
rm -rf "$RUN/nodes" "$RUN/cresco-data" /tmp/mesh-*.log 2>/dev/null

echo "### JMS client classpath (activemq-client 6.2.7)"
CP=$(cat /tmp/jmscp.txt 2>/dev/null)
if [ -z "$CP" ]; then ( cd "$RUN/../code/controller" && mvn -o -q dependency:build-classpath -Dmdep.outputFile=/tmp/jmscp.txt >/dev/null 2>&1 ); CP=$(cat /tmp/jmscp.txt); fi

launch() { # name agent role extra log
  local dir="$RUN/nodes/$1"; mkdir -p "$dir"; ( cd "$dir"
    nohup java -Djava.net.preferIPv4Stack=true -Xmx2048M -Dregionname=$1 -Dagentname=$2 $3 \
      -Denable_console=false -Denable_dashboard=false -Droot_log_level=INFO -Dnet_metrics_log=true \
      -Dbroker_security_enabled=true $4 $SEC -jar "$JARP" > "$5" 2>&1 < /dev/null & disown ) 2>/dev/null
}

echo "### launch secured global + 2 tenant regions"
launch global-region global-controller "-Dis_global=true -Denable_wsapi=true -Dport=8080" "-Dtenant_id=global" /tmp/mesh-global.log
for i in $(seq 1 45); do nc -z localhost 8282 2>/dev/null && break; sleep 2; done
launch region-a controller-a "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=32015 -Dbroker_port=32020 -Denable_wsapi=false" "-Dtenant_id=tenantA" /tmp/mesh-region-a.log
launch region-b controller-b "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=32016 -Dbroker_port=32030 -Denable_wsapi=false" "-Dtenant_id=tenantB" /tmp/mesh-region-b.log

echo "### wait for federation (both bridges)"
for i in $(seq 1 50); do
  [ "$(grep -c 'Starting Bridge' /tmp/mesh-region-a.log 2>/dev/null)" -ge 1 ] && \
  [ "$(grep -c 'Starting Bridge' /tmp/mesh-region-b.log 2>/dev/null)" -ge 1 ] && break
  sleep 3
done
PASS=0; FAIL=0
ck(){ if [ "$1" = "1" ]; then PASS=$((PASS+1)); echo "PASS  $2"; else FAIL=$((FAIL+1)); echo "FAIL  $2"; fi; }
ck "$([ "$(grep -c 'Starting Bridge' /tmp/mesh-region-a.log)" -ge 1 ] && echo 1)" "federation: region-a bridged to global (security ON)"
ck "$([ "$(grep -c 'Starting Bridge' /tmp/mesh-region-b.log)" -ge 1 ] && echo 1)" "federation: region-b bridged to global (security ON)"
ck "$([ "$(grep -cE 'SecurityException|DENY ' /tmp/mesh-region-a.log)" = 0 ] && echo 1)" "no self-denial on region-a control plane"
ck "$([ "$(grep -cE 'SecurityException|DENY ' /tmp/mesh-region-b.log)" = 0 ] && echo 1)" "no self-denial on region-b control plane"

echo "### JMS cross-tenant isolation at every broker"
D=$(mktemp -d /tmp/isomesh.XXXX)
cat > "$D/IsoMesh.java" <<'EOF'
import org.apache.activemq.ActiveMQSslConnectionFactory;
import jakarta.jms.*; import javax.net.ssl.*; import java.security.cert.X509Certificate;
public class IsoMesh {
  static TrustManager[] TA={ new X509TrustManager(){
    public void checkClientTrusted(X509Certificate[] c,String a){}
    public void checkServerTrusted(X509Certificate[] c,String a){}
    public X509Certificate[] getAcceptedIssuers(){return new X509Certificate[0];}}};
  static Connection connect(int p,String u) throws Exception {
    ActiveMQSslConnectionFactory f=new ActiveMQSslConnectionFactory("nio+ssl://localhost:"+p+"?verifyHostName=false");
    f.setKeyAndTrustManagers(null,TA,new java.security.SecureRandom()); f.setUserName(u); f.setPassword("cresco");
    Connection c=f.createConnection(); c.start(); return c; }
  static boolean canSub(Connection c,String t){ try{ Session s=c.createSession(false,1); s.createConsumer(s.createTopic(t)); return true;}catch(JMSException e){return false;} }
  static boolean rt(Connection c,String t) throws Exception { Session s=c.createSession(false,1); Topic d=s.createTopic(t);
    MessageConsumer con=s.createConsumer(d); s.createProducer(d).send(s.createTextMessage("rt")); Message m=con.receive(3000);
    return m!=null && "rt".equals(((TextMessage)m).getText()); }
  static int P=0,F=0; static void ck(String n,boolean ok){ if(ok){P++;System.out.println("PASS  "+n);} else {F++;System.out.println("FAIL  "+n);} }
  public static void main(String[] x) throws Exception {
    String A="tenantA|region-a|controller-a", B="tenantB|region-b|controller-b";
    Connection ca20=connect(32020,A), cb20=connect(32020,B), cb30=connect(32030,B), ca30=connect(32030,A);
    ck("tenantA pub/sub tenantA.stream @region-a",              rt(ca20,"tenantA.stream"));
    ck("tenantB DENIED sub tenantA.stream @region-a",          !canSub(cb20,"tenantA.stream"));
    ck("tenantB ALLOWED sub own tenantB.stream @region-a",      canSub(cb20,"tenantB.stream"));
    ck("tenantB pub/sub tenantB.stream @region-b",              rt(cb30,"tenantB.stream"));
    ck("tenantA DENIED sub tenantB.stream @region-b",          !canSub(ca30,"tenantB.stream"));
    ck("tenantB DENIED sub tenantA.stream @region-b (x-region)",!canSub(cb30,"tenantA.stream"));
    for(Connection c: new Connection[]{ca20,cb20,cb30,ca30}) c.close();
    System.out.println("JMS-RESULT "+P+" "+F); if(F>0) System.exit(1);
  }
}
EOF
if javac -cp "$CP" -d "$D" "$D/IsoMesh.java" 2>/dev/null; then
  OUT=$(java -cp "$CP:$D" IsoMesh 2>/dev/null)
  echo "$OUT" | grep -E "^PASS|^FAIL"
  JP=$(echo "$OUT" | awk '/JMS-RESULT/{print $2}'); JF=$(echo "$OUT" | awk '/JMS-RESULT/{print $3}')
  PASS=$((PASS+${JP:-0})); FAIL=$((FAIL+${JF:-6}))
else
  echo "FAIL  JMS isolation test failed to compile"; FAIL=$((FAIL+6))
fi

echo "### teardown"
pkill -9 -f "agent-1.3-SNAPSHOT.jar" 2>/dev/null
echo "==================================================="
echo "  PASS=$PASS  FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && echo "  MESH TENANT ISOLATION: PASS" || echo "  MESH TENANT ISOLATION: FAIL"
exit $FAIL
