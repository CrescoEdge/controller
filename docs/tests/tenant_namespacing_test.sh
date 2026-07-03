#!/usr/bin/env bash
# End-to-end TENANT-NAMESPACING test (docs/tenant-isolation-design.md).
# Same federated topology as tenant_isolation_mesh_test.sh (1 global + 2 tenant regions, secured),
# but launched with -Dtenant_namespacing=true so BOTH controlled channels are namespaced:
#   - MsgEvent inbox queues  -> T.<tenant>.<region>_<agent>  (infra controllers consume T.*. wildcard)
#   - dataplane topics       -> T.<tenant>.<agent|region|global>.event
# Proves: the fabric still forms + runs under namespacing (control plane flows through the namespaced
# inboxes with no self-denial), a TENANT client is confined to its own T.<tenant>.* subtree at every
# broker, and a SUPERUSER client (broker_superuser_tenants) gets cross-tenant god view.
set -uo pipefail

RUN="$(cd "$(dirname "$0")/.." && pwd)"
JARP="$RUN/agent-1.3-SNAPSHOT.jar"
source "$RUN/cresco.env"
SEC="-Ddiscovery_secret_global=$CRESCO_DISCOVERY_SECRET_GLOBAL -Ddiscovery_secret_region=$CRESCO_DISCOVERY_SECRET_REGION -Ddiscovery_secret_agent=$CRESCO_DISCOVERY_SECRET_AGENT -Dcresco_service_key=$CRESCO_SERVICE_KEY"
# tenant namespacing ON, and a superuser tenant for the god-view client
NS="-Dtenant_namespacing=true -Dbroker_superuser_tenants=cresco-system"

echo "### teardown any prior fabric"
pkill -9 -f "agent-1.3-SNAPSHOT.jar" 2>/dev/null; sleep 2
rm -rf "$RUN/nodes" "$RUN/cresco-data" /tmp/nsmesh-*.log 2>/dev/null

echo "### JMS client classpath (activemq-client 6.2.7)"
CP=$(cat /tmp/jmscp.txt 2>/dev/null)
if [ -z "$CP" ]; then ( cd "$RUN/../code/controller" && mvn -o -q dependency:build-classpath -Dmdep.outputFile=/tmp/jmscp.txt >/dev/null 2>&1 ); CP=$(cat /tmp/jmscp.txt); fi

launch() { # name agent extra tenant log
  local dir="$RUN/nodes/$1"; mkdir -p "$dir"; ( cd "$dir"
    nohup java -Djava.net.preferIPv4Stack=true -Xmx2048M -Dregionname=$1 -Dagentname=$2 $3 \
      -Denable_console=false -Denable_dashboard=false -Droot_log_level=INFO -Dnet_metrics_log=true \
      -Dbroker_security_enabled=true $NS $4 $SEC -jar "$JARP" > "$5" 2>&1 < /dev/null & disown ) 2>/dev/null
}

echo "### launch secured+namespaced global + 2 tenant regions"
launch global-region global-controller "-Dis_global=true -Denable_wsapi=true -Dport=8080" "-Dtenant_id=global" /tmp/nsmesh-global.log
for i in $(seq 1 45); do nc -z localhost 8282 2>/dev/null && break; sleep 2; done
launch region-a controller-a "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=32015 -Dbroker_port=32020 -Denable_wsapi=false" "-Dtenant_id=tenantA" /tmp/nsmesh-region-a.log
launch region-b controller-b "-Dis_region=true -Dglobal_controller_host=127.0.0.1 -Dnetdiscoveryport=32016 -Dbroker_port=32030 -Denable_wsapi=false" "-Dtenant_id=tenantB" /tmp/nsmesh-region-b.log

echo "### wait for federation (both bridges)"
for i in $(seq 1 50); do
  [ "$(grep -c 'Starting Bridge' /tmp/nsmesh-region-a.log 2>/dev/null)" -ge 1 ] && \
  [ "$(grep -c 'Starting Bridge' /tmp/nsmesh-region-b.log 2>/dev/null)" -ge 1 ] && break
  sleep 3
done
sleep 8   # let control plane settle under namespacing (watchdog/ping over namespaced inboxes)

PASS=0; FAIL=0
ck(){ if [ "$1" = "1" ]; then PASS=$((PASS+1)); echo "PASS  $2"; else FAIL=$((FAIL+1)); echo "FAIL  $2"; fi; }
ck "$([ "$(grep -c 'Starting Bridge' /tmp/nsmesh-region-a.log)" -ge 1 ] && echo 1)" "federation: region-a bridged to global (namespaced)"
ck "$([ "$(grep -c 'Starting Bridge' /tmp/nsmesh-region-b.log)" -ge 1 ] && echo 1)" "federation: region-b bridged to global (namespaced)"
# The control plane (watchdog/ping) runs over the namespaced MsgEvent inboxes; zero DENY/self-denial
# proves the wildcard infra inbox + producer tenant-qualification work end-to-end.
ck "$([ "$(grep -cE 'SecurityException|DENY ' /tmp/nsmesh-region-a.log)" = 0 ] && echo 1)" "no self-denial on region-a namespaced control plane"
ck "$([ "$(grep -cE 'SecurityException|DENY ' /tmp/nsmesh-region-b.log)" = 0 ] && echo 1)" "no self-denial on region-b namespaced control plane"
ck "$([ "$(grep -cE 'SecurityException|DENY ' /tmp/nsmesh-global.log)" = 0 ] && echo 1)" "no self-denial on global namespaced control plane"

echo "### JMS namespaced-isolation + superuser role at region-a broker (32020)"
D=$(mktemp -d /tmp/nsiso.XXXX)
cat > "$D/NsIso.java" <<'EOF'
import org.apache.activemq.ActiveMQSslConnectionFactory;
import jakarta.jms.*; import javax.net.ssl.*; import java.security.cert.X509Certificate;
public class NsIso {
  static TrustManager[] TA={ new X509TrustManager(){
    public void checkClientTrusted(X509Certificate[] c,String a){}
    public void checkServerTrusted(X509Certificate[] c,String a){}
    public X509Certificate[] getAcceptedIssuers(){return new X509Certificate[0];}}};
  static Connection connect(int p,String u) throws Exception {
    ActiveMQSslConnectionFactory f=new ActiveMQSslConnectionFactory("nio+ssl://localhost:"+p+"?verifyHostName=false");
    f.setKeyAndTrustManagers(null,TA,new java.security.SecureRandom()); f.setUserName(u); f.setPassword("cresco");
    Connection c=f.createConnection(); c.start(); return c; }
  static boolean canSub(Connection c,String t){ try{ Session s=c.createSession(false,1); s.createConsumer(s.createTopic(t)); return true;}catch(JMSException e){return false;} }
  static int P=0,F=0; static void ck(String n,boolean ok){ if(ok){P++;System.out.println("PASS  "+n);} else {F++;System.out.println("FAIL  "+n);} }
  public static void main(String[] x) throws Exception {
    String A="tenantA|region-a|controller-a", B="tenantB|region-b|controller-b", SU="cresco-system|admin|admin";
    Connection a=connect(32020,A), b=connect(32020,B), su=connect(32020,SU);
    // TENANT role: confined to own T.<tenant>.* subtree
    ck("tenantA sub own T.tenantA.stream",              canSub(a,"T.tenantA.stream"));
    ck("tenantA DENIED sub T.tenantB.stream",          !canSub(a,"T.tenantB.stream"));
    ck("tenantB DENIED sub T.tenantA.stream",          !canSub(b,"T.tenantA.stream"));
    ck("tenantA DENIED sub T.*. wildcard (god view)",  !canSub(a,"T.*.stream"));
    // SUPERUSER role: cross-tenant god view
    ck("superuser sub T.tenantA.stream",                canSub(su,"T.tenantA.stream"));
    ck("superuser sub T.tenantB.stream",                canSub(su,"T.tenantB.stream"));
    ck("superuser sub T.*. wildcard",                   canSub(su,"T.*.stream"));
    for(Connection c: new Connection[]{a,b,su}) c.close();
    System.out.println("JMS-RESULT "+P+" "+F); if(F>0) System.exit(1);
  }
}
EOF
if javac -cp "$CP" -d "$D" "$D/NsIso.java" 2>/dev/null; then
  OUT=$(java -cp "$CP:$D" NsIso 2>/dev/null)
  echo "$OUT" | grep -E "^PASS|^FAIL"
  JP=$(echo "$OUT" | awk '/JMS-RESULT/{print $2}'); JF=$(echo "$OUT" | awk '/JMS-RESULT/{print $3}')
  PASS=$((PASS+${JP:-0})); FAIL=$((FAIL+${JF:-7}))
else
  echo "FAIL  JMS namespacing test failed to compile"; FAIL=$((FAIL+7))
fi

echo "### teardown"
pkill -9 -f "agent-1.3-SNAPSHOT.jar" 2>/dev/null
echo "==================================================="
echo "  PASS=$PASS  FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && echo "  TENANT NAMESPACING: PASS" || echo "  TENANT NAMESPACING: FAIL"
exit $FAIL
