package io.cresco.agent.controller.communication;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.CrescoIdentity;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Distributes revocations with the trust relationships that already exist: a node's revocation list (the key fingerprints
 * it holds as revoked) is signed with its identity key and carried on the parent/child pings, only when the other side
 * does not hold it yet (each side advertises the id of the list it holds; an empty list costs nothing).
 *
 * <ul>
 *   <li>A PARENT's list (region to its agents, global to its regions) applies to every name: the parent is the authority
 *       for its subtree.</li>
 *   <li>A REGION's list at the global applies only to identities of that region (a region revokes its own names) and then
 *       travels on with the global's list to every region. Agents' lists are not accepted: an agent is not an authority.</li>
 *   <li>A list is accepted only when its signing certificate verifies here as the expected peer (the controller this node
 *       joined, or the child region controller that pinged) -- by its bound key or its region's recorded CA -- and the
 *       signature over the list verifies with that certificate's key. Merging is monotonic (revocation is permanent), so a
 *       replayed or stale list changes nothing.</li>
 * </ul>
 */
public final class RevocationSync {

    private RevocationSync() { }

    /** On a ping: the id of the parent's list the child holds. */
    static final String HAVE = "rev_have";
    /** On a ping: the child's own list, signed (only while the parent has not acknowledged it). */
    static final String CHILD_LIST = "rev_child_list";
    /** On a pong: the id of the child's list the parent holds. */
    static final String ACK = "rev_ack";
    /** On a pong: the parent's list, signed (only when the child holds another). */
    static final String PARENT_LIST = "rev_parent_list";

    private static final Gson GSON = new Gson();
    private static final String LABEL = "cresco-revocations-v1";

    /** A signed revocation list. */
    static final class SignedList {
        int v = 1;
        String issuer;      // region/agent
        String tenant;
        long issued;
        List<String> keys;  // sorted key fingerprints
        String cert;        // the issuer's leaf certificate, base64 DER
        String sig;         // base64
    }

    /** Per node: what it holds of its parent's list, and what its parent acknowledged of its own. */
    private static final Map<ControllerEngine, String[]> CHILD_STATE = java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());
    /** Per parent node: child path -> id of that child's list the parent merged. */
    private static final Map<ControllerEngine, Map<String, String>> PARENT_STATE = java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    /** The id of a list of fingerprints (sorted): SHA-256 hex; "" for an empty list. */
    static String idOf(List<String> keys) {
        if (keys == null || keys.isEmpty()) return "";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (String k : keys) { md.update(k.getBytes(StandardCharsets.UTF_8)); md.update((byte) ','); }
            return java.util.HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] signedBytes(SignedList l) {
        return (LABEL + "|" + l.issuer + "|" + l.tenant + "|" + l.issued + "|" + String.join(",", l.keys)).getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> ownKeys(ControllerEngine ce) {
        return new ArrayList<>(ce.getCertificateManager().getPeerTrust().revocations().keySet());   // sorted (TreeMap)
    }

    /** This node's list, signed with its identity key; null when empty or it cannot sign. */
    static String signOwn(ControllerEngine ce) {
        List<String> keys = ownKeys(ce);
        if (keys.isEmpty()) return null;
        try {
            CertificateManager cm = ce.getCertificateManager();
            SignedList l = new SignedList();
            l.issuer = ce.cstate.getRegion() + "/" + ce.cstate.getAgent();
            l.tenant = SecuritySettings.tenant(ce.getPluginBuilder().getConfig());
            l.issued = System.currentTimeMillis();
            l.keys = keys;
            l.cert = Base64.getEncoder().encodeToString(cm.getPublicCertificate()[0].getEncoded());
            l.sig = Base64.getEncoder().encodeToString(cm.signWithIdentity(signedBytes(l)));
            return GSON.toJson(l);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Verify {@code json} as the list of {@code region/agent} and return its keys, or null (logged by the caller). The
     * certificate must verify here as that identity (bound key or the region's recorded CA), and the signature with its key.
     */
    static List<String> verify(ControllerEngine ce, String json, String region, String agent, String[] why) {
        try {
            SignedList l = GSON.fromJson(json, SignedList.class);
            if (l == null || l.v != 1 || l.keys == null || l.cert == null || l.sig == null) { why[0] = "malformed"; return null; }
            if (!(region + "/" + agent).equals(l.issuer)) { why[0] = "issued by " + l.issuer + ", not " + region + "/" + agent; return null; }
            if (l.keys.size() > 100_000) { why[0] = "too long"; return null; }
            X509Certificate leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(l.cert)));
            String[] w = new String[1];
            CrescoIdentity id = ce.getCertificateManager().getPeerTrust().verifiedIdentity(new X509Certificate[]{leaf}, w);
            if (id == null || !region.equals(id.getRegion()) || !agent.equals(id.getAgent())) {
                why[0] = "its certificate does not verify here as " + region + "/" + agent + (w[0] == null ? "" : " (" + w[0] + ")");
                return null;
            }
            Signature sig = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(leaf.getPublicKey().getAlgorithm()));
            sig.initVerify(leaf.getPublicKey());
            sig.update(signedBytes(l));
            if (!sig.verify(Base64.getDecoder().decode(l.sig))) { why[0] = "the signature does not verify"; return null; }
            List<String> sorted = new ArrayList<>(l.keys);
            java.util.Collections.sort(sorted);
            return sorted;
        } catch (Exception e) {
            why[0] = e.getClass().getSimpleName() + ": " + e.getMessage();
            return null;
        }
    }

    // ------------------------------------------------------------------ child side

    /** Before a child (agent or region) pings its parent. */
    public static void onChildPing(ControllerEngine ce, MsgEvent ping) {
        try {
            String[] st = CHILD_STATE.computeIfAbsent(ce, k -> new String[]{"", ""});   // {parentListId, ackedOwnId}
            ping.setParam(HAVE, st[0]);
            if (ce.cstate.isRegionalController()) {                                   // a region reports its own revocations
                String ownId = idOf(ownKeys(ce));
                if (!ownId.isEmpty() && !ownId.equals(st[1])) {
                    String signed = signOwn(ce);
                    if (signed != null) ping.setCompressedParam(CHILD_LIST, signed);
                }
            }
        } catch (Exception ignore) { }
    }

    /** On the parent's pong: merge the parent's list (verified as the parent), and note what the parent acknowledged. */
    public static void onParentPong(ControllerEngine ce, MsgEvent pong, String parentRegion, String parentAgent) {
        try {
            String[] st = CHILD_STATE.computeIfAbsent(ce, k -> new String[]{"", ""});
            String ack = pong.getParam(ACK);
            if (ack != null) st[1] = ack;
            String list = pong.getParam(PARENT_LIST) == null ? null : pong.getCompressedParam(PARENT_LIST);
            if (list == null || parentRegion == null || parentAgent == null) return;
            String[] why = new String[1];
            List<String> keys = verify(ce, list, parentRegion, parentAgent, why);
            if (keys == null) {
                ce.getPluginBuilder().getLogger(RevocationSync.class.getName(), io.cresco.library.utilities.CLogger.Level.Info)
                        .warn("Revocation list on the pong of " + parentRegion + "/" + parentAgent + " REFUSED: " + why[0]);
                return;
            }
            ce.getCertificateManager().getPeerTrust().mergeRevocations(keys, "revocation list of " + parentRegion + "/" + parentAgent, null);
            st[0] = idOf(keys);
        } catch (Exception ignore) { }
    }

    // ------------------------------------------------------------------ parent side

    /** In a parent's ping handler, on the ping it turns into the pong. */
    public static void onPingAtParent(ControllerEngine ce, MsgEvent msg, boolean childIsRegion) {
        try {
            String childRegion = msg.getSrcRegion(), childAgent = msg.getSrcAgent();
            String childList = msg.getParam(CHILD_LIST) == null ? null : msg.getCompressedParam(CHILD_LIST);
            String childHave = msg.getParam(HAVE);
            msg.removeParam(CHILD_LIST);
            msg.removeParam(HAVE);
            Map<String, String> merged = PARENT_STATE.computeIfAbsent(ce, k -> new ConcurrentHashMap<>());
            String child = childRegion + "/" + childAgent;
            if (childList != null && childIsRegion && childRegion != null && childAgent != null) {
                String[] why = new String[1];
                List<String> keys = verify(ce, childList, childRegion, childAgent, why);
                if (keys == null) {
                    ce.getPluginBuilder().getLogger(RevocationSync.class.getName(), io.cresco.library.utilities.CLogger.Level.Info)
                            .warn("Revocation list on the ping of " + child + " REFUSED: " + why[0]);
                } else {
                    ce.getCertificateManager().getPeerTrust().mergeRevocations(keys, "revocation list of region " + child, childRegion);
                    if (merged.size() < 10_000) merged.put(child, idOf(keys));
                }
            }
            String ack = merged.get(child);
            if (ack != null) msg.setParam(ACK, ack);
            String ownId = idOf(ownKeys(ce));
            if (!ownId.isEmpty() && !ownId.equals(childHave)) {
                String signed = signOwn(ce);
                if (signed != null) msg.setCompressedParam(PARENT_LIST, signed);
            }
        } catch (Exception ignore) { }
    }
}
