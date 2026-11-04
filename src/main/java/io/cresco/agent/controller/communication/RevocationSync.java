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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Distributes revocations along the trust relationships that already exist: each controller's revocation list is signed
 * with its identity key and carried on the parent/child pings, as DELTAS of a versioned list.
 *
 * <p><b>Versions.</b> A node's list is the sequence of its revocations, each at a position (seq) in its registry's epoch
 * (a random id made with the registry: {@link PeerTrustRegistry#revocationEpoch}). A child's ping says how far it holds
 * its parent's list ({@code <epoch>.<seq>}); the pong carries the entries after that (at most
 * {@code revocation_sync_max_entries}, default 4096, per pong) or nothing. A region's ping carries its OWN entries after
 * what its parent acknowledged; the pong acknowledges ({@code <epoch>.<seq>}) and always names the parent's epoch. When
 * that epoch changes (the parent's registry was re-created or reset) the child resets what it believes the parent holds
 * and sends its list again, and asks for the parent's from the start. Per ping, when nothing changed, the cost is a few
 * string comparisons: O(1) in the size of the list. Signed deltas are cached (one signature for every child at the same
 * position). Only durable entries are served, so a position is never reused after a crash.
 *
 * <p><b>Who may send a list.</b>
 * <ul>
 *   <li>A PARENT's list (region to its agents, global to its regions) is accepted only when its signing certificate
 *       verifies here as the controller this node joined -- the parent's name comes from this node's state, never from the
 *       message; by the key bound to it, or under the regional CA by the CA recorded for its region (which issues a
 *       controller's name to that controller only) -- and its key is not bound here as an agent's; it applies as listed:
 *       an unscoped entry to every name, a scoped one to its region's names.</li>
 *   <li>A CHILD's list is accepted only at a global ({@code childIsRegion}), only when signed with the key bound HERE as a
 *       CONTROLLER (a region that joined this node) -- never an agent's, never a name the message claims: the issuer is the
 *       identity the signing certificate verifies as, and a broker-stamped caller ({@code cresco_caller}), when present, must
 *       be that same controller. Its entries pass a POSITIVE scope check ({@link PeerTrustRegistry#mergeRevocations(List,
 *       String, String, boolean)}): only keys of that region's own names, applied to that region's names only.</li>
 * </ul>
 * Merging is monotonic (revocation is permanent), so a replayed or stale list changes nothing. Entries are put in force
 * when the list is verified; their bindings, tombstones and durable record follow on the registry's background writer in
 * one write, so the ping/pong path never waits for a disk.
 */
public final class RevocationSync {

    private RevocationSync() { }

    /** On a ping: how far the child holds its parent's list, {@code <epoch>.<seq>} (a 64-hex digest from a v1 node). */
    static final String HAVE = "rev_have";
    /** On a ping: the child's own list, a signed delta (only while the parent has not acknowledged it). */
    static final String CHILD_LIST = "rev_child_list";
    /** On a pong: how far the parent processed the child's own list, {@code <child epoch>.<seq>}. */
    static final String ACK = "rev_ack";
    /** On a pong: the parent's list, a signed delta (only when the child holds less). */
    static final String PARENT_LIST = "rev_parent_list";
    /** On a pong: the parent's registry epoch (always): a change makes the child send its list again. */
    static final String EPOCH = "rev_epoch";
    /** On a ping: the child's clock (the parent reports a skew). */
    static final String CLOCK = "rev_ts";

    private static final Gson GSON = new Gson();
    private static final String LABEL = "cresco-revocations-v2";

    /** A signed revocation list (a delta of positions {@code (from, to]} of the issuer's list in {@code epoch}). */
    static final class SignedList {
        int v = 2;
        String issuer;          // region/agent
        String tenant;
        String epoch;
        long from, to;
        long issued;
        List<String> entries;   // fp|scope|name|exp
        String cert;            // the issuer's leaf certificate, base64 DER
        String sig;             // base64
    }

    /** A list that verified. */
    static final class Verified {
        String region, agent, epoch;
        long from, to;
        List<PeerTrustRegistry.ListEntry> entries;
    }

    /** Per node: what it holds of its parent's list, what its parent acknowledged of its own, its children's acks, a cache. */
    static final class State {
        volatile String parentEpoch = "";
        volatile long parentSeq;
        volatile String ackedEpoch = "";
        volatile long ackedSeq = -1;                   // -1: the parent acknowledged nothing (send from the start)
        volatile long lastSentFrom = -1, lastSentTo = -1, lastSentAt;
        final Map<String, String> childAcks = new ConcurrentHashMap<>();
        final Map<String, Long> lastWarned = new ConcurrentHashMap<>();
        final Map<String, String> cache = java.util.Collections.synchronizedMap(new LinkedHashMap<String, String>(16, 0.75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<String, String> e) { return size() > 64; }
        });
    }

    private static final Map<ControllerEngine, State> STATE = java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    private static State state(ControllerEngine ce) {
        return STATE.computeIfAbsent(ce, k -> new State());
    }

    /** The id of a list of fingerprints (sorted): SHA-256 hex; "" for an empty list (the v1 list id). */
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
        return (LABEL + "|" + l.v + "|" + l.issuer + "|" + l.tenant + "|" + l.epoch + "|" + l.from + "|" + l.to + "|" + l.issued + "|"
                + String.join(",", l.entries)).getBytes(StandardCharsets.UTF_8);
    }

    private static int maxEntries(ControllerEngine ce) {
        try { return Math.max(16, ce.getPluginBuilder().getConfig().getIntegerParam("revocation_sync_max_entries", 4096)); }
        catch (Exception e) { return 4096; }
    }

    private static io.cresco.library.utilities.CLogger logger(ControllerEngine ce) {
        return ce.getPluginBuilder().getLogger(RevocationSync.class.getName(), io.cresco.library.utilities.CLogger.Level.Info);
    }

    /** A signed delta of this node's list after {@code from} ({@code ownOnly}: only its own entries); cached; null when it cannot sign. */
    static String signedDelta(ControllerEngine ce, long from, boolean ownOnly) {
        PeerTrustRegistry reg = ce.getCertificateManager().getPeerTrust();
        long upTo = ownOnly ? reg.ownRevocationSeq() : reg.revocationSeq();
        String epoch = reg.revocationEpoch();
        String key = (ownOnly ? "own|" : "all|") + epoch + "|" + from + "|" + upTo;
        State st = state(ce);
        String cached = st.cache.get(key);
        if (cached != null) return cached;
        int max = maxEntries(ce);
        List<PeerTrustRegistry.Revocation> rs = reg.revocationsAfter(from, max, ownOnly);
        long to = rs.size() >= max ? rs.get(rs.size() - 1).seq : upTo;
        List<String> entries = new ArrayList<>(rs.size());
        for (PeerTrustRegistry.Revocation r : rs) {
            if (r.seq > to) break;
            entries.add(PeerTrustRegistry.ListEntry.of(r).format());
        }
        try {
            CertificateManager cm = ce.getCertificateManager();
            SignedList l = new SignedList();
            l.issuer = ce.cstate.getRegion() + "/" + ce.cstate.getAgent();
            l.tenant = SecuritySettings.tenant(ce.getPluginBuilder().getConfig());
            l.epoch = epoch;
            l.from = from;
            l.to = to;
            l.issued = System.currentTimeMillis();
            l.entries = entries;
            l.cert = Base64.getEncoder().encodeToString(cm.getPublicCertificate()[0].getEncoded());
            l.sig = Base64.getEncoder().encodeToString(cm.signWithIdentity(signedBytes(l)));
            String json = GSON.toJson(l);
            st.cache.put(key, json);
            return json;
        } catch (Exception e) {
            return null;
        }
    }

    /** This node's own list, whole, signed with its identity key; null when empty or it cannot sign. */
    static String signOwn(ControllerEngine ce) {
        if (ce.getCertificateManager().getPeerTrust().ownRevocationSeq() == 0) return null;
        return signedDelta(ce, 0, true);
    }

    /** Parse and check a signed list: its certificate must verify here and sign it. The caller checks WHO it is. */
    private static Verified verifySigned(ControllerEngine ce, String json, X509Certificate[] signer, String[] why) {
        try {
            SignedList l = GSON.fromJson(json, SignedList.class);
            if (l == null || l.v != 2 || l.entries == null || l.cert == null || l.sig == null || l.issuer == null || l.epoch == null
                    || l.from < 0 || l.to < l.from) { why[0] = "malformed"; return null; }
            if (l.entries.size() > 100_000) { why[0] = "too long"; return null; }
            X509Certificate leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(l.cert)));
            String[] w = new String[1];
            CrescoIdentity id = ce.getCertificateManager().getPeerTrust().verifiedIdentity(new X509Certificate[]{leaf}, w);
            if (id == null || !(id.getRegion() + "/" + id.getAgent()).equals(l.issuer)) {
                why[0] = "its certificate does not verify here as its issuer " + l.issuer + (w[0] == null ? "" : " (" + w[0] + ")");
                return null;
            }
            Signature sig = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(leaf.getPublicKey().getAlgorithm()));
            sig.initVerify(leaf.getPublicKey());
            sig.update(signedBytes(l));
            if (!sig.verify(Base64.getDecoder().decode(l.sig))) { why[0] = "the signature does not verify"; return null; }
            Verified v = new Verified();
            v.region = id.getRegion();
            v.agent = id.getAgent();
            v.epoch = l.epoch;
            v.from = l.from;
            v.to = l.to;
            v.entries = new ArrayList<>(l.entries.size());
            for (String e : l.entries) {
                PeerTrustRegistry.ListEntry x = PeerTrustRegistry.ListEntry.parse(e);
                if (x == null) { why[0] = "a malformed entry"; return null; }
                v.entries.add(x);
            }
            signer[0] = leaf;
            return v;
        } catch (Exception e) {
            why[0] = e.getClass().getSimpleName() + ": " + e.getMessage();
            return null;
        }
    }

    /**
     * Verify {@code json} as the list of the CONTROLLER {@code region/agent}: signed with the key bound here as a controller
     * to that name (an agent's key, or one only vouched for by a CA, is not a controller's). Returns its entries, or null.
     */
    static List<String> verify(ControllerEngine ce, String json, String region, String agent, String[] why) {
        Verified v = verifyController(ce, json, region, agent, true, why);
        if (v == null) return null;
        List<String> out = new ArrayList<>(v.entries.size());
        for (PeerTrustRegistry.ListEntry e : v.entries) out.add(e.format());
        return out;
    }

    /**
     * @param requireBinding true (a CHILD's list): the signing key must be bound here as a controller. False (the PARENT's
     *                       list, whose name comes from this node's own state): the certificate must verify here as that
     *                       name -- by its bound key, or under the regional CA by the CA this node recorded for the parent's
     *                       region, which issues a controller's name to that controller only -- and must not be bound here as
     *                       an agent.
     */
    private static Verified verifyController(ControllerEngine ce, String json, String region, String agent, boolean requireBinding, String[] why) {
        X509Certificate[] signer = new X509Certificate[1];
        Verified v = verifySigned(ce, json, signer, why);
        if (v == null) return null;
        if (region != null && agent != null && (!region.equals(v.region) || !agent.equals(v.agent))) {
            why[0] = "issued by " + v.region + "/" + v.agent + ", not " + region + "/" + agent;
            return null;
        }
        String role = ce.getCertificateManager().getPeerTrust().boundRole(v.region, v.agent, signer[0].getPublicKey());
        if (!requireBinding && (role == null || PeerTrustRegistry.ROLE_CONTROLLER.equals(role))) return v;
        if (!PeerTrustRegistry.ROLE_CONTROLLER.equals(role)) {
            why[0] = v.region + "/" + v.agent + " is not a controller bound here by its key (" + (role == null ? "not bound" : "bound as " + role)
                    + "): only controllers send revocation lists";
            return null;
        }
        return v;
    }

    private static long seqOf(String v) {
        if (v == null) return -1;
        int dot = v.lastIndexOf('.');
        try { return dot < 0 ? -1 : Long.parseLong(v.substring(dot + 1)); } catch (NumberFormatException e) { return -1; }
    }

    private static String epochOf(String v) {
        if (v == null) return "";
        int dot = v.lastIndexOf('.');
        return dot < 0 ? "" : v.substring(0, dot);
    }

    private static void warnOnce(ControllerEngine ce, State st, String key, String text) {
        long now = System.currentTimeMillis();
        Long last = st.lastWarned.get(key);
        if (last != null && now - last < 600_000L) return;
        if (st.lastWarned.size() > 4096) st.lastWarned.clear();
        st.lastWarned.put(key, now);
        logger(ce).warn(text);
    }

    // ------------------------------------------------------------------ child side

    /** Before a child (agent or region) pings its parent. */
    public static void onChildPing(ControllerEngine ce, MsgEvent ping) {
        try {
            State st = state(ce);
            PeerTrustRegistry reg = ce.getCertificateManager().getPeerTrust();
            ping.setParam(HAVE, st.parentEpoch.isEmpty() ? "" : st.parentEpoch + "." + st.parentSeq);
            ping.setParam(CLOCK, Long.toString(System.currentTimeMillis()));
            if (!ce.cstate.isRegionalController()) return;                       // a region reports its own revocations
            long own = reg.ownRevocationSeq();
            if (own == 0) return;
            long acked = reg.revocationEpoch().equals(st.ackedEpoch) ? st.ackedSeq : -1;
            if (own <= acked) return;
            long from = Math.max(0, acked), now = System.currentTimeMillis();
            // sent and not acknowledged yet: send the same delta again at most every 30 s (a parent that refuses it)
            if (from == st.lastSentFrom && own == st.lastSentTo && now - st.lastSentAt < 30_000L) return;
            String signed = signedDelta(ce, from, true);
            if (signed == null) return;
            ping.setCompressedParam(CHILD_LIST, signed);
            st.lastSentFrom = from;
            st.lastSentTo = own;
            st.lastSentAt = now;
        } catch (Exception ignore) { }
    }

    /**
     * On the parent's pong: note its epoch (a change resets what this node believes the parent holds), what it acknowledged,
     * and merge the parent's list -- verified as the controller this node joined ({@code parentRegion/parentAgent}, from this
     * node's own state).
     */
    public static void onParentPong(ControllerEngine ce, MsgEvent pong, String parentRegion, String parentAgent) {
        try {
            State st = state(ce);
            PeerTrustRegistry reg = ce.getCertificateManager().getPeerTrust();
            String epoch = pong.getParam(EPOCH);
            if (epoch != null && !epoch.isEmpty() && !epoch.equals(st.parentEpoch)) {
                if (!st.parentEpoch.isEmpty()) {
                    logger(ce).info("The revocation registry of " + parentRegion + "/" + parentAgent + " is new (epoch " + epoch + "): sending this"
                            + " node's revocations again and fetching its list from the start");
                }
                st.parentEpoch = epoch;
                st.parentSeq = 0;
                st.ackedEpoch = "";
                st.ackedSeq = -1;
                st.lastSentFrom = st.lastSentTo = -1;
            }
            String ack = pong.getParam(ACK);
            String myEpoch = reg.revocationEpoch();
            if (ack != null && epochOf(ack).equals(myEpoch)) {
                st.ackedEpoch = myEpoch;
                st.ackedSeq = seqOf(ack);
            } else if (epoch != null) {
                st.ackedEpoch = "";
                st.ackedSeq = -1;                                                 // the parent holds nothing of ours (restarted)
            }
            String list = pong.getParam(PARENT_LIST) == null ? null : pong.getCompressedParam(PARENT_LIST);
            if (list == null || parentRegion == null || parentAgent == null) return;
            String[] why = new String[1];
            Verified v = verifyController(ce, list, parentRegion, parentAgent, false, why);
            if (v == null) {
                warnOnce(ce, st, "parent|" + why[0], "Revocation list on the pong of " + parentRegion + "/" + parentAgent + " REFUSED: " + why[0]);
                return;
            }
            if (!st.parentEpoch.isEmpty() && !v.epoch.equals(st.parentEpoch)) return;   // from another epoch: ignore
            if (v.from > st.parentSeq) return;                                         // a gap: ask again from what we hold
            reg.mergeRevocations(v.entries, "revocation list of " + parentRegion + "/" + parentAgent, null, true);
            st.parentEpoch = v.epoch;
            st.parentSeq = Math.max(st.parentSeq, v.to);
        } catch (Exception ignore) { }
    }

    // ------------------------------------------------------------------ parent side

    /**
     * In a parent's ping handler, on the ping it turns into the pong. {@code childIsRegion}: this is the global's handler,
     * where a CHILD controller's list may be accepted (a region's handler never accepts one: agents are not authorities).
     */
    public static void onPingAtParent(ControllerEngine ce, MsgEvent msg, boolean childIsRegion) {
        try {
            State st = state(ce);
            PeerTrustRegistry reg = ce.getCertificateManager().getPeerTrust();
            String childList = msg.getParam(CHILD_LIST) == null ? null : msg.getCompressedParam(CHILD_LIST);
            String have = msg.getParam(HAVE);
            String clock = msg.getParam(CLOCK);
            msg.removeParam(CHILD_LIST);
            msg.removeParam(HAVE);
            msg.removeParam(CLOCK);
            String src = msg.getSrcRegion() + "/" + msg.getSrcAgent();
            String child = null;
            if (childList != null && childIsRegion) {
                String[] why = new String[1];
                Verified v = verifyChild(ce, msg, childList, why);
                if (v == null) {
                    warnOnce(ce, st, "child|" + src + "|" + why[0], "Revocation list on the ping from " + src + " REFUSED: " + why[0]);
                } else {
                    child = v.region + "/" + v.agent;
                    String prev = st.childAcks.get(child);
                    long held = v.epoch.equals(epochOf(prev)) ? seqOf(prev) : 0;
                    if (v.from <= held) {
                        reg.mergeRevocations(v.entries, "revocation list of region " + child, v.region, true);
                        if (st.childAcks.size() < 100_000 || prev != null) st.childAcks.put(child, v.epoch + "." + Math.max(held, v.to));
                    }
                }
            }
            String ack = st.childAcks.get(child != null ? child : src);
            if (ack != null) msg.setParam(ACK, ack);
            msg.setParam(EPOCH, reg.revocationEpoch());
            String delta = parentDelta(ce, reg, have);
            if (delta != null) msg.setCompressedParam(PARENT_LIST, delta);
            if (clock != null) {
                try { ce.getCertificateManager().noteClock(Long.parseLong(clock), "child " + src); } catch (NumberFormatException ignore) { }
            }
        } catch (Exception ignore) { }
    }

    /**
     * A child CONTROLLER's list: signed with the key bound here as a controller, of another region than this node's, not this
     * node's own parent; and when the broker stamped the caller ({@code cresco_caller}), that caller is the same controller.
     */
    private static Verified verifyChild(ControllerEngine ce, MsgEvent msg, String json, String[] why) {
        Verified v = verifyController(ce, json, null, null, true, why);
        if (v == null) return null;
        if (v.region.equals(ce.cstate.getRegion())) {
            why[0] = v.region + "/" + v.agent + " is of this node's own region: not a child region";
            return null;
        }
        if (v.region.equals(ce.cstate.getGlobalRegion()) && v.agent.equals(ce.cstate.getGlobalAgent())) {
            why[0] = v.region + "/" + v.agent + " is this node's parent, not a child";
            return null;
        }
        // the verified caller the broker stamped (phase 3): when there is one it must be this same controller ("anonymous"
        // only in the explicit insecure development mode, where no identity is verified and the signature alone decides)
        String caller = msg.getParam(io.cresco.library.security.Caller.PARAM_CALLER);
        if (caller != null && !caller.isEmpty() && !"anonymous".equals(caller)
                && !(caller.startsWith("controller:") && caller.endsWith("/" + v.region + "/" + v.agent))) {
            why[0] = "the verified caller " + caller + " is not the controller " + v.region + "/" + v.agent + " that signed the list";
            return null;
        }
        return v;
    }

    /** The parent's list after what the child holds ({@code have}), or null when it holds all of it. O(1) when nothing changed. */
    private static String parentDelta(ControllerEngine ce, PeerTrustRegistry reg, String have) {
        long durable = reg.revocationSeq();
        if (durable == 0) return null;
        long from = 0;
        if (have != null && !have.isEmpty()) {
            if (have.indexOf('.') < 0) {
                if (have.equals(reg.revocationDigest())) return null;          // a v1 node that holds the whole list
            } else if (epochOf(have).equals(reg.revocationEpoch())) {
                from = Math.max(0, seqOf(have));
            }
        }
        if (from >= durable) return null;
        return signedDelta(ce, from, false);
    }
}
