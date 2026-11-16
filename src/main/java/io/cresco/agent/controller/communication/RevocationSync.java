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
 * string comparisons: O(1) in the size of the list. The pong also states the last position of the parent's list, so a
 * child knows when it holds all of it ({@link #parentListComplete}). Signed deltas are cached (one signature for every child at the same
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
 *
 * <p><b>Revocations by name.</b> A global holds no key for an agent of another region (it is bound at that region's
 * controller), so a name its operator revokes cannot be put on its list. The global reserves the name and ASKS: while such
 * a request is open, the pong to the ONE controller it records for the name's region carries it ({@link #NAMES}).
 * <ul>
 *   <li>Asked: only that region's one joined controller, as the one-controller-per-region rule records it
 *       ({@link PeerTrustRegistry#controllerOf}, the same record discovery keeps: none, or more than one controller
 *       recorded for the region, and nothing is sent), verified as the pinger, in the tenant recorded for it, and only
 *       when its ping carried {@link #NAMES_FRESH} -- a random value an earlier build does not send. A request is signed
 *       anew for each pong, over that value: it answers THIS ping.</li>
 *   <li>Taken: by a region's controller only (an agent takes none), from the controller it joined (its name from this
 *       node's own state; verified as its list is; its tenant the one recorded for it; where its registry records this
 *       node's one global, that global only), addressed to this node, for names
 *       of this node's own region, and FRESH: it must repeat the value of one of this node's last pings. A request that was
 *       held back, or is delivered again, repeats none and is refused. It carries the AGE of the operator's act, so the
 *       region dates the act by its own clock and leaves a later binding alone
 *       ({@link PeerTrustRegistry#applyNameRevocation}).</li>
 *   <li>Answered ({@link #NAME_REPORT}, on the region's next ping): signed with its identity key, addressed to its global,
 *       naming the request's id. Read at the global only while a request is open, only from that one controller in its
 *       tenant; it settles the request and nothing else ({@link PeerTrustRegistry#acceptNameAnswer}): no key is revoked on
 *       a region's word, nothing is distributed.</li>
 * </ul>
 * Cost: one short parameter on a region controller's ping; on a pong nothing while no request is open for the pinger's
 * region (one volatile read), one signature while one is. Nothing new reaches a node of an earlier build.
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
    /** On a pong: the last position of the parent's list (always, with the epoch): the child holds all of it at that position. */
    static final String SEQ = "rev_seq";
    /** On a ping: the child's clock (the parent reports a skew). */
    static final String CLOCK = "rev_ts";
    /**
     * On a region controller's ping to its global: a fresh random value (16 hex digits). It says that this controller
     * takes revocations by name, and a request must repeat it. An earlier build sends none, and is asked nothing.
     */
    static final String NAMES_FRESH = "rev_names_fresh";
    /** On a pong, from a global to a region's one controller: the open revocations by name of that region's names (signed). */
    static final String NAMES = "rev_names";
    /** On a ping, from a region's controller to its global: its answers to those (signed). */
    static final String NAME_REPORT = "rev_name_report";

    private static final Gson GSON = new Gson();
    private static final String LABEL = "cresco-revocations-v2";
    /** Another label than a list's: neither is ever taken for the other; the kind, the addressee and the fresh value are signed too. */
    private static final String NAMES_LABEL = "cresco-name-revocations-v2";
    private static final String KIND_ASK = "ask", KIND_REPORT = "report";
    /** F-17: a global's answers to a region controller's questions about former names ({@link RecordedMoves}). */
    private static final String KIND_MOVES = "moves";
    /** The most items one signed set of requests or answers carries (a global has at most that many open for one region). */
    private static final int NAMES_MAX = PeerTrustRegistry.NAME_REVOCATIONS_OPEN_PER_REGION;
    /** How many of its last pings' fresh values a region's controller still takes a request for. */
    private static final int FRESH_KEPT = 3;

    /**
     * Signed revocations by name. {@code ask}: a global's requests to one region's controller; items {@code A|id|name|age}
     * (revoke the name; the operator did so {@code age} ms ago) and {@code L|id|name|key} (the operator rebound it: to that
     * key, or -- empty -- to the first one presented); {@code fresh} repeats the value of the ping it answers.
     * {@code report}: that controller's answers; items {@code id|name|status|key} ({@link PeerTrustRegistry#acceptNameAnswer}).
     */
    static final class SignedNames {
        int v = 2;
        String kind;
        String issuer;          // region/agent
        String tenant;          // the issuer's: compared with its certificate's and with the one recorded for it
        String to;              // region/agent it is for: a set made for one node is nothing to another
        String fresh;
        long issued;
        List<String> items;
        String cert;
        List<String> chain;
        String sig;
    }

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
        List<String> chain;     // the certificates above it (base64 DER), for a pre-provisioned PKI path; not signed
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
        /** This node's link to its parent is up, as its health watcher last saw it. */
        volatile boolean parentJoined;
        /** A pong since this node last joined its parent showed that it holds the parent's whole list. */
        volatile boolean parentComplete;
        /** A pong since this node last joined its parent stated no position of its list (a parent of an earlier build). */
        volatile boolean parentStatesNoPosition;
        final Map<String, String> childAcks = new ConcurrentHashMap<>();
        /** At a region's controller: the fresh values of its last pings, {value, when sent}, newest first (replaced, never mutated). */
        volatile String[][] fresh = new String[0][];
        /** At a region's controller: the requests on its global's last pong, id -> {A or L, name}; and its last signed answer. */
        volatile Map<String, String[]> nameAsks = Map.of();
        volatile String[] nameReport;
        /** The last warning logged (tests). */
        volatile String lastWarning;
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

    private static byte[] signedBytes(SignedNames l) {
        return (NAMES_LABEL + "|" + l.v + "|" + l.kind + "|" + l.issuer + "|" + l.tenant + "|" + l.to + "|" + l.fresh + "|" + l.issued + "|"
                + String.join(",", l.items)).getBytes(StandardCharsets.UTF_8);
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

    /**
     * A signed delta of this node's list after {@code from} ({@code ownOnly}: only its own entries); null when it cannot sign.
     * Cached per position AND per signing certificate: a renewed or re-keyed leaf signs anew, and a cached list whose
     * certificate is no longer valid is never served (children would refuse it), it is signed again with the current one.
     */
    static String signedDelta(ControllerEngine ce, long from, boolean ownOnly) {
        PeerTrustRegistry reg = ce.getCertificateManager().getPeerTrust();
        long upTo = ownOnly ? reg.ownRevocationSeq() : reg.revocationSeq();
        String epoch = reg.revocationEpoch();
        X509Certificate[] mine = ce.getCertificateManager().getPublicCertificate();
        if (mine == null || mine.length == 0 || mine[0] == null) return null;
        long now = System.currentTimeMillis();
        if (now >= mine[0].getNotAfter().getTime() || now < mine[0].getNotBefore().getTime()) {
            warnOnce(ce, state(ce), "own-cert-invalid", "This node's certificate is not valid now (" + mine[0].getNotBefore() + " - "
                    + mine[0].getNotAfter() + "): its revocation list cannot be signed for its peers until it is renewed");
            return null;
        }
        String key = (ownOnly ? "own|" : "all|") + epoch + "|" + from + "|" + upTo + "|" + PeerIdentity.spkiFingerprint(mine[0])
                + "|" + mine[0].getSerialNumber();
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
            l.cert = Base64.getEncoder().encodeToString(mine[0].getEncoded());
            if (mine.length > 1 && mine[1] != null) {
                l.chain = new ArrayList<>();
                for (int i = 1; i < mine.length && mine[i] != null; i++) l.chain.add(Base64.getEncoder().encodeToString(mine[i].getEncoded()));
            }
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

    /**
     * The identity that signed {@code signed}: the certificate must verify HERE as {@code issuer} (by its bound key, its
     * region's recorded CA or the operator's PKI; not revoked, not tombstoned) and its key must have made {@code sigB64}
     * over {@code signed}. Sets {@code signer[0]} to the leaf; null (and {@code why}) otherwise. The caller checks WHO it is.
     */
    private static CrescoIdentity verifySigner(ControllerEngine ce, String issuer, String certB64, List<String> chainB64, byte[] signed,
                                               String sigB64, X509Certificate[] signer, String[] why) throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate leaf = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(certB64)));
        List<X509Certificate> presented = new ArrayList<>();
        presented.add(leaf);
        if (chainB64 != null) {
            for (int i = 0; i < chainB64.size() && i < CertificateManager.MAX_CHAIN_CERTS; i++) {
                presented.add((X509Certificate) cf.generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(chainB64.get(i)))));
            }
        }
        String[] w = new String[1];
        CrescoIdentity id = ce.getCertificateManager().getPeerTrust().verifiedIdentity(presented.toArray(new X509Certificate[0]), w);
        if (id == null || !(id.getRegion() + "/" + id.getAgent()).equals(issuer)) {
            why[0] = "its certificate does not verify here as its issuer " + issuer + (w[0] == null ? "" : " (" + w[0] + ")");
            return null;
        }
        Signature sig = Signature.getInstance(io.cresco.library.crypto.CrescoCrypto.signatureAlgorithm(leaf.getPublicKey().getAlgorithm()));
        sig.initVerify(leaf.getPublicKey());
        sig.update(signed);
        if (!sig.verify(Base64.getDecoder().decode(sigB64))) { why[0] = "the signature does not verify"; return null; }
        signer[0] = leaf;
        return id;
    }

    /** Parse and check a signed list: its certificate must verify here and sign it. The caller checks WHO it is. */
    private static Verified verifySigned(ControllerEngine ce, String json, X509Certificate[] signer, String[] why) {
        try {
            SignedList l = GSON.fromJson(json, SignedList.class);
            if (l == null || l.v != 2 || l.entries == null || l.cert == null || l.sig == null || l.issuer == null || l.epoch == null
                    || l.from < 0 || l.to < l.from) { why[0] = "malformed"; return null; }
            if (l.entries.size() > 100_000) { why[0] = "too long"; return null; }
            CrescoIdentity id = verifySigner(ce, l.issuer, l.cert, l.chain, signedBytes(l), l.sig, signer, why);
            if (id == null) return null;
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
        why[0] = controllerRefusal(ce, v.region, v.agent, signer, region, agent, requireBinding);
        return why[0] == null ? v : null;
    }

    /**
     * Why the verified signer {@code vRegion/vAgent} is not the controller {@code region/agent} (null names: any) this node
     * takes a list -- or revocations by name -- from, or null. See {@link #verifyController} for {@code requireBinding}.
     */
    private static String controllerRefusal(ControllerEngine ce, String vRegion, String vAgent, X509Certificate[] signer, String region,
                                            String agent, boolean requireBinding) {
        if (region != null && agent != null && (!region.equals(vRegion) || !agent.equals(vAgent))) {
            return "issued by " + vRegion + "/" + vAgent + ", not " + region + "/" + agent;
        }
        // the role the signer's own certificate carries (pre-provisioned), never one remembered for the name
        String role = ce.getCertificateManager().getPeerTrust().boundRole(vRegion, vAgent, signer);
        if (!requireBinding && (role == null || PeerTrustRegistry.ROLE_CONTROLLER.equals(role))) return null;
        if (!PeerTrustRegistry.ROLE_CONTROLLER.equals(role)) {
            return vRegion + "/" + vAgent + " is not a controller bound here by its key (" + (role == null ? "not bound" : "bound as " + role)
                    + "): only controllers send revocation lists";
        }
        return null;
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
        st.lastWarning = text;
        logger(ce).warn(text);
    }

    /** Tests: the last warning this node's revocation sync logged. */
    static String lastWarning(ControllerEngine ce) { return state(ce).lastWarning; }

    // ------------------------------------------------------------------ child side

    /** Before a child (agent or region) pings its parent. */
    public static void onChildPing(ControllerEngine ce, MsgEvent ping) {
        try {
            State st = state(ce);
            PeerTrustRegistry reg = ce.getCertificateManager().getPeerTrust();
            ping.setParam(HAVE, st.parentEpoch.isEmpty() ? "" : st.parentEpoch + "." + st.parentSeq);
            ping.setParam(CLOCK, Long.toString(System.currentTimeMillis()));
            if (!ce.cstate.isRegionalController()) return;                       // a region reports its own revocations
            try { freshValue(st, ping); answerNames(ce, st, reg, ping); } catch (Exception ignore) { }
            // F-17: whether its nodes' former addresses are recorded moves (answered, signed, on this ping's pong)
            try { ce.recordedMoves().onPing(ping); } catch (Exception ignore) { }
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
        onParentList(ce, pong, parentRegion, parentAgent);
        // after the list (the registry then holds what the parent revoked by key): what it revoked by name
        try { onParentNames(ce, state(ce), pong, parentRegion, parentAgent); } catch (Exception ignore) { }
        try { onParentMoves(ce, state(ce), pong, parentRegion, parentAgent); } catch (Exception ignore) { }
    }

    private static void onParentList(ControllerEngine ce, MsgEvent pong, String parentRegion, String parentAgent) {
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
            if (ack != null && epochOf(ack).equals(myEpoch) && seqOf(ack) > reg.ownRevocationSeq()) {
                // the parent acknowledged positions of this epoch this node never made: its registry went back in time
                boolean renewed = reg.rollbackSuspected("the parent " + parentRegion + "/" + parentAgent + " acknowledged position " + seqOf(ack)
                        + " of this node's own revocation list, which has only " + reg.ownRevocationSeq() + " (restored from an older copy?)");
                myEpoch = reg.revocationEpoch();
                st.lastSentFrom = st.lastSentTo = -1;
                // no new epoch (one was made within the hour): the acknowledgement is not of this list as it is -- trusting it
                // would mark positions this node writes next as delivered; send the whole list again instead
                if (!renewed) ack = null;
            }
            if (ack != null && epochOf(ack).equals(myEpoch)) {
                st.ackedEpoch = myEpoch;
                st.ackedSeq = seqOf(ack);
            } else if (epoch != null) {
                st.ackedEpoch = "";
                st.ackedSeq = -1;                                                 // the parent holds nothing of ours (restarted)
            }
            String list = pong.getParam(PARENT_LIST) == null ? null : pong.getCompressedParam(PARENT_LIST);
            if (list == null || parentRegion == null || parentAgent == null) {
                if (list == null) noteHeld(st, pong);
                return;
            }
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
            // a whole list (from the start) that ends before what this node held of it: the parent's list is shorter than it was
            // (it went back in time within the hour after a new epoch): hold what it serves now, so its next positions arrive
            st.parentSeq = (v.from == 0 && v.to < st.parentSeq) ? v.to : Math.max(st.parentSeq, v.to);
            noteHeld(st, pong);
        } catch (Exception ignore) { }
    }

    /**
     * After a pong: this node holds the parent's whole list when it holds the position the pong states as the list's last
     * ({@link #SEQ}) in the parent's epoch. A list that arrived in parts (at most {@code revocation_sync_max_entries} per
     * pong) is whole only with its last part; a parent that states no position (an earlier build) never completes it.
     */
    private static void noteHeld(State st, MsgEvent pong) {
        if (st.parentComplete) return;
        String seq = pong.getParam(SEQ), epoch = pong.getParam(EPOCH);
        if (seq == null) st.parentStatesNoPosition = true;
        if (seq == null || epoch == null || epoch.isEmpty() || !epoch.equals(st.parentEpoch)) return;
        st.parentStatesNoPosition = false;
        try {
            if (st.parentSeq >= Long.parseLong(seq)) st.parentComplete = true;
        } catch (NumberFormatException ignore) { }
    }

    /**
     * From a child's health watcher, each time it looks: is this node's link to its parent up (an agent active under its
     * region, a region joined to its global)? A join after the link was down (or the first) starts anew: the parent's
     * list counts as held only once a pong of THIS join shows it. True when this call saw the join (the watcher then
     * pings at once: the list, the trust bundle and an agent's attestation come on the first pong).
     */
    public static boolean onParentLink(ControllerEngine ce, boolean up) {
        State st = state(ce);
        if (!up) {
            st.parentJoined = false;
            return false;
        }
        if (st.parentJoined) return false;
        st.parentComplete = false;
        st.parentStatesNoPosition = false;
        st.parentJoined = true;
        return true;
    }

    /**
     * Is this node still waiting for (part of) its parent's revocation list -- so that its inbox holds what it cannot judge
     * without it? True from a join until {@link #parentListComplete}; not for a parent of an earlier build, whose pongs
     * state no position: whether its list is whole cannot be known, and a node below it judges with what it holds, as
     * nodes did before the position was stated.
     */
    public static boolean awaitingParentList(ControllerEngine ce) {
        if (parentListComplete(ce)) return false;
        State st = STATE.get(ce);
        return st == null || !st.parentStatesNoPosition;
    }

    /**
     * Does this node hold its parent's COMPLETE revocation list -- every position up to the last the parent stated on a
     * pong -- since it last joined that parent? False from the join until a pong shows it (the first, unless the list is
     * longer than one pong carries); true for a node without a parent (a global, a region that joined no global, a
     * standalone node). While the parent is lost the answer of the last join stands (a region keeps running through its
     * global's outage); it is false again from the moment the node rejoins. Entries the parent adds later arrive on the
     * following pongs and do not make it false. Read-only: two volatile reads and the controller mode.
     */
    public static boolean parentListComplete(ControllerEngine ce) {
        io.cresco.library.agent.ControllerMode mode = ce.cstate.getControllerState();
        if (ce.cstate.isGlobalController() || (mode != null && mode.name().startsWith("STANDALONE"))) return true;
        boolean up;
        if (ce.cstate.isRegionalController()) {
            up = mode == io.cresco.library.agent.ControllerMode.REGION_GLOBAL;
            if (!up && mode != io.cresco.library.agent.ControllerMode.REGION_GLOBAL_INIT
                    && mode != io.cresco.library.agent.ControllerMode.REGION_GLOBAL_FAILED) return true;      // no global
        } else {
            up = mode == io.cresco.library.agent.ControllerMode.AGENT;
        }
        State st = STATE.get(ce);
        // joined, but the watcher has not seen this join yet: what the node held before it does not count
        return st != null && st.parentComplete && (st.parentJoined || !up);
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
            String fresh = msg.getParam(NAMES_FRESH);
            // a region's answers are read only while this node has a request open: otherwise nothing is parsed or verified
            String nameReport = childIsRegion && reg.nameRevocationsOpen() > 0 && msg.getParam(NAME_REPORT) != null
                    ? msg.getCompressedParam(NAME_REPORT) : null;
            String moveAsk = childIsRegion ? msg.getParam(RecordedMoves.ASK) : null;
            msg.removeParam(CHILD_LIST);
            msg.removeParam(HAVE);
            msg.removeParam(CLOCK);
            msg.removeParam(NAMES_FRESH);
            msg.removeParam(NAME_REPORT);
            msg.removeParam(NAMES);                                              // never one the ping brought
            msg.removeParam(RecordedMoves.ASK);
            msg.removeParam(RecordedMoves.ANSWER);
            String src = msg.getSrcRegion() + "/" + msg.getSrcAgent();
            String child = null;
            // a node that answers region controllers' pings as their global is one, whatever its configuration said at start
            if (childIsRegion && !reg.isGlobal()) reg.setGlobal(true);
            String reporter = nameReport == null ? null : acceptAnswers(ce, st, reg, msg, nameReport, src);
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
            // who pinged, as far as this node can tell: the controller whose signed list verified, else the caller the broker
            // stamped -- when its name is bound here (never the names the message itself carries)
            String pinger = child != null ? child : reporter != null ? reporter : verifiedPinger(ce, reg, msg, childIsRegion);
            boolean anonymous = "anonymous".equals(msg.getParam(io.cresco.library.security.Caller.PARAM_CALLER));
            // the global serves its list (fingerprints and names) only to its verified child controllers (and in the explicit
            // insecure development mode, where nothing is verified); a region serves its own list to its agents
            boolean serve = !childIsRegion || pinger != null || anonymous;
            if (pinger != null) {
                String ack = st.childAcks.get(pinger);
                if (ack != null) msg.setParam(ACK, ack);
            }
            msg.setParam(EPOCH, reg.revocationEpoch());
            msg.setParam(SEQ, Long.toString(reg.revocationSeq()));
            if (serve) {
                if (pinger != null && have != null && have.indexOf('.') > 0 && epochOf(have).equals(reg.revocationEpoch())
                        && seqOf(have) > reg.revocationSeq()) {
                    // a verified child holds more of this epoch than this node does: this registry went back in time
                    boolean renewed = reg.rollbackSuspected(pinger + " holds position " + seqOf(have) + " of this node's revocation list, which"
                            + " has only " + reg.revocationSeq() + " (the registry was restored from an older copy?)");
                    msg.setParam(EPOCH, reg.revocationEpoch());
                    msg.setParam(SEQ, Long.toString(reg.revocationSeq()));
                    // no new epoch (one was made within the hour): what the child says it holds is not of this list as it is --
                    // serve the whole list, so the positions this node writes next are not skipped as already held
                    if (!renewed) have = null;
                }
                String delta = parentDelta(ce, reg, have);
                if (delta != null) msg.setCompressedParam(PARENT_LIST, delta);
            }
            // revocations by name: only from a global, only to a VERIFIED pinger, only while one is open (one volatile read)
            if (childIsRegion && pinger != null && reg.nameRevocationsOpen() > 0) askNames(ce, reg, msg, pinger, fresh);
            // F-17: a verified region controller's questions about its nodes' former names, answered signed for this ping
            if (childIsRegion && pinger != null && moveAsk != null && fresh != null && FRESH.matcher(fresh).matches()) {
                List<String> items = RecordedMoves.answers(reg, moveAsk);
                String signed = items == null ? null : signNames(ce, KIND_MOVES, pinger, fresh, items);
                if (signed != null) msg.setCompressedParam(RecordedMoves.ANSWER, signed);
            }
            // clock skew is recorded only for a verified peer bound here (a ping's claimed name cannot raise an alarm)
            if (clock != null && pinger != null) {
                try { ce.getCertificateManager().noteClock(Long.parseLong(clock), "child " + pinger); } catch (NumberFormatException ignore) { }
            }
        } catch (Exception ignore) { }
    }

    /**
     * The name {@code region/agent} of the caller the broker stamped on {@code msg} ({@code cresco_caller}), when it is an
     * agent or controller whose name is bound here -- at the global ({@code childIsRegion}) only a controller bound as one, of
     * another region; else null.
     */
    static String verifiedPinger(ControllerEngine ce, PeerTrustRegistry reg, MsgEvent msg, boolean childIsRegion) {
        String caller = msg.getParam(io.cresco.library.security.Caller.PARAM_CALLER);
        if (caller == null) return null;
        int colon = caller.indexOf(':');
        if (colon <= 0) return null;
        String kind = caller.substring(0, colon);
        String[] p = caller.substring(colon + 1).split("/");
        if (p.length != 3 || !CrescoNames.isValid(p[1]) || !CrescoNames.isValid(p[2])) return null;
        String region = p[1], agent = p[2];
        String role = reg.roleOf(p[0], region, agent);
        if (role == null || PeerTrustRegistry.ROLE_SELF.equals(role)) return null;
        if (childIsRegion) {
            if (!"controller".equals(kind) || !PeerTrustRegistry.ROLE_CONTROLLER.equals(role) || region.equals(ce.cstate.getRegion())) return null;
        } else if (!"agent".equals(kind) && !"controller".equals(kind)) {
            return null;
        }
        return region + "/" + agent;
    }

    /**
     * A child CONTROLLER's list: signed with the key bound here as a controller, of another region than this node's, not this
     * node's own parent; and when the broker stamped the caller ({@code cresco_caller}), that caller is the same controller.
     */
    private static Verified verifyChild(ControllerEngine ce, MsgEvent msg, String json, String[] why) {
        Verified v = verifyController(ce, json, null, null, true, why);
        if (v == null) return null;
        why[0] = childRefusal(ce, msg, v.region, v.agent);
        return why[0] == null ? v : null;
    }

    /**
     * Why the controller {@code region/agent} -- whose signature verified, with the key bound here as a controller -- is not
     * a child region's controller on this ping, or null: it is of this node's own region, or this node's parent, or the
     * broker stamped another caller on the message.
     */
    private static String childRefusal(ControllerEngine ce, MsgEvent msg, String region, String agent) {
        if (region.equals(ce.cstate.getRegion())) return region + "/" + agent + " is of this node's own region: not a child region";
        if (region.equals(ce.cstate.getGlobalRegion()) && agent.equals(ce.cstate.getGlobalAgent())) {
            return region + "/" + agent + " is this node's parent, not a child";
        }
        // the verified caller the broker stamped (phase 3): when there is one it must be this same controller ("anonymous"
        // only in the explicit insecure development mode, where no identity is verified and the signature alone decides)
        String caller = msg.getParam(io.cresco.library.security.Caller.PARAM_CALLER);
        if (caller != null && !caller.isEmpty() && !"anonymous".equals(caller)
                && !(caller.startsWith("controller:") && caller.endsWith("/" + region + "/" + agent))) {
            return "the verified caller " + caller + " is not the controller " + region + "/" + agent + " that signed the list";
        }
        return null;
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

    // ------------------------------------------------------------------ revocations by name

    private static final java.util.regex.Pattern FRESH = java.util.regex.Pattern.compile("[0-9a-f]{16}");
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();

    /** A region controller's ping: a new fresh value on it, kept with the last few (a request must repeat one of them). */
    private static void freshValue(State st, MsgEvent ping) {
        byte[] b = new byte[8];
        RANDOM.nextBytes(b);
        String v = java.util.HexFormat.of().formatHex(b);
        String[][] old = st.fresh, now = new String[Math.min(FRESH_KEPT, old.length + 1)][];
        now[0] = new String[]{v, Long.toString(System.currentTimeMillis())};
        System.arraycopy(old, 0, now, 1, now.length - 1);
        st.fresh = now;
        ping.setParam(NAMES_FRESH, v);
    }

    /** Sign {@code items} as this node's requests or answers for {@code to}; null when it cannot sign now. */
    private static String signNames(ControllerEngine ce, String kind, String to, String fresh, List<String> items) {
        try {
            CertificateManager cm = ce.getCertificateManager();
            X509Certificate[] mine = cm.getPublicCertificate();
            if (mine == null || mine.length == 0 || mine[0] == null) return null;
            long now = System.currentTimeMillis();
            if (now >= mine[0].getNotAfter().getTime() || now < mine[0].getNotBefore().getTime()) return null;
            SignedNames l = new SignedNames();
            l.kind = kind;
            l.issuer = ce.cstate.getRegion() + "/" + ce.cstate.getAgent();
            l.tenant = SecuritySettings.tenant(ce.getPluginBuilder().getConfig());
            l.to = to;
            l.fresh = fresh == null ? "" : fresh;
            l.issued = now;
            l.items = items;
            l.cert = Base64.getEncoder().encodeToString(mine[0].getEncoded());
            if (mine.length > 1 && mine[1] != null) {
                l.chain = new ArrayList<>();
                for (int i = 1; i < mine.length && mine[i] != null; i++) l.chain.add(Base64.getEncoder().encodeToString(mine[i].getEncoded()));
            }
            l.sig = Base64.getEncoder().encodeToString(cm.signWithIdentity(signedBytes(l)));
            return GSON.toJson(l);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parse and check signed requests or answers of {@code kind}: the certificate verifies here as the issuer and signed
     * them, the tenant they state is that certificate's, and they are addressed to THIS node. Sets {@code who} to the
     * issuer's {region, agent}. The caller checks WHO the issuer is. Null (and {@code why}) otherwise.
     */
    private static SignedNames verifyNames(ControllerEngine ce, String json, String kind, X509Certificate[] signer, String[] who, String[] why) {
        try {
            SignedNames l = GSON.fromJson(json, SignedNames.class);
            if (l == null || l.v != 2 || l.items == null || l.cert == null || l.sig == null || l.issuer == null || l.to == null
                    || l.tenant == null || l.fresh == null || !kind.equals(l.kind)) { why[0] = "malformed"; return null; }
            if (l.items.size() > NAMES_MAX) { why[0] = "too long"; return null; }
            CrescoIdentity id = verifySigner(ce, l.issuer, l.cert, l.chain, signedBytes(l), l.sig, signer, why);
            if (id == null) return null;
            if (!l.tenant.equals(id.getTenant())) {
                why[0] = "it states tenant " + l.tenant + ", and its certificate is of tenant " + id.getTenant();
                return null;
            }
            if (!l.to.equals(ce.cstate.getRegion() + "/" + ce.cstate.getAgent())) {
                why[0] = "it is addressed to " + l.to + ", not to this node";
                return null;
            }
            who[0] = id.getRegion();
            who[1] = id.getAgent();
            return l;
        } catch (Exception e) {
            why[0] = e.getClass().getSimpleName() + ": " + e.getMessage();
            return null;
        }
    }

    /**
     * At a global, on a verified region controller's ping while a revocation by name is open: the open ones of ITS region's
     * names, signed for this pong -- when it is the ONE controller this node records for that region, and its ping carried
     * a fresh value. Otherwise nothing is sent and the registry is told why (what the reminders say).
     */
    private static void askNames(ControllerEngine ce, PeerTrustRegistry reg, MsgEvent msg, String pinger, String fresh) {
        String region = pinger.substring(0, pinger.indexOf('/'));
        List<PeerTrustRegistry.NameRevocation> open = reg.nameRevocationsFor(region);
        if (open.isEmpty()) return;
        PeerTrustRegistry.RegionController rc = reg.controllerOf(region);
        String notSent = rc.why != null ? rc.why
                : !rc.is(pinger) ? pinger + " pinged, and the controller recorded for region " + region + " is " + String.join(", ", rc.names)
                : fresh == null || !FRESH.matcher(fresh).matches() ? pinger + " says nothing of revocations by name on its pings (an earlier build"
                + " does not; it is asked once it is upgraded)" : null;
        String signed = null;
        if (notSent == null) {
            long now = System.currentTimeMillis();
            List<String> items = new ArrayList<>(open.size());
            for (PeerTrustRegistry.NameRevocation x : open) {
                if (items.size() >= NAMES_MAX) break;
                items.add(PeerTrustRegistry.NameRevocation.ASKED.equals(x.state) ? "A|" + x.id + "|" + x.name + "|" + Math.max(0, now - x.t)
                        : "L|" + x.id + "|" + x.name + "|" + (x.allow == null ? "" : x.allow));
            }
            signed = signNames(ce, KIND_ASK, pinger, fresh, items);
            if (signed == null) notSent = "this node cannot sign now (its certificate is not valid)";
        }
        reg.noteNameRegion(region, pinger, notSent);
        if (signed != null) msg.setCompressedParam(NAMES, signed);
    }

    /**
     * At a global, while a revocation by name is open: a region controller's signed answers. WHO: verified exactly as a
     * child's revocation list is -- signed with the key bound HERE as a controller to the issuer's name, of another region
     * than this node's, not this node's parent, the broker-stamped caller (when there is one) that same controller,
     * addressed to this node -- AND the one controller this node records for that region, in the tenant recorded for it
     * (an answer from any other name bound under the region is not read). WHAT each answer then does is the registry's to
     * decide ({@link PeerTrustRegistry#acceptNameAnswer}). Returns the verified controller ({@code region/agent}), or null.
     */
    private static String acceptAnswers(ControllerEngine ce, State st, PeerTrustRegistry reg, MsgEvent msg, String json, String src) {
        String[] why = new String[1], who = new String[2];
        X509Certificate[] signer = new X509Certificate[1];
        SignedNames l = verifyNames(ce, json, KIND_REPORT, signer, who, why);
        String controller = null;
        if (l != null) {
            controller = who[0] + "/" + who[1];
            why[0] = controllerRefusal(ce, who[0], who[1], signer, null, null, true);
            if (why[0] == null) why[0] = childRefusal(ce, msg, who[0], who[1]);
            if (why[0] == null) {
                PeerTrustRegistry.RegionController rc = reg.controllerOf(who[0]);
                why[0] = rc.why != null ? rc.why : !rc.is(controller) ? "the controller recorded for region " + who[0] + " is " + String.join(", ", rc.names)
                        : !rc.tenant.equals(l.tenant) ? "it is of tenant " + l.tenant + ", and " + rc.name + " is recorded with tenant " + rc.tenant : null;
            }
        }
        if (l == null || why[0] != null) {
            warnOnce(ce, st, "nameanswers|" + src + "|" + why[0], "Answers to revocations by name on the ping from " + src + " REFUSED, none"
                    + " read: " + why[0]);
            return null;
        }
        int ignored = 0;
        String first = null;
        for (String item : l.items) {
            String bad = reg.acceptNameAnswer(who[0], controller, item);
            if (bad != null && ignored++ == 0) first = bad;
        }
        // one line per controller and reason every ten minutes, however many answers it sends
        if (ignored > 0) {
            warnOnce(ce, st, "nameanswer|" + controller + "|" + first, ignored + " answer(s) of " + controller + " to revocations by name"
                    + " IGNORED (" + first + ")");
        }
        return controller;
    }

    /**
     * At a region's controller, on its global's pong: the revocations by name the global asks of it. See the class comment
     * for what is taken. Without them on the pong nothing is asked (any more).
     */
    private static void onParentNames(ControllerEngine ce, State st, MsgEvent pong, String parentRegion, String parentAgent) {
        String json = pong.getParam(NAMES) == null ? null : pong.getCompressedParam(NAMES);
        if (json == null) {
            if (!st.nameAsks.isEmpty()) st.nameAsks = Map.of();
            return;
        }
        if (!ce.cstate.isRegionalController() || parentRegion == null || parentAgent == null) return;
        String parent = parentRegion + "/" + parentAgent, region = ce.cstate.getRegion();
        PeerTrustRegistry reg = ce.getCertificateManager().getPeerTrust();
        String[] why = new String[1], who = new String[2];
        X509Certificate[] signer = new X509Certificate[1];
        SignedNames l = verifyNames(ce, json, KIND_ASK, signer, who, why);
        long sentAt = 0;
        if (l != null) {
            why[0] = controllerRefusal(ce, who[0], who[1], signer, parentRegion, parentAgent, false);
            String tenant = reg.boundTenant(parentRegion, parentAgent);
            if (why[0] == null && tenant != null && !tenant.equals(l.tenant)) {
                why[0] = "it is of tenant " + l.tenant + ", and " + parent + " is bound here with tenant " + tenant;
            }
            // a node has ONE global (PeerTrustRegistry#join): where its registry records it, only that one asks
            String g = reg.globalRecorded();
            if (why[0] == null && g != null && !g.equals(parent)) why[0] = "this node's global is recorded here as " + g + ", not " + parent;
            if (why[0] == null) {
                // fresh: it repeats the value of one of this node's last pings (and was therefore signed after that ping)
                for (String[] f : st.fresh) if (f[0].equals(l.fresh)) sentAt = Long.parseLong(f[1]);
                if (sentAt == 0) why[0] = "it is not fresh: it answers none of this node's last " + FRESH_KEPT + " pings (held back, or delivered again)";
            }
        }
        if (l == null || why[0] != null) {
            warnOnce(ce, st, "names|" + why[0], "Revocations by name on the pong of " + parent + " REFUSED: " + why[0]);
            return;
        }
        Map<String, String[]> asks = new LinkedHashMap<>();
        for (String item : l.items) {
            String[] p = item == null ? new String[0] : item.split("\\|", -1);
            if (p.length < 4 || !("A".equals(p[0]) || "L".equals(p[0]))) continue;
            if (!p[2].startsWith(region + "/")) {
                warnOnce(ce, st, "names|other|" + p[2], "A revocation by name of " + p[2] + " on the pong of " + parent + " is IGNORED: it is"
                        + " not a name of this node's region " + region);
                continue;
            }
            if ("A".equals(p[0])) {
                long age;
                try { age = Math.max(0, Long.parseLong(p[3])); } catch (NumberFormatException e) { continue; }
                // the operator's act, by this node's clock: no earlier than (the ping this answers) - (its age when it was signed)
                reg.applyNameRevocation(p[1], p[2], sentAt - age, parent);
            } else {
                reg.applyNameRebind(p[1], p[2], p[3].isEmpty() ? null : p[3], parent);
            }
            asks.put(p[1], new String[]{p[0], p[2]});
        }
        st.nameAsks = asks;
    }

    /**
     * F-17, at a region's controller, on its global's pong: the answers to its questions about its nodes' former names
     * ({@link RecordedMoves}). Taken as revocations by name are: signed by the controller this node joined as its global (its
     * name from this node's own state, its tenant the one bound here, the one global its registry records), addressed to
     * this node, and fresh -- repeating the value of one of this node's last pings, so an answer held back or delivered
     * again is not taken. Nothing (one parameter read) when the pong carries none.
     */
    private static void onParentMoves(ControllerEngine ce, State st, MsgEvent pong, String parentRegion, String parentAgent) {
        if (pong.getParam(RecordedMoves.ANSWER) == null) return;
        String json = pong.getCompressedParam(RecordedMoves.ANSWER);
        if (json == null || !ce.cstate.isRegionalController() || parentRegion == null || parentAgent == null) return;
        String parent = parentRegion + "/" + parentAgent;
        PeerTrustRegistry reg = ce.getCertificateManager().getPeerTrust();
        String[] why = new String[1], who = new String[2];
        X509Certificate[] signer = new X509Certificate[1];
        SignedNames l = verifyNames(ce, json, KIND_MOVES, signer, who, why);
        if (l != null) {
            why[0] = controllerRefusal(ce, who[0], who[1], signer, parentRegion, parentAgent, false);
            String tenant = reg.boundTenant(parentRegion, parentAgent);
            if (why[0] == null && tenant != null && !tenant.equals(l.tenant)) {
                why[0] = "it is of tenant " + l.tenant + ", and " + parent + " is bound here with tenant " + tenant;
            }
            String g = reg.globalRecorded();
            if (why[0] == null && g != null && !g.equals(parent)) why[0] = "this node's global is recorded here as " + g + ", not " + parent;
            if (why[0] == null) {
                boolean fresh = false;
                for (String[] f : st.fresh) if (f[0].equals(l.fresh)) fresh = true;
                if (!fresh) why[0] = "it is not fresh: it answers none of this node's last " + FRESH_KEPT + " pings (held back, or delivered again)";
            }
        }
        if (l == null || why[0] != null) {
            warnOnce(ce, st, "moves|" + why[0], "Answers about former addresses (region_alias) on the pong of " + parent + " REFUSED: " + why[0]);
            return;
        }
        ce.recordedMoves().record(l.items);
    }

    /**
     * At a region's controller, before its ping: its answers to what its global last asked -- to a revocation once it is
     * answered and recorded here, to a rebind once the name is no longer reserved for it -- signed with this node's
     * identity key and addressed to its global. Nothing (one volatile read) while nothing is asked.
     */
    private static void answerNames(ControllerEngine ce, State st, PeerTrustRegistry reg, MsgEvent ping) {
        Map<String, String[]> asks = st.nameAsks;
        if (asks.isEmpty()) return;
        List<String> items = new ArrayList<>();
        for (Map.Entry<String, String[]> e : asks.entrySet()) {
            String id = e.getKey(), name = e.getValue()[1];
            if ("A".equals(e.getValue()[0])) {
                String[] a = reg.nameAnswer(id);
                // (W: the request was withdrawn before it was applied here -- the rebind that did so is what is answered)
                if (a != null && !"W".equals(a[0])) items.add(id + "|" + name + "|" + a[0] + "|" + (a[1] == null ? "" : a[1]));
            } else {
                String status = reg.nameLiftAnswer(id, name);
                if (status != null) items.add(id + "|" + name + "|" + status + "|");
            }
        }
        if (items.isEmpty()) return;
        String to = ce.cstate.getGlobalRegion() + "/" + ce.cstate.getGlobalAgent();
        String content = to + "|" + String.join(",", items);
        String[] last = st.nameReport;
        if (last == null || !last[0].equals(content)) {
            String signed = signNames(ce, KIND_REPORT, to, null, items);
            if (signed == null) return;
            st.nameReport = last = new String[]{content, signed};
        }
        ping.setCompressedParam(NAME_REPORT, last[1]);
    }
}
