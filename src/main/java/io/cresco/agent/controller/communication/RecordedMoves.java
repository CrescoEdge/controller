package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * F-17: is the former address a node reads (region_alias, {@link RegionAlias}) bound to a move the FABRIC recorded, not to
 * the node's own word?
 *
 * <p>A node may read the inbox of its former address, {@code T.<tenant>.<former region>_<its agent name>}. The former region
 * in that name is the node's own statement, so a node given the agent name of a node of another region (in the standard
 * profile an agent's name is its own claim, bound first come in its region) read that node's inbox. What the fabric does
 * record of a move is the KEY: the controller a node joined bound its name {@code <region>/<agent>} to the key it presented
 * (first come, strictly: a second key is refused unless the operator rebinds the name), and a node keeps its identity key
 * when it moves to another region (CertificateManager re-issues its leaf for the new name with the same key). So the inbox
 * of {@code <former region>_<agent>} is given to the node {@code <region>/<agent>} only when the key it presents is the one
 * bound to {@code <former region>/<agent>} where that name was recorded: the node that moved, and no other.
 *
 * <p>Where the record is: the documented move is out of the global's region (every agent on the global's broker first,
 * then one region per site), and the global binds the agents of its own region. A global answers from its own registry; a
 * region's controller asks its global -- the questions ride its ping ({@link #ASK}), the answers come on the pong signed
 * with the global's identity key, addressed to this controller and repeating the fresh value of one of its last pings
 * ({@link #ANSWER}, {@link RevocationSync}) -- and keeps them: a no for {@value #NO_MS} ms; a yes until the global says no
 * (asked again every {@value #YES_MS} ms, kept meanwhile and while the global is away). A former address in any other region is not recorded at a global: it is refused, as is any
 * where no global can be asked. Pre-provisioned identities (the production profile) are judged by the broker as before: the
 * agent name is the operator's issuance there (CrescoAuthorizationBroker.formerAddressRefusal).
 *
 * <p>Cost: asked when a consumer is added (a map read), never per message. At most one open question per key (a node that
 * names many former regions asks one at a time), at most {@value #ASK_MAX} per ping, at most {@value #MAX} kept.
 */
public final class RecordedMoves {

    /** On a region controller's ping: its questions, {@code tenant|region|agent|fp} joined by {@code ,}. */
    public static final String ASK = "alias_ask";
    /** On its global's pong: the signed answers, {@code tenant|region|agent|fp|1} (recorded) or {@code ...|0}. */
    public static final String ANSWER = "alias_moves";
    static final int ASK_MAX = 16;
    static final long YES_MS = 3_600_000L, NO_MS = 30_000L, ASK_AGAIN_MS = 5_000L, ASK_FOR_MS = 120_000L, ASK_NOW_EVERY_MS = 1_000L;
    static final int MAX = 4096;
    private static final java.util.regex.Pattern FP = java.util.regex.Pattern.compile("[0-9a-f]{64}");

    private final ControllerEngine ce;
    /** question -> {1 recorded / 0 not, valid until}. */
    private final Map<String, long[]> known = new ConcurrentHashMap<>();
    /** key fingerprint -> its one open question {question, first asked, last put on a ping (0: not yet)}. */
    private final Map<String, Object[]> open = new ConcurrentHashMap<>();
    private volatile long askWanted, askedNowAt;

    public RecordedMoves(ControllerEngine ce) {
        this.ce = ce;
    }

    static String question(String tenant, String region, String agent, String fp) {
        return tenant + "|" + region + "|" + agent + "|" + fp;
    }

    /** {tenant, region, agent, fp} of a well-formed question, else null. */
    static String[] parse(String q) {
        String[] p = q == null ? null : q.split("\\|", -1);
        if (p == null || p.length != 4 || !CrescoNames.isValidTenant(p[0]) || !CrescoNames.isValid(p[1]) || !CrescoNames.isValid(p[2])
                || !FP.matcher(p[3]).matches()) return null;
        return p;
    }

    /**
     * Does the key {@code fp} of the node {@code <its region>/<agent>} of {@code tenant} hold the former name
     * {@code formerRegion/agent} where the fabric recorded it? TRUE or FALSE; null when it is not known here yet: it has been
     * asked (the region controller's next ping, at once when its watcher looks), and the caller asks again later.
     */
    public Boolean moved(String tenant, String formerRegion, String agent, String fp) {
        if (fp != null) fp = fp.toLowerCase(java.util.Locale.ROOT);
        String q = question(tenant, formerRegion, agent, fp);
        if (parse(q) == null || ce == null || ce.cstate == null) return Boolean.FALSE;
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        if (cs.isGlobalController()) {
            // the global holds the record of its own region's names
            PeerTrustRegistry reg = registry();
            return reg != null && reg.agentKeyRecorded(tenant, formerRegion, agent, fp);
        }
        if (!cs.isRegionalController() || cs.getGlobalRegion() == null || cs.getGlobalAgent() == null) return Boolean.FALSE;  // nobody to ask
        long now = System.currentTimeMillis();
        long[] v = known.get(q);
        if (v != null && (v[1] > now || v[0] == 1)) {
            // a yes stands until the global says no (it changes only when the operator rebinds or revokes the former name):
            // past its time it is asked again, and kept meanwhile -- also while the global is away
            if (v[1] <= now) ask(q, fp, now);
            return v[0] == 1;
        }
        ask(q, fp, now);
        return null;
    }

    private PeerTrustRegistry registry() {
        CertificateManager cm = ce.getCertificateManager();
        return cm == null ? null : cm.getPeerTrust();
    }

    /** Open {@code q} for its key, unless that key has another question open (not answered yet, nor given up). */
    private void ask(String q, String fp, long now) {
        Object[] o = open.get(fp);
        if (o != null && !q.equals(o[0]) && now - (Long) o[1] < ASK_FOR_MS) return;
        if (o == null || !q.equals(o[0])) {
            if (open.size() >= MAX) return;
            open.put(fp, o = new Object[]{q, now, 0L});
        }
        if (now - (Long) o[2] >= ASK_AGAIN_MS) askWanted = now;    // not on a ping a moment ago: ping now
    }

    /** For the region's ping watcher (every 250 ms): ping now for a new question? At most once a second. */
    public boolean takeAskNow(long now) {
        long w = askWanted;
        if (w == 0L || open.isEmpty() || now - askedNowAt < ASK_NOW_EVERY_MS) return false;
        askWanted = 0L;
        askedNowAt = now;
        return true;
    }

    /** At a region's controller, before its ping to its global: the open questions (at most {@value #ASK_MAX}). */
    void onPing(MsgEvent ping) {
        if (open.isEmpty()) return;
        long now = System.currentTimeMillis();
        List<String> items = new ArrayList<>();
        for (Map.Entry<String, Object[]> e : open.entrySet()) {
            Object[] o = e.getValue();
            if (now - (Long) o[1] > ASK_FOR_MS) { open.remove(e.getKey(), o); continue; }     // a global that never answers
            if (items.size() >= ASK_MAX || now - (Long) o[2] < ASK_AGAIN_MS) continue;          // on a ping a moment ago
            items.add((String) o[0]);
            e.setValue(new Object[]{o[0], o[1], now});
        }
        if (!items.isEmpty()) ping.setParam(ASK, String.join(",", items));
    }

    /** At a global, on a verified region controller's ping: its questions answered from this node's registry, or null. */
    static List<String> answers(PeerTrustRegistry reg, String asked) {
        if (reg == null || asked == null || asked.isEmpty()) return null;
        List<String> out = new ArrayList<>();
        for (String q : asked.split(",", ASK_MAX + 1)) {
            if (out.size() >= ASK_MAX) break;
            String[] p = parse(q);
            if (p == null) continue;
            out.add(q + "|" + (reg.agentKeyRecorded(p[0], p[1], p[2], p[3]) ? "1" : "0"));
        }
        return out.isEmpty() ? null : out;
    }

    /** At a region's controller: its global's answers, verified (RevocationSync). */
    void record(List<String> items) {
        long now = System.currentTimeMillis();
        for (String item : items) {
            int bar = item == null ? -1 : item.lastIndexOf('|');
            if (bar < 0) continue;
            String q = item.substring(0, bar), a = item.substring(bar + 1);
            String[] p = parse(q);
            if (p == null || !("1".equals(a) || "0".equals(a))) continue;
            boolean yes = "1".equals(a);
            if (known.size() >= MAX) known.entrySet().removeIf(e -> e.getValue()[1] <= now);
            if (known.size() >= MAX) known.clear();
            known.put(q, new long[]{yes ? 1 : 0, now + (yes ? YES_MS : NO_MS)});
            Object[] o = open.get(p[3]);
            if (o != null && q.equals(o[0])) open.remove(p[3], o);
        }
    }

    /** Tests: the questions open now. */
    int openCount() { return open.size(); }
}
