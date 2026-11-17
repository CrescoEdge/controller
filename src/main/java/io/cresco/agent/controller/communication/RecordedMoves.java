package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;

import java.util.ArrayList;
import java.util.Comparator;
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
 * record of a node is its name bound to its KEY: the controller it joined bound {@code <region>/<agent>} to the key it
 * presented (first come, strictly), and a node keeps its identity key when it moves (CertificateManager re-issues its leaf for
 * the new name with the same key). So the inbox of {@code <former region>_<agent>} is the node {@code <region>/<agent>}'s only
 * when the key it presents is the one bound to {@code <former region>/<agent>} where that name was recorded.
 *
 * <p><b>Who answers.</b> The controller that bound the former name: the global for its own region's names; for any other
 * region, that region's ONE controller ({@link PeerTrustRegistry#controllerOf}), which the global asks for the asker (the
 * channel revocations by name use). A region's controller asks its global: its questions ride its ping ({@link #ASK}); the
 * global answers on the pong, signed, addressed to that controller and fresh ({@link #ANSWER}); a question about another
 * region goes, signed, on the pong to that region's controller ({@link #RELAY_ASK}), whose signed answer comes on its next
 * ping ({@link #RELAY_REPORT}) and is served to the asker on its next ping. A global answers only questions of the asking
 * controller's own tenant (any, for a superuser tenant's). Without a global nothing records a move: refused.
 *
 * <p><b>Kept.</b> A yes {@value #YES_MS} ms, asked again after {@value #REFRESH_MS} ms while a node's consumer uses it (the
 * broker's sweep says so: {@link #movedInUse}; such a question carries {@code |u}, and only then does the answering controller
 * refresh the former name's binding, so a move in use keeps it from being pruned); a no {@value #NO_MS} ms. A controller
 * that answered yes looks again at every later ping of the asker and sends a NO when the name was rebound, revoked or removed
 * since. Unknown: the broker gives the consumer and withholds its messages until the answer is yes (CrescoAuthorizationBroker).
 *
 * <p><b>Bounded.</b> One open question per asking identity ({@code tenant|agent|key}); at most {@value #MAX} open, the
 * oldest asked (then the oldest) making room; each ping carries the newest and the longest-waiting ({@value #ASK_MAX} in all),
 * so neither a flood of new identities nor an old backlog starves a question. Relayed: {@value #RELAY_PER_REGION} open per
 * region; answers given: {@value #GIVEN_MAX} per asker.
 */
public final class RecordedMoves implements CrescoAuthorizationBroker.MoveRecord {

    /** On a region controller's ping: its questions, {@code tenant|region|agent|fp[|u]} joined by {@code ,}. */
    public static final String ASK = "alias_ask";
    /** On its global's pong: the signed answers, {@code tenant|region|agent|fp|1} (recorded) or {@code ...|0}. */
    public static final String ANSWER = "alias_moves";
    /** On a global's pong to a region's one controller: signed questions about that region's names. */
    public static final String RELAY_ASK = "alias_relay_ask";
    /** On that controller's ping: its signed answers. */
    public static final String RELAY_REPORT = "alias_relay_report";
    static final String IN_USE = "|u";
    static final int ASK_MAX = 32, MAX = 4096, GIVEN_MAX = 1024, RELAY_PER_REGION = 64, ITEMS_MAX = 64;
    static final long YES_MS = 1_800_000L, REFRESH_MS = 600_000L, NO_MS = 300_000L, ASK_AGAIN_MS = 5_000L, ASK_FOR_MS = 120_000L,
            ASK_NOW_EVERY_MS = 1_000L;
    private static final java.util.regex.Pattern FP = java.util.regex.Pattern.compile("[0-9a-f]{64}");

    private final ControllerEngine ce;
    /** Asked here: question -> {1 recorded / 0 not, valid until, ask again (in use) after}. */
    private final Map<String, long[]> known = new ConcurrentHashMap<>();
    /** Asking identity ({@code tenant|agent|fp}) -> its one open question {question, first asked, last on a ping, in use, order}. */
    private final Map<String, Object[]> open = new ConcurrentHashMap<>();
    private volatile long askWanted, askedNowAt;
    private final java.util.concurrent.atomic.AtomicLong order = new java.util.concurrent.atomic.AtomicLong();
    private volatile java.util.function.Consumer<String> listener;

    /** At a global: asker controller -> the questions it was answered yes (looked at again on its pings). */
    private final Map<String, Map<String, Boolean>> given = new ConcurrentHashMap<>();
    /** At a global: asker controller -> the questions it asked that wait for another region's answer (sent when it comes). */
    private final Map<String, Map<String, Boolean>> waiting = new ConcurrentHashMap<>();
    /** At a global: relayed answers, question -> {1/0, valid until, ask again (in use) after}. */
    private final Map<String, long[]> relayed = new ConcurrentHashMap<>();
    /** At a global: per former region, the questions for its controller -> {first asked, last sent, in use}. */
    private final Map<String, Map<String, long[]>> relayOpen = new ConcurrentHashMap<>();
    /** At a region's controller: answers for its global to send, question -> 1/0; and the yes it gave (looked at again). */
    private final Map<String, Boolean> report = new ConcurrentHashMap<>();
    private final Map<String, Boolean> reportGiven = new ConcurrentHashMap<>();

    public RecordedMoves(ControllerEngine ce) {
        this.ce = ce;
    }

    /** Told the question ({@code tenant|region|agent|fp}) of each answer that arrives here (the broker judges its consumers). */
    public void setListener(java.util.function.Consumer<String> l) {
        this.listener = l;
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

    @Override
    public Boolean moved(String tenant, String formerRegion, String agent, String fp) {
        return moved(tenant, formerRegion, agent, fp, false);
    }

    @Override
    public Boolean movedInUse(String tenant, String formerRegion, String agent, String fp) {
        return moved(tenant, formerRegion, agent, fp, true);
    }

    /**
     * Does the key {@code fp} of the node {@code <its region>/<agent>} of {@code tenant} hold the former name
     * {@code formerRegion/agent} where the fabric recorded it? TRUE or FALSE; null when it is not known here yet: it has been
     * asked, and the caller withholds until it is. {@code inUse}: a consumer of that node uses the answer (the broker's
     * sweep): a yes is asked again after {@value #REFRESH_MS} ms, as a use that keeps the binding.
     */
    Boolean moved(String tenant, String formerRegion, String agent, String fp, boolean inUse) {
        if (fp != null) fp = fp.toLowerCase(java.util.Locale.ROOT);
        String q = question(tenant, formerRegion, agent, fp);
        if (parse(q) == null || ce == null || ce.cstate == null) return Boolean.FALSE;
        io.cresco.agent.core.ControllerStateImp cs = ce.cstate;
        long now = System.currentTimeMillis();
        if (cs.isGlobalController()) {
            if (formerRegion.equals(cs.getRegion())) {
                PeerTrustRegistry reg = registry();
                return reg != null && reg.agentKeyRecorded(tenant, formerRegion, agent, fp, inUse);
            }
            long[] v = relayed.get(q);
            if (v != null && v[1] > now) {
                if (v[0] == 1 && inUse && now >= v[2]) relayAsk(formerRegion, q, true, now);
                return v[0] == 1;
            }
            if (!recordedController(formerRegion)) return Boolean.FALSE;
            relayAsk(formerRegion, q, inUse, now);
            return null;
        }
        if (!cs.isRegionalController() || !withGlobal(cs)) return Boolean.FALSE;                // nobody records it
        long[] v = known.get(q);
        if (v != null && v[1] > now) {
            if (v[0] == 1 && inUse && now >= v[2]) ask(q, tenant, agent, fp, true, now);
            return v[0] == 1;
        }
        ask(q, tenant, agent, fp, inUse, now);
        return null;
    }

    /**
     * Has this region a global -- joined now, joined or begun to join since it started (away now: a global outage, a
     * restart), or configured with one it has not reached yet? Only a region that has none answers no for want of a record;
     * the others wait for the answer (a no taken while the global is away closed a moved node's consumer).
     */
    private boolean withGlobal(io.cresco.agent.core.ControllerStateImp cs) {
        if (cs.getGlobalRegion() != null && cs.getGlobalAgent() != null) return true;
        if (ce.knewAGlobal()) return true;
        io.cresco.library.agent.ControllerMode m = cs.getControllerState();
        if (m == io.cresco.library.agent.ControllerMode.REGION_GLOBAL_INIT || m == io.cresco.library.agent.ControllerMode.REGION_GLOBAL_FAILED
                || m == io.cresco.library.agent.ControllerMode.REGION_GLOBAL) return true;
        try {
            String host = ce.getPluginBuilder().getConfig().getStringParam("global_controller_host");
            return host != null && !host.isBlank();
        } catch (RuntimeException e) {
            return false;
        }
    }

    private PeerTrustRegistry registry() {
        CertificateManager cm = ce.getCertificateManager();
        return cm == null ? null : cm.getPeerTrust();
    }

    /** Open {@code q} as the one question of its asking identity (a newer one replaces it); full: the oldest asked makes room. */
    private void ask(String q, String tenant, String agent, String fp, boolean inUse, long now) {
        String who = tenant + "|" + agent + "|" + fp;
        Object[] o = open.get(who);
        if (o == null || !q.equals(o[0])) {
            if (o == null && open.size() >= MAX) evictOne();
            open.put(who, o = new Object[]{q, now, 0L, inUse, order.incrementAndGet()});
        } else if (inUse && !(Boolean) o[3]) {
            open.put(who, o = new Object[]{q, o[1], o[2], true, o[4]});
        }
        if (now - (Long) o[2] >= ASK_AGAIN_MS) askWanted = now;      // not on a ping a moment ago: ping now
    }

    /** Room for one: the oldest question already put on a ping (its answer is still taken), else the oldest. */
    private void evictOne() {
        Map.Entry<String, Object[]> oldestSent = null, oldest = null;
        for (Map.Entry<String, Object[]> e : open.entrySet()) {
            Object[] o = e.getValue();
            if (oldest == null || (Long) o[4] < (Long) oldest.getValue()[4]) oldest = e;
            if ((Long) o[2] > 0 && (oldestSent == null || (Long) o[4] < (Long) oldestSent.getValue()[4])) oldestSent = e;
        }
        Map.Entry<String, Object[]> out = oldestSent != null ? oldestSent : oldest;
        if (out != null) open.remove(out.getKey(), out.getValue());
    }

    /** For the region's ping watcher (every 250 ms): ping now for a new question or a relayed answer? At most once a second. */
    public boolean takeAskNow(long now) {
        long w = askWanted;
        if (w == 0L || (open.isEmpty() && report.isEmpty()) || now - askedNowAt < ASK_NOW_EVERY_MS) return false;
        askWanted = 0L;
        askedNowAt = now;
        return true;
    }

    /**
     * At a region's controller, before its ping to its global: the open questions -- the newest and the longest-waiting
     * among those not on a ping a moment ago, {@value #ASK_MAX} in all.
     */
    void onPing(MsgEvent ping) {
        if (open.isEmpty()) return;
        long now = System.currentTimeMillis();
        List<Map.Entry<String, Object[]>> due = new ArrayList<>();
        for (Map.Entry<String, Object[]> e : open.entrySet()) {
            Object[] o = e.getValue();
            if (now - (Long) o[1] > ASK_FOR_MS) { open.remove(e.getKey(), o); continue; }       // a global that never answers
            if (now - (Long) o[2] >= ASK_AGAIN_MS) due.add(e);
        }
        if (due.isEmpty()) return;
        due.sort(Comparator.comparingLong(e -> (Long) e.getValue()[4]));
        List<Map.Entry<String, Object[]>> pick = new ArrayList<>();
        for (int i = due.size() - 1; i >= 0 && pick.size() < ASK_MAX / 2; i--) pick.add(due.get(i));      // the newest
        for (int i = 0; i < due.size() && pick.size() < ASK_MAX; i++) if (!pick.contains(due.get(i))) pick.add(due.get(i));  // the oldest
        List<String> items = new ArrayList<>(pick.size());
        for (Map.Entry<String, Object[]> e : pick) {
            Object[] o = e.getValue();
            items.add(o[0] + ((Boolean) o[3] ? IN_USE : ""));
            open.replace(e.getKey(), o, new Object[]{o[0], o[1], now, o[3], o[4]});
        }
        ping.setParam(ASK, String.join(",", items));
    }

    /** At a region's controller: its global's verified answers. */
    void record(List<String> items) {
        long now = System.currentTimeMillis();
        for (String item : items) {
            String[] qa = split(item);
            if (qa == null) continue;
            String[] p = parse(qa[0]);
            boolean yes = "1".equals(qa[1]);
            if (known.size() >= MAX) known.entrySet().removeIf(e -> e.getValue()[1] <= now);
            if (known.size() >= MAX) known.clear();
            known.put(qa[0], yes ? new long[]{1, now + YES_MS, now + REFRESH_MS} : new long[]{0, now + NO_MS, now + NO_MS});
            String who = p[0] + "|" + p[2] + "|" + p[3];
            Object[] o = open.get(who);
            if (o != null && qa[0].equals(o[0])) open.remove(who, o);
            tell(qa[0]);
        }
    }

    private void tell(String q) {
        java.util.function.Consumer<String> l = listener;
        if (l != null) try { l.accept(q); } catch (RuntimeException ignore) { }
    }

    /** {question, "1" or "0"} of an answer item, or null. */
    private static String[] split(String item) {
        int bar = item == null ? -1 : item.lastIndexOf('|');
        if (bar < 0) return null;
        String q = item.substring(0, bar), a = item.substring(bar + 1);
        return parse(q) != null && ("1".equals(a) || "0".equals(a)) ? new String[]{q, a} : null;
    }

    // ------------------------------------------------------------------ at a global

    /**
     * At a global, on the ping of the verified region controller {@code asker} of {@code askerTenant}: the answers to its
     * questions ({@code asked}, may be null) -- of its own tenant only, unless {@code superuser} -- from this node's registry
     * for its own region's names and from relayed answers for another region's (asked of that region's controller when not
     * known); then a NO for every yes it was given earlier that no longer holds. At most {@value #ITEMS_MAX} items; empty: none.
     */
    List<String> answersFor(String asker, String askerTenant, boolean superuser, String asked) {
        List<String> out = new ArrayList<>();
        PeerTrustRegistry reg = registry();
        String own = ce.cstate.getRegion();
        long now = System.currentTimeMillis();
        Map<String, Boolean> mine = given.get(asker);
        java.util.Set<String> answered = new java.util.HashSet<>();
        if (asked != null && !asked.isEmpty() && reg != null) {
            int n = 0;
            for (String item : asked.split(",", ASK_MAX + 1)) {
                if (n++ >= ASK_MAX) break;
                boolean inUse = item.endsWith(IN_USE);
                String q = inUse ? item.substring(0, item.length() - IN_USE.length()) : item;
                String[] p = parse(q);
                if (p == null || (!superuser && !p[0].equals(askerTenant))) continue;       // another tenant's names: not answered
                Boolean v;
                if (p[1].equals(own)) {
                    v = reg.agentKeyRecorded(p[0], p[1], p[2], p[3], inUse);
                } else {
                    long[] r = relayed.get(q);
                    if (r != null && r[1] > now && !(inUse && r[0] == 1 && now >= r[2])) {
                        v = r[0] == 1;
                    } else if (!recordedController(p[1])) {
                        v = Boolean.FALSE;                              // no one controller recorded for it: nothing records the name
                    } else {
                        relayAsk(p[1], q, inUse, now);
                        v = r != null && r[1] > now ? Boolean.valueOf(r[0] == 1) : null;     // a yes being refreshed still holds
                        if (v == null) {
                            Map<String, Boolean> w = waiting.computeIfAbsent(asker, k -> new ConcurrentHashMap<>());
                            if (w.size() < ASK_MAX * 4) w.put(q, Boolean.TRUE);
                        }
                    }
                }
                if (v == null) continue;
                out.add(q + "|" + (v ? "1" : "0"));
                answered.add(q);
                if (v) {
                    if (mine == null) given.put(asker, mine = new ConcurrentHashMap<>());
                    if (mine.size() < GIVEN_MAX || mine.containsKey(q)) mine.put(q, Boolean.TRUE);
                } else if (mine != null) {
                    mine.remove(q);
                }
            }
        }
        // relayed answers that came since this asker asked: sent now, not on its next question
        Map<String, Boolean> w = waiting.get(asker);
        if (w != null) {
            for (String q : new ArrayList<>(w.keySet())) {
                if (out.size() >= ITEMS_MAX) break;
                if (answered.contains(q)) { w.remove(q); continue; }
                long[] r = relayed.get(q);
                if (r == null || r[1] <= now) {
                    long[] o = relayOpen.getOrDefault(parse(q)[1], Map.of()).get(q);
                    if (o == null) w.remove(q);                       // given up: the asker asks again
                    continue;
                }
                boolean yes = r[0] == 1;
                out.add(q + "|" + (yes ? "1" : "0"));
                answered.add(q);
                w.remove(q);
                if (yes) {
                    if (mine == null) given.put(asker, mine = new ConcurrentHashMap<>());
                    if (mine.size() < GIVEN_MAX || mine.containsKey(q)) mine.put(q, Boolean.TRUE);
                }
            }
        }
        if (mine != null) {
            for (String q : new ArrayList<>(mine.keySet())) {
                if (out.size() >= ITEMS_MAX) break;
                if (answered.contains(q)) continue;
                String[] p = parse(q);
                Boolean still;
                if (p[1].equals(own)) still = reg != null && reg.agentKeyRecorded(p[0], p[1], p[2], p[3], false);
                else { long[] r = relayed.get(q); still = r == null || r[1] <= now ? null : Boolean.valueOf(r[0] == 1); }
                if (Boolean.FALSE.equals(still)) {
                    out.add(q + "|0");
                    mine.remove(q);
                }
            }
        }
        return out;
    }

    /** At a global: is ONE controller recorded for {@code region} (who would hold its names' record)? */
    private boolean recordedController(String region) {
        PeerTrustRegistry reg = registry();
        return reg != null && reg.controllerOf(region).why == null;
    }

    /** At a global: ask the controller of {@code region} about {@code q} (on its next ping's pong). */
    private void relayAsk(String region, String q, boolean inUse, long now) {
        Map<String, long[]> m = relayOpen.get(region);
        if (m == null) {
            if (relayOpen.size() >= MAX) return;
            relayOpen.putIfAbsent(region, new ConcurrentHashMap<>());
            m = relayOpen.get(region);
        }
        long[] o = m.get(q);
        if (o == null) {
            if (m.size() >= RELAY_PER_REGION) return;
            m.put(q, new long[]{now, 0L, inUse ? 1 : 0});
        } else if (inUse && o[2] == 0) {
            m.put(q, new long[]{o[0], o[1], 1});
        }
    }

    /** At a global: are questions waiting for {@code region}'s controller? (A map read.) */
    boolean hasRelayAsks(String region) {
        Map<String, long[]> m = relayOpen.get(region);
        return m != null && !m.isEmpty();
    }

    /** At a global, on the ping of {@code region}'s one controller: the questions for it, or null (at most {@value #ASK_MAX}). */
    List<String> relayAsksFor(String region) {
        Map<String, long[]> m = relayOpen.get(region);
        if (m == null || m.isEmpty()) return null;
        long now = System.currentTimeMillis();
        List<String> items = new ArrayList<>();
        for (Map.Entry<String, long[]> e : m.entrySet()) {
            long[] o = e.getValue();
            if (now - o[0] > ASK_FOR_MS) { m.remove(e.getKey(), o); continue; }
            if (items.size() >= ASK_MAX || now - o[1] < ASK_AGAIN_MS) continue;
            items.add(e.getKey() + (o[2] == 1 ? IN_USE : ""));
            m.replace(e.getKey(), o, new long[]{o[0], now, o[2]});
        }
        return items.isEmpty() ? null : items;
    }

    /** At a global: the verified answers of {@code region}'s one controller (about its own names only). */
    void relayReported(String region, List<String> items) {
        long now = System.currentTimeMillis();
        Map<String, long[]> m = relayOpen.get(region);
        for (String item : items) {
            String[] qa = split(item);
            if (qa == null || !region.equals(parse(qa[0])[1])) continue;
            boolean yes = "1".equals(qa[1]);
            if (relayed.size() >= MAX) relayed.entrySet().removeIf(e -> e.getValue()[1] <= now);
            if (relayed.size() >= MAX) relayed.clear();
            relayed.put(qa[0], yes ? new long[]{1, now + YES_MS, now + REFRESH_MS} : new long[]{0, now + NO_MS, now + NO_MS});
            if (m != null) m.remove(qa[0]);
            tell(qa[0]);                                   // a node on this global's own broker may use it
        }
    }

    // ------------------------------------------------------------------ at a former region's controller

    /** At a region's controller: its global's verified questions about this region's names, answered from its registry. */
    void answerRelay(List<String> items) {
        PeerTrustRegistry reg = registry();
        String own = ce.cstate.getRegion();
        if (reg == null) return;
        for (String item : items) {
            boolean inUse = item.endsWith(IN_USE);
            String q = inUse ? item.substring(0, item.length() - IN_USE.length()) : item;
            String[] p = parse(q);
            if (p == null || !p[1].equals(own) || report.size() >= ITEMS_MAX) continue;
            boolean yes = reg.agentKeyRecorded(p[0], p[1], p[2], p[3], inUse);
            report.put(q, yes);
            if (yes) { if (reportGiven.size() < GIVEN_MAX) reportGiven.put(q, Boolean.TRUE); }
            else reportGiven.remove(q);
        }
        if (!report.isEmpty()) askWanted = System.currentTimeMillis();
    }

    /**
     * At a region's controller, before its ping: its answers for its global, and a NO for every yes it gave that no longer
     * holds (the name rebound, revoked, removed). Null when there is nothing to say. Taken: sent once.
     */
    List<String> takeReport() {
        PeerTrustRegistry reg = registry();
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Boolean> e : report.entrySet()) {
            if (out.size() >= ITEMS_MAX) break;
            out.add(e.getKey() + "|" + (e.getValue() ? "1" : "0"));
            report.remove(e.getKey(), e.getValue());
        }
        if (reg != null) {
            for (String q : new ArrayList<>(reportGiven.keySet())) {
                if (out.size() >= ITEMS_MAX) break;
                String[] p = parse(q);
                if (!reg.agentKeyRecorded(p[0], p[1], p[2], p[3], false)) {
                    out.add(q + "|0");
                    reportGiven.remove(q);
                }
            }
        }
        return out.isEmpty() ? null : out;
    }

    /** Tests: the questions open now. */
    int openCount() { return open.size(); }
}
