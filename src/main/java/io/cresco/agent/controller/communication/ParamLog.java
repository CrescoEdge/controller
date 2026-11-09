package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;

import java.util.Map;
import java.util.TreeMap;

/**
 * What a log line may say about a message: its type, action and header, and the NAMES of its parameters with the size of
 * each value -- never a value (a parameter may carry credentials, file contents, a user's data or another caller's
 * traffic). Every controller log of a message goes through {@link #describe}.
 */
public final class ParamLog {

    private ParamLog() { }

    private static final int MAX_NAMES = 40;
    private static final java.util.concurrent.ConcurrentHashMap<String, Long> LAST = new java.util.concurrent.ConcurrentHashMap<>();

    /** Rate limit for a log line of kind {@code key}: true at most once per 10 s per key (keys are bounded by the caller). */
    public static boolean allow(String key) {
        long now = System.currentTimeMillis();
        Long last = LAST.get(key);
        if (last != null && now - last < 10_000L) return false;
        if (LAST.size() > 1_000) LAST.clear();
        LAST.put(key, now);
        return true;
    }

    /** The one line logged for an action no executor knows: names and sizes only, at most once per 10 s per tier. */
    public static String unknownAction(MsgEvent m) {
        return "unknown action (" + describe(m) + ")";
    }

    /** {@code type=.. action=.. <header> params={name:size, ...}} without any parameter value. */
    public static String describe(MsgEvent m) {
        if (m == null) return "(no message)";
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("type=").append(m.getMsgType()).append(" action=").append(safeAction(m.getParam("action")))
                    .append(" src=").append(m.getSrcRegion()).append('_').append(m.getSrcAgent()).append('/').append(m.getSrcPlugin())
                    .append(" dst=").append(m.getDstRegion()).append('_').append(m.getDstAgent()).append('/').append(m.getDstPlugin());
            Map<String, String> p = m.getParams();
            if (p != null) {
                sb.append(" params={");
                int n = 0;
                for (Map.Entry<String, String> e : new TreeMap<>(p).entrySet()) {
                    if (n++ == MAX_NAMES) { sb.append(", ... ").append(p.size() - MAX_NAMES).append(" more"); break; }
                    if (n > 1) sb.append(", ");
                    sb.append(safeName(e.getKey())).append(':').append(e.getValue() == null ? 0 : e.getValue().length());
                }
                sb.append('}');
            }
        } catch (RuntimeException e) {
            sb.append(" (unreadable)");
        }
        return sb.toString();
    }

    /** The names (and value sizes) of a parameter map of any value type, never a value: {@code {name:size, ...}}. */
    public static String names(Map<String, ?> p) {
        if (p == null) return "{}";
        StringBuilder sb = new StringBuilder("{");
        int n = 0;
        try {
            for (Map.Entry<String, ?> e : new TreeMap<>(p).entrySet()) {
                if (n++ == MAX_NAMES) { sb.append(", ... ").append(p.size() - MAX_NAMES).append(" more"); break; }
                if (n > 1) sb.append(", ");
                Object v = e.getValue();
                sb.append(safeName(e.getKey())).append(':').append(v == null ? 0 : String.valueOf(v).length());
            }
        } catch (RuntimeException unreadable) {
            sb.append(" (unreadable)");
        }
        return sb.append('}').toString();
    }

    /** {@link #describe} for a raw parameter map. */
    public static String describe(Map<String, String> p) {
        MsgEvent m = new MsgEvent();
        m.setParams(p == null ? new java.util.HashMap<>() : p);
        return describe(m);
    }

    private static final java.util.regex.Pattern ACTION = java.util.regex.Pattern.compile("[A-Za-z0-9_.-]+");
    private static final java.util.regex.Pattern NAME = java.util.regex.Pattern.compile("[A-Za-z0-9_.:@/-]+");

    /** An action name is a short token; anything else is logged by its size only. */
    private static String safeAction(String a) {
        if (a == null) return "null";
        return a.length() <= 64 && ACTION.matcher(a).matches() ? a : "(" + a.length() + " chars)";
    }

    private static String safeName(String k) {
        if (k == null) return "null";
        return k.length() <= 128 && NAME.matcher(k).matches() ? k : "(" + k.length() + " chars)";
    }

    /**
     * {@link #describe} when, and only if, a log line is actually written: for a debug or trace line on a message's
     * path, {@code logger.trace("... {}", ParamLog.of(m))}. Described eagerly, every message paid for a line that was
     * never written (a fifth of a controller's CPU per message on the control plane).
     */
    public static Object of(MsgEvent m) {
        return new Object() {
            @Override public String toString() { return describe(m); }
        };
    }

    /** {@link #names} when, and only if, a log line is actually written (see {@link #of}). */
    public static Object namesOf(Map<String, ?> p) {
        return new Object() {
            @Override public String toString() { return names(p); }
        };
    }
}
