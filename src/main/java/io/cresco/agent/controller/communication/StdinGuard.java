package io.cresco.agent.controller.communication;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.filter.BinaryExpression;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.Expression;
import org.apache.activemq.filter.LogicExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.filter.PropertyExpression;
import org.apache.activemq.filter.UnaryExpression;
import org.apache.activemq.selector.SelectorParser;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * No dataplane subscriber but a process's own executor sees that process's stdin (dataplane messages with
 * {@code type} {@code input} or {@code input-<id>}). Defence in depth: the executor takes stdin through its
 * {@code write_stdin} action now, but an older writer may still put stdin on the dataplane.
 *
 * <ul>
 *   <li><b>The guarantee</b>: every selector is narrowed, unconditionally, to {@code (<selector>) AND }{@link #NO_STDIN}
 *       ({@link #narrow}). The selector is parsed on its own first and the narrowed form is parsed again and checked to be
 *       exactly that conjunction, so no {@code OR}, parenthesis or quoting in the selector reaches past the exclusion.
 *       What a selector contains never decides whether it is narrowed (round-4 review F1: a selector that merely
 *       contained the text of {@link #NO_STDIN} used to be passed through unchanged).</li>
 *   <li><b>A clear error</b>: a selector that asks for stdin is refused ({@link #refusal}). Judged on the parsed
 *       selector, never on its text: a comparison of the property {@code type} that is reached without a negation, holds
 *       for a stdin type and does not hold for an ordinary one names stdin. A selector that does not parse, or that
 *       evaluates message bodies (XPATH, XQUERY), is refused.</li>
 * </ul>
 * Applied per bundle by the controller's dataplane (the executor bundle is exempt), and by the broker to every dataplane
 * subscription that is not this controller's own or a bridge's (the broker cannot tell the plugins of a remote node
 * apart; that node's controller does).
 */
public final class StdinGuard {

    private StdinGuard() { }

    /** The executor: the one bundle a process's stdin is for. */
    public static final String EXECUTOR_BUNDLE = "io.cresco.executor";

    /** Matches no stdin message: no {@code type}, or a {@code type} other than {@code input} and {@code input-*}. */
    public static final String NO_STDIN = "(type IS NULL OR (type <> 'input' AND type NOT LIKE 'input-%'))";

    /** The stdin types a refusal probes with, besides the stdin literals the selector itself names. */
    private static final String[] STDIN_PROBES = {"input", "input-0", "input-0a1b2c", "input-"};
    /** A type that is not stdin: a comparison that holds for it too does not single stdin out. */
    private static final String ORDINARY_PROBE = "zz-cresco-not-stdin";

    private static final String REFUSED = "refused: the selector asks for a process's stdin (type 'input' or 'input-*'); stdin never "
            + "travels on the dataplane to any subscriber but the executor (write it with the executor's write_stdin action). A "
            + "selector that only excludes it (type <> 'input', NOT LIKE 'input-%') is accepted";

    /** Is {@code type} a stdin type ({@code input} or {@code input-*})? */
    static boolean isStdinType(String type) {
        return type != null && (type.equals("input") || type.startsWith("input-"));
    }

    /** Why {@code selector} is refused (malformed, evaluates bodies, or asks for stdin), or null. */
    public static String refusal(String selector) {
        if (selector == null || selector.trim().isEmpty()) return null;
        BooleanExpression parsed;
        try {
            parsed = SelectorParser.parse(selector);
        } catch (Exception e) {
            return "refused: the selector does not parse (" + e.getMessage() + ")";
        }
        if (mentionsBodyEvaluator(parsed)) return "refused: a dataplane selector filters on properties only (no XPATH/XQUERY)";
        Set<String> candidates = new LinkedHashSet<>();
        for (String p : STDIN_PROBES) candidates.add(p);
        for (String lit : literals(selector)) if (isStdinType(lit)) candidates.add(lit);
        List<BooleanExpression> named = new ArrayList<>();
        try {
            walk(parsed, true, named);
        } catch (Opaque opaque) {
            return null;                                      // unknown structure: narrowing alone keeps stdin out
        }
        for (BooleanExpression leaf : named) {
            if (matchesAny(leaf, candidates) && !matches(leaf, ORDINARY_PROBE)) return REFUSED;
        }
        return null;
    }

    /**
     * {@code selector} narrowed so it never matches stdin: always {@code (<selector>) AND }{@link #NO_STDIN}, checked to
     * parse as exactly that conjunction. A selector that does not parse on its own (or would not stay inside its
     * parentheses) is refused with an {@link IllegalArgumentException}; callers run {@link #refusal} first.
     */
    public static String narrow(String selector) {
        if (selector == null || selector.trim().isEmpty()) return NO_STDIN;
        BooleanExpression own;
        try {
            own = SelectorParser.parse(selector);
        } catch (Exception e) {
            throw new IllegalArgumentException("the selector does not parse: " + e.getMessage());
        }
        String narrowed = "(" + selector + ") AND " + NO_STDIN;
        BooleanExpression whole;
        try {
            whole = SelectorParser.parse(narrowed);
        } catch (Exception e) {
            throw new IllegalArgumentException("the narrowed selector does not parse: " + e.getMessage());
        }
        if (!isConjunctionOf(whole, own)) {
            throw new IllegalArgumentException("the selector does not stay inside its parentheses");
        }
        return narrowed;
    }

    /** Is {@code whole} exactly {@code own AND NO_STDIN}? */
    private static boolean isConjunctionOf(BooleanExpression whole, BooleanExpression own) {
        if (!(whole instanceof LogicExpression)) return false;
        LogicExpression and = (LogicExpression) whole;
        if (!"AND".equals(and.getExpressionSymbol())) return false;
        List<BooleanExpression> ops = operands(and);
        if (ops == null || ops.size() != 2) return false;
        try {
            return ops.get(0).toString().equals(own.toString())
                    && ops.get(1).toString().equals(SelectorParser.parse(NO_STDIN).toString());
        } catch (Exception e) {
            return false;
        }
    }

    // ---------------------------------------------------------------- the parsed selector

    private static final class Opaque extends Exception {
        Opaque() { super(null, null, false, false); }
    }

    /** Collect the comparisons of {@code type} reached with positive polarity (an even number of NOTs above them). */
    private static void walk(Expression e, boolean positive, List<BooleanExpression> named) throws Opaque {
        if (e instanceof LogicExpression) {
            List<BooleanExpression> ops = operands((LogicExpression) e);
            if (ops == null) throw new Opaque();
            for (BooleanExpression o : ops) walk(o, positive, named);
            return;
        }
        if (e instanceof UnaryExpression && "NOT".equals(((UnaryExpression) e).getExpressionSymbol())) {
            walk(((UnaryExpression) e).getRight(), !positive, named);
            return;
        }
        if (e instanceof BooleanExpression && referencesType(e)) {
            if (positive) named.add((BooleanExpression) e);
        }
    }

    private static boolean referencesType(Expression e) {
        if (e instanceof PropertyExpression) return "type".equals(((PropertyExpression) e).getName());
        if (e instanceof BinaryExpression) {
            return referencesType(((BinaryExpression) e).getLeft()) || referencesType(((BinaryExpression) e).getRight());
        }
        if (e instanceof UnaryExpression) return referencesType(((UnaryExpression) e).getRight());
        return false;
    }

    private static boolean mentionsBodyEvaluator(Expression e) {
        String n = e.getClass().getName();
        if (n.contains("XPath") || n.contains("XQuery")) return true;
        if (e instanceof LogicExpression) {
            List<BooleanExpression> ops = operands((LogicExpression) e);
            if (ops == null) return true;
            for (BooleanExpression o : ops) if (mentionsBodyEvaluator(o)) return true;
            return false;
        }
        if (e instanceof BinaryExpression) {
            return mentionsBodyEvaluator(((BinaryExpression) e).getLeft()) || mentionsBodyEvaluator(((BinaryExpression) e).getRight());
        }
        if (e instanceof UnaryExpression) return mentionsBodyEvaluator(((UnaryExpression) e).getRight());
        return false;
    }

    private static volatile java.lang.reflect.Field logicOperands;

    /** The operands of an AND/OR (n-ary in ActiveMQ 6), or null when they cannot be read. */
    @SuppressWarnings("unchecked")
    private static List<BooleanExpression> operands(LogicExpression e) {
        try {
            java.lang.reflect.Field f = logicOperands;
            if (f == null) {
                f = LogicExpression.class.getDeclaredField("expressions");
                f.setAccessible(true);
                logicOperands = f;
            }
            return new ArrayList<>((List<BooleanExpression>) f.get(e));
        } catch (Exception reflectionUnavailable) {
            BooleanExpression l = e.getLeft(), r = e.getRight();
            if (l == null || r == null) return null;
            List<BooleanExpression> two = new ArrayList<>(2);
            two.add(l);
            two.add(r);
            return two;
        }
    }

    private static boolean matchesAny(BooleanExpression leaf, Set<String> types) {
        for (String t : types) if (matches(leaf, t)) return true;
        return false;
    }

    private static boolean matches(BooleanExpression leaf, String type) {
        try {
            ActiveMQMessage m = new ActiveMQMessage();
            m.setStringProperty("type", type);
            MessageEvaluationContext ctx = new NonCachedMessageEvaluationContext();
            ctx.setMessageReference(m);
            return leaf.matches(ctx);
        } catch (Exception e) {
            return false;
        }
    }

    /** The string literals of a selector, read with the selector grammar's quoting ({@code ''} is a quote). */
    static List<String> literals(String selector) {
        List<String> out = new ArrayList<>();
        int i = 0, n = selector.length();
        while (i < n) {
            if (selector.charAt(i) != '\'') { i++; continue; }
            StringBuilder b = new StringBuilder();
            i++;
            while (i < n) {
                char c = selector.charAt(i);
                if (c == '\'') {
                    if (i + 1 < n && selector.charAt(i + 1) == '\'') { b.append('\''); i += 2; continue; }
                    break;
                }
                b.append(c);
                i++;
            }
            out.add(b.toString());
            i++;
        }
        return out;
    }

    /** A dataplane topic: {@code agent.event}, {@code region.event}, {@code global.event}, shards, tenant-qualified. */
    public static boolean isDataplaneTopic(org.apache.activemq.command.ActiveMQDestination d) {
        if (d == null || !d.isTopic()) return false;
        if (d.isComposite()) {
            for (org.apache.activemq.command.ActiveMQDestination c : d.getCompositeDestinations()) {
                if (isDataplaneTopic(c)) return true;
            }
            return false;
        }
        String n = d.getPhysicalName();
        return n.endsWith(".event") || n.contains(".event.") || n.equals("event") || n.endsWith(".>") || n.endsWith(".*") || n.equals(">");
    }
}
