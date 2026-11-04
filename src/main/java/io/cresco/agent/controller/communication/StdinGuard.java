package io.cresco.agent.controller.communication;

import java.util.regex.Pattern;

/**
 * No dataplane subscriber but a process's own executor sees that process's stdin (dataplane messages with
 * {@code type} {@code input} or {@code input-<id>}). Defence in depth: the executor takes stdin through its
 * {@code write_stdin} action now, but an older writer may still put stdin on the dataplane.
 *
 * <ul>
 *   <li>A selector that names stdin ({@code 'input'}, {@code 'input-...'}, {@code 'input%'}) is refused;</li>
 *   <li>any other selector is narrowed with {@link #NO_STDIN}, so it can never match a stdin message.</li>
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

    private static final Pattern NAMES_STDIN = Pattern.compile("'input(-|'|%|_)", Pattern.CASE_INSENSITIVE);
    /** Mentions that EXCLUDE stdin (a selector that keeps stdin out, like {@link #NO_STDIN}, is not refused). */
    private static final Pattern EXCLUDES = Pattern.compile(
            "<>\\s*'[^']*'|NOT\\s+LIKE\\s+'[^']*'(\\s+ESCAPE\\s+'[^']*')?|NOT\\s+IN\\s*\\([^)]*\\)", Pattern.CASE_INSENSITIVE);

    /** Why {@code selector} is refused (it asks for stdin: names it other than to exclude it), or null. */
    public static String refusal(String selector) {
        if (selector != null && NAMES_STDIN.matcher(EXCLUDES.matcher(selector).replaceAll(" ")).find()) {
            return "refused: the selector asks for a process's stdin (type 'input' or 'input-*'); stdin never travels on the "
                    + "dataplane to any subscriber but the executor (write it with the executor's write_stdin action). A selector "
                    + "that only excludes it (type <> 'input', NOT LIKE 'input-%') is accepted";
        }
        return null;
    }

    /** {@code selector} narrowed so it never matches stdin. */
    public static String narrow(String selector) {
        if (selector == null || selector.trim().isEmpty()) return NO_STDIN;
        if (selector.contains(NO_STDIN)) return selector;
        return "(" + selector + ") AND " + NO_STDIN;
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
