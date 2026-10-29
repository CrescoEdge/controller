package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Who may receive a recipient-bound dataplane message. Pure (no ActiveMQ), so it can be tested exhaustively;
 * {@link CrescoAuthorizationBroker} applies it on every subscription and every dispatch.
 *
 * <p><b>Binding a stream to its recipient.</b> Dataplane streams share the agent/region/global topics and are told
 * apart by message properties and consumer selectors. A sender binds a message to ONE recipient by setting the
 * string property {@value #TO} to the recipient agent's path {@code <region>_<agent>} (both names in the
 * {@link CrescoNames} grammar, which has no {@code _}, so the path is unambiguous; and optionally
 * {@value #TO_TENANT} to its tenant). The broker then delivers that message only to:
 * <ul>
 *   <li>infrastructure connections: the broker's own controller over {@code vm://}, and broker-to-broker bridges
 *       from verified controllers (the broker at the next hop applies the same rule to its own consumers);</li>
 *   <li>a client connection whose identity (certificate subject under mutual TLS, else the asserted
 *       {@code tenant|region|agent} username) is that agent (and that tenant, when {@value #TO_TENANT} is set).</li>
 * </ul>
 * Anyone else's subscription simply does not receive it, whatever its selector says; a subscription whose selector
 * explicitly asks for another identity's stream ({@code cresco_to='<someone else>'}) is refused when it is made.
 * Messages without {@value #TO} are not affected.
 *
 * <p>The recipient is an agent (a broker connection), not a plugin inside it: plugins that share an agent share its
 * identity. Content confidentiality per plugin is end-to-end encryption's job (GKT).
 */
public final class StreamRecipientPolicy {

    /** Message property: the recipient agent path {@code <region>_<agent>}. */
    public static final String TO = "cresco_to";
    /** Optional message property: the recipient's tenant. */
    public static final String TO_TENANT = "cresco_to_tenant";

    private StreamRecipientPolicy() { }

    // cresco_to = 'x'   |   'x' = cresco_to   |   cresco_to IN ('x', 'y')   (JMS string literals double '' to escape)
    private static final String LIT = "'((?:[^']|'')*)'";
    private static final Pattern EQ = Pattern.compile("(?i)(?<![\\w.])" + TO + "\\s*=\\s*" + LIT);
    private static final Pattern EQ_REV = Pattern.compile("(?i)" + LIT + "\\s*=\\s*" + TO + "(?![\\w.])");
    private static final Pattern IN = Pattern.compile("(?i)(?<![\\w.])" + TO + "\\s+IN\\s*\\(([^)]*)\\)");
    private static final Pattern ANY_LIT = Pattern.compile(LIT);

    /** May {@code consumer} receive a message bound to {@code to} (tenant {@code toTenant}, may be null)? */
    public static TenantPolicy.Decision mayReceive(CrescoIdentity consumer, String to, String toTenant) {
        if (to == null) return TenantPolicy.Decision.allow("not recipient-bound");
        String[] rcpt = CrescoNames.splitPath(to);
        if (rcpt == null) return TenantPolicy.Decision.deny("recipient-bound to '" + to + "', which is not a valid <region>_<agent>");
        if (consumer == null || !CrescoNames.isValid(consumer.getRegion()) || !CrescoNames.isValid(consumer.getAgent())) {
            return TenantPolicy.Decision.deny("recipient-bound to " + to + "; the consumer has no valid identity");
        }
        if (!rcpt[0].equals(consumer.getRegion()) || !rcpt[1].equals(consumer.getAgent())) {
            return TenantPolicy.Decision.deny("recipient-bound to " + to + ", not " + consumer.getAgentPath());
        }
        if (toTenant != null && !toTenant.isEmpty() && !toTenant.equals(consumer.getTenant())) {
            return TenantPolicy.Decision.deny("recipient-bound to tenant " + toTenant + ", not " + consumer.getTenant());
        }
        return TenantPolicy.Decision.allow("intended recipient");
    }

    /**
     * May {@code consumer} subscribe with {@code selector}? Refused only when the selector names, as {@value #TO},
     * an identity other than the consumer's own: that subscription exists to read someone else's stream.
     */
    public static TenantPolicy.Decision maySubscribe(CrescoIdentity consumer, String selector) {
        List<String> named = recipientsNamedIn(selector);
        if (named.isEmpty()) return TenantPolicy.Decision.allow("selector names no recipient");
        String own = (consumer == null) ? null : CrescoNames.path(consumer.getRegion(), consumer.getAgent());
        for (String r : named) {
            if (own == null || CrescoNames.splitPath(r) == null || !own.equals(r)) {
                return TenantPolicy.Decision.deny("selector asks for the stream of recipient " + r
                        + (own == null ? " but the subscriber has no identity" : (", subscriber is " + own)));
            }
        }
        return TenantPolicy.Decision.allow("selector names only the subscriber");
    }

    /** The recipient identities a selector pins with {@value #TO} = '...' or {@value #TO} IN (...). */
    static List<String> recipientsNamedIn(String selector) {
        List<String> out = new ArrayList<>();
        if (selector == null || selector.indexOf(TO) < 0 && selector.toLowerCase().indexOf(TO) < 0) return out;
        Matcher m = EQ.matcher(selector);
        while (m.find()) out.add(unquote(m.group(1)));
        m = EQ_REV.matcher(selector);
        while (m.find()) out.add(unquote(m.group(1)));
        m = IN.matcher(selector);
        while (m.find()) {
            Matcher lit = ANY_LIT.matcher(m.group(1));
            while (lit.find()) out.add(unquote(lit.group(1)));
        }
        return out;
    }

    private static String unquote(String s) {
        return s.replace("''", "'");
    }
}
