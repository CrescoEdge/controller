package io.cresco.agent.controller.communication;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** StdinGuard judges the parsed selector and always narrows (review Phase 3 F1). */
class StdinGuardParseTest {

    private static boolean matches(String selector, String type) throws Exception {
        ActiveMQTextMessage m = new ActiveMQTextMessage();
        if (type != null) m.setStringProperty("type", type);
        m.setStringProperty("x", "1");
        MessageEvaluationContext ctx = new NonCachedMessageEvaluationContext();
        ctx.setMessageReference(m);
        return SelectorParser.parse(selector).matches(ctx);
    }

    @Test
    void aSelectorThatLeavesItsParenthesesIsRefused() {
        String escape = "x = '1') OR (type = 'input-1'";
        assertNotNull(StdinGuard.refusal(escape), "unbalanced on its own: refused");
        assertThrows(IllegalArgumentException.class, () -> StdinGuard.narrow(escape));
        assertNotNull(StdinGuard.refusal("type = "), "malformed");
    }

    @Test
    void narrowingIsUnconditional() throws Exception {
        for (String s : new String[]{StdinGuard.NO_STDIN, StdinGuard.NO_STDIN + " OR x = '1'", "x = '1' OR " + StdinGuard.NO_STDIN,
                "(" + StdinGuard.NO_STDIN + ") OR type IS NOT NULL", "x = '1'"}) {
            String n = StdinGuard.narrow(s);
            assertEquals("(" + s + ") AND " + StdinGuard.NO_STDIN, n);
            for (String t : new String[]{"input", "input-0", "input-0a1b2c"}) assertFalse(matches(n, t), n + " matched " + t);
            assertTrue(matches(n, "event") || !matches(s, "event"), "an ordinary message still passes what it passed: " + s);
        }
    }

    @Test
    void refusalReadsTheParsedSelector() {
        assertNotNull(StdinGuard.refusal("type LIKE '_nput%'"), "a pattern that matches stdin names it");
        assertNotNull(StdinGuard.refusal("NOT (NOT (type = 'input'))"), "double negation");
        assertNull(StdinGuard.refusal("NOT (type = 'input')"), "an exclusion");
        assertNull(StdinGuard.refusal("type <> 'event'"), "does not single stdin out (narrowed instead)");
        assertNotNull(StdinGuard.refusal("type IN ('input-7')"));
        assertNull(StdinGuard.refusal("type = 'inputs'"));
        assertNull(StdinGuard.refusal("x = 'input-1'"), "another property");
    }
}
