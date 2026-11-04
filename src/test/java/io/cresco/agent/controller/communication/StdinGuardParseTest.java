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

    /**
     * Release-blocker regression (plugin round-4 review): ActiveMQ flattens AND chains, and the first fix refused every
     * {@code A AND B} selector as "not staying inside its parentheses" -- filerepo could not start. Every well-formed
     * selector is accepted, narrowed, and still selects what it selected (minus stdin).
     */
    @Test
    void everyWellFormedCompositionIsAcceptedAndNarrowed() throws Exception {
        String[] selectors = {
                // filerepo's own listeners (RepoEngine.createSubListener), exactly
                "filerepo_name='repo-1' AND broadcast",
                "filerepo_name='repo-1' AND region_id='rgA' AND agent_id='a1' AND plugin_id='plugin-7'",
                // typical wsapi / pycrescolib dataplane queries
                "stream_name='s1'",
                "stream_name='s1' AND type='event'",
                "region_id='rgA' AND agent_id='a1' AND (type='kpi' OR type='event')",
                "stream_name = 'gkt-1' AND cresco_to IS NULL",
                "NOT (type = 'heartbeat') AND stream_name LIKE 'logs-%'",
                // nested parentheses, mixed operators
                "((a = 1 AND b = 2) OR (c = 3 AND (d = 4 OR e = 5))) AND f IS NOT NULL",
                "(((x = 'y')))",
                "a = 1 OR b = 2 OR c = 3",
                "a BETWEEN 1 AND 5 AND b IN ('p', 'q') AND NOT c LIKE 'z%'",
        };
        for (String sel : selectors) {
            assertNull(StdinGuard.refusal(sel), "refused: " + sel);
            String n = StdinGuard.narrow(sel);
            assertTrue(n.startsWith("(" + sel) && n.endsWith(") AND " + StdinGuard.NO_STDIN), n);
            for (String t : new String[]{"input", "input-0a1b2c"}) assertFalse(matches(n, t), n + " matched " + t);
        }
        // what the filerepo selector selected, it still selects
        ActiveMQTextMessage m = new ActiveMQTextMessage();
        m.setStringProperty("filerepo_name", "repo-1");
        m.setBooleanProperty("broadcast", true);
        MessageEvaluationContext ctx = new NonCachedMessageEvaluationContext();
        ctx.setMessageReference(m);
        assertTrue(SelectorParser.parse(StdinGuard.narrow("filerepo_name='repo-1' AND broadcast")).matches(ctx));
        m.setStringProperty("type", "input-1");
        assertFalse(SelectorParser.parse(StdinGuard.narrow("filerepo_name='repo-1' AND broadcast")).matches(ctx), "but never stdin");
    }

    @Test
    void aCommentCannotSwallowTheExclusion() throws Exception {
        String sel = "x = '1' OR type = 'input-1' -- the rest is a comment";
        boolean parses;
        try { SelectorParser.parse(sel); parses = true; } catch (Exception e) { parses = false; }
        if (!parses) return;                                       // this selector engine has no comments: nothing to swallow
        String n = StdinGuard.narrow(sel);
        assertFalse(matches(n, "input-1"), n);
    }
}

