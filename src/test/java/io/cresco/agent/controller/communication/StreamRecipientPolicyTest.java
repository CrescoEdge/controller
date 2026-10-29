package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Who may receive, and who may subscribe to, a recipient-bound dataplane stream. */
class StreamRecipientPolicyTest {

    private static final CrescoIdentity A1 = CrescoIdentity.of("t1", "rgA", "a1", null);
    private static final CrescoIdentity A2 = CrescoIdentity.of("t1", "rgA", "a2", null);

    @Test
    void onlyTheRecipientReceives() {
        assertTrue(StreamRecipientPolicy.mayReceive(A2, null, null).allowed, "unbound messages are unaffected");
        assertTrue(StreamRecipientPolicy.mayReceive(A1, "rgA_a1", null).allowed);
        assertFalse(StreamRecipientPolicy.mayReceive(A2, "rgA_a1", null).allowed);
        assertFalse(StreamRecipientPolicy.mayReceive(null, "rgA_a1", null).allowed);
        assertTrue(StreamRecipientPolicy.mayReceive(A1, "rgA_a1", "t1").allowed);
        assertFalse(StreamRecipientPolicy.mayReceive(A1, "rgA_a1", "t2").allowed);
    }

    @Test
    void aSelectorAskingForAnotherRecipientIsRefused() {
        assertTrue(StreamRecipientPolicy.maySubscribe(A2, null).allowed);
        assertTrue(StreamRecipientPolicy.maySubscribe(A2, "stream_name='x' AND type='input'").allowed);
        assertTrue(StreamRecipientPolicy.maySubscribe(A1, "cresco_to='rgA_a1'").allowed);
        assertFalse(StreamRecipientPolicy.maySubscribe(A2, "cresco_to='rgA_a1'").allowed);
        assertFalse(StreamRecipientPolicy.maySubscribe(A2, "stream_name='x' AND cresco_to = 'rgA_a1'").allowed);
        assertFalse(StreamRecipientPolicy.maySubscribe(A2, "'rgA_a1'=cresco_to").allowed);
        assertFalse(StreamRecipientPolicy.maySubscribe(A2, "CRESCO_TO IN ('rgA_a2', 'rgA_a1')").allowed);
        assertTrue(StreamRecipientPolicy.maySubscribe(A2, "cresco_to IN ('rgA_a2')").allowed);
        assertFalse(StreamRecipientPolicy.maySubscribe(null, "cresco_to='rgA_a2'").allowed);
        // a property that merely ends in cresco_to is not the recipient property
        assertTrue(StreamRecipientPolicy.maySubscribe(A2, "x_cresco_to='rgA_a1'").allowed);
        assertEquals(List.of("it's"), StreamRecipientPolicy.recipientsNamedIn("cresco_to='it''s'"));
    }
}
