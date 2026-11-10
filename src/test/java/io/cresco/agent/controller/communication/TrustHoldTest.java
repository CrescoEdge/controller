package io.cresco.agent.controller.communication;

import jakarta.jms.Message;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an inbox cannot judge yet is held and handed back -- when the trust data changes, and for the last time once it
 * has been held its full time -- within bounds. It was refused at once: every message that reached a node while its
 * trust data was arriving was lost.
 */
class TrustHoldTest {

    private static Message msg(String text) throws Exception {
        ActiveMQTextMessage m = new ActiveMQTextMessage();
        m.setText(text);
        return m;
    }

    private static void await(java.util.function.BooleanSupplier cond, long ms) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < end) Thread.sleep(5);
    }

    @Test
    void aHeldMessageComesBackWhenTheTrustDataChanges() throws Exception {
        List<Message> back = new CopyOnWriteArrayList<>();
        List<Boolean> mayHoldAgain = new CopyOnWriteArrayList<>();
        TrustHold[] self = new TrustHold[1];
        TrustHold hold = self[0] = new TrustHold(5_000L, 16, 1 << 20, m -> {
            mayHoldAgain.add(self[0].mayHold(System.currentTimeMillis()));
            back.add(m);
        });
        try {
            Message m = msg("one");
            assertNull(hold.since(), "a message off the wire is no redelivery");
            assertTrue(hold.hold(m, 3, System.currentTimeMillis()));
            Thread.sleep(400);
            assertTrue(back.isEmpty(), "handed back although nothing changed and its time is not over");
            assertEquals(1, hold.size());
            hold.trustChanged();
            await(() -> !back.isEmpty(), 3_000);
            assertEquals(List.of(m), back);
            assertEquals(List.of(true), mayHoldAgain, "within its time a message handed back may be held again");
            assertEquals(0, hold.size());
        } finally {
            hold.close();
        }
    }

    @Test
    void aMessageIsHeldItsTimeAndThenHandedBackForTheLastTime() throws Exception {
        List<Message> back = new CopyOnWriteArrayList<>();
        List<Boolean> mayHoldAgain = new CopyOnWriteArrayList<>();
        TrustHold[] self = new TrustHold[1];
        TrustHold hold = self[0] = new TrustHold(300L, 16, 1 << 20, m -> {
            long now = System.currentTimeMillis();
            mayHoldAgain.add(self[0].mayHold(now));
            if (self[0].mayHold(now)) self[0].hold(m, 3, now);         // as an inbox that still cannot judge it
            else back.add(m);
        });
        try {
            long t0 = System.currentTimeMillis();
            Message m = msg("one");
            assertTrue(hold.hold(m, 3, t0));
            hold.trustChanged();                                         // tried again at once: still held, its time counted from t0
            await(() -> !back.isEmpty(), 3_000);
            long took = System.currentTimeMillis() - t0;
            assertEquals(List.of(m), back);
            assertTrue(took >= 300 && took < 2_000, "held " + took + " ms for a hold of 300 ms");
            assertFalse(mayHoldAgain.get(mayHoldAgain.size() - 1), "after its time a message may not be held again");
            assertEquals(1, hold.heldTotal.get(), "one message, held once (a re-hold is the same message)");
        } finally {
            hold.close();
        }
    }

    @Test
    void theHoldIsBoundedInMessagesAndInSize() throws Exception {
        TrustHold hold = new TrustHold(5_000L, 3, 100, m -> { });
        try {
            long now = System.currentTimeMillis();
            assertTrue(hold.hold(msg("a"), 40, now));
            assertTrue(hold.hold(msg("b"), 40, now));
            assertFalse(hold.hold(msg("c"), 40, now), "past its size");
            assertTrue(hold.hold(msg("d"), 10, now));
            assertFalse(hold.hold(msg("e"), 1, now), "past its count");
            assertEquals(3, hold.size());
            assertEquals(2, hold.notHeld.get());
            hold.close();
            assertFalse(hold.hold(msg("f"), 1, now), "held after close");
            assertEquals(0, hold.size());
        } finally {
            hold.close();
        }
    }
}
