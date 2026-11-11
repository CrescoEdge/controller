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
            assertTrue(hold.hold(msg("a"), 40, now, "rgA/a1"));
            assertTrue(hold.hold(msg("b"), 40, now, "rgA/a2"));
            assertFalse(hold.hold(msg("c"), 40, now, "rgA/a3"), "past its size");
            assertTrue(hold.hold(msg("d"), 10, now, "rgA/a3"));
            assertFalse(hold.hold(msg("e"), 1, now, "rgA/a4"), "past its count");
            assertEquals(3, hold.size());
            assertEquals(2, hold.notHeld.get());
            hold.close();
            assertFalse(hold.hold(msg("f"), 1, now), "held after close");
            assertEquals(0, hold.size());
        } finally {
            hold.close();
        }
    }

    @Test
    void oneSenderTakesAnEighthOfTheHoldAndNoMore() throws Exception {
        // what is held is not verified yet: one sender must not be able to take the room of every other
        TrustHold hold = new TrustHold(60_000L, 64, 1 << 20, m -> { });
        try {
            long now = System.currentTimeMillis();
            int own = 64 / TrustHold.SENDER_DIVISOR;
            for (int i = 0; i < own; i++) assertTrue(hold.hold(msg("x" + i), 1, now, "rgB/flood"), "message " + i + " of one sender");
            assertFalse(hold.hold(msg("x"), 1, now, "rgB/flood"), "one sender holds more than its share");
            assertFalse(hold.hold(msg("x"), 1, now, "rgB/flood"));
            for (int i = 0; i < own; i++) assertTrue(hold.hold(msg("y" + i), 1, now, "rgA/a" + i), "another sender found no room");
            for (int i = 0; i < own; i++) assertTrue(hold.hold(msg("z" + i), 1, now, null), "a message whose sender is not known");
            assertFalse(hold.hold(msg("z"), 1, now, null), "the unknown senders together hold more than one sender's share");
            assertEquals(3 * own, hold.size());
        } finally {
            hold.close();
        }
    }

    @Test
    void aSendersShareIsFreeAgainOnceItsMessagesWentBack() throws Exception {
        List<Message> back = new CopyOnWriteArrayList<>();
        TrustHold hold = new TrustHold(60_000L, 16, 1 << 20, back::add);
        try {
            long now = System.currentTimeMillis();
            assertTrue(hold.hold(msg("a"), 1, now, "rgA/a1"));
            assertTrue(hold.hold(msg("b"), 1, now, "rgA/a1"));
            assertFalse(hold.hold(msg("c"), 1, now, "rgA/a1"));
            hold.trustChanged();
            await(() -> back.size() == 2, 3_000);
            assertEquals(2, back.size());
            assertTrue(hold.hold(msg("d"), 1, System.currentTimeMillis(), "rgA/a1"), "its share was not given back");
        } finally {
            hold.close();
        }
    }

    @Test
    void aChangeBetweenTheVerdictAndTheHoldIsNotMissed() throws Exception {
        // the inbox judged the message (not yet), the trust data changed, a tick took the change -- and only then was the
        // message held: it waited out its whole time although the data it needed was in
        List<Message> back = new CopyOnWriteArrayList<>();
        TrustHold hold = new TrustHold(30_000L, 16, 1 << 20, back::add);
        try {
            assertTrue(hold.hold(msg("first"), 1, System.currentTimeMillis(), "rgA/a1"));   // starts the timer
            long judgedAt = System.currentTimeMillis();
            Thread.sleep(5);
            hold.trustChanged();
            await(() -> back.size() == 1, 3_000);                                            // the tick took the change
            assertEquals(1, back.size());
            Message late = msg("late");
            assertTrue(hold.hold(late, 1, judgedAt, "rgA/a2"));                              // judged before the change, held after
            await(() -> back.contains(late), 3_000);
            assertTrue(back.contains(late), "a message judged before a change of the trust data and held after it waits its whole time");
        } finally {
            hold.close();
        }
    }

    @Test
    void aRedeliveryThatFailsEndsNeitherTheOthersNorTheTimer() throws Exception {
        List<Message> back = new CopyOnWriteArrayList<>();
        TrustHold hold = new TrustHold(60_000L, 16, 1 << 20, m -> {
            back.add(m);
            if (back.size() == 1) throw new AssertionError("an Error out of the inbox");
            if (back.size() == 2) throw new IllegalStateException("an exception out of the inbox");
        });
        try {
            long now = System.currentTimeMillis();
            for (int i = 0; i < 3; i++) assertTrue(hold.hold(msg("m" + i), 1, now, "rgA/a" + i));
            hold.trustChanged();
            await(() -> back.size() == 3, 3_000);
            assertEquals(3, back.size(), "one failed redelivery kept the others from going back");
            assertTrue(hold.hold(msg("after"), 1, System.currentTimeMillis(), "rgA/a9"));
            hold.trustChanged();
            await(() -> back.size() == 4, 3_000);
            assertEquals(4, back.size(), "the hold's timer ended with a failed redelivery: nothing held is ever judged again");
        } finally {
            hold.close();
        }
    }
}
