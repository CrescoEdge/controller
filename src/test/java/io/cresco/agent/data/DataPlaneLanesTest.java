package io.cresco.agent.data;

import io.cresco.library.data.PacedListener;
import jakarta.jms.MessageListener;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Dataplane backpressure plumbing: lanes send in the sending thread in-JVM; paced listeners are recognised wrapped. */
class DataPlaneLanesTest {

    @Test
    void anInJvmLaneRunsTheBrokersWorkInTheSendingThread() {
        assertEquals("vm://localhost?async=false", DataPlaneServiceImpl.laneUri("vm://localhost"));
        assertEquals("vm://b?create=false&async=false", DataPlaneServiceImpl.laneUri("vm://b?create=false"));
        assertEquals("vm://b?async=true", DataPlaneServiceImpl.laneUri("vm://b?async=true"), "an explicit choice is kept");
        String net = "failover:(nio+ssl://10.0.0.1:32010)?maxReconnectAttempts=5";
        assertEquals(net, DataPlaneServiceImpl.laneUri(net), "a network lane is a TCP connection of its own already");
    }

    @Test
    void aPacedListenerIsRecognisedThroughItsGapDetector() {
        PacedListener paced = m -> { };
        MessageListener plain = m -> { };
        assertTrue(DataPlaneServiceImpl.paced(paced));
        assertTrue(DataPlaneServiceImpl.paced(StreamSequence.detecting(paced)), "addMessageListener wraps it before it is placed");
        assertFalse(DataPlaneServiceImpl.paced(plain));
        assertFalse(DataPlaneServiceImpl.paced(StreamSequence.detecting(plain)));
    }
}
