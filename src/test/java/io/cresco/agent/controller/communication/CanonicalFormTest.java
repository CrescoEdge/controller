package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The bytes a signature or MAC covers never change: every node of every build must compute the same canonical form. The
 * builder is held to the reference implementation (a sorted map written through a DataOutputStream) over messages with
 * covered and uncovered parameters, empty and null values, and multi-byte text.
 */
class CanonicalFormTest {

    static byte[] reference(MsgEvent m) throws Exception {
        ByteArrayOutputStream bo = new ByteArrayOutputStream(512);
        DataOutputStream o = new DataOutputStream(bo);
        str(o, "cresco-msg-v2");
        str(o, m.getMsgType() == null ? null : m.getMsgType().name());
        str(o, m.getSrcRegion());
        str(o, m.getSrcAgent());
        str(o, m.getSrcPlugin());
        TreeMap<String, String> p = new TreeMap<>();
        if (m.getParams() != null) {
            for (Map.Entry<String, String> e : m.getParams().entrySet()) {
                if (e.getValue() != null && MsgSigning.covered(e.getKey())) p.put(e.getKey(), e.getValue());
            }
        }
        o.writeInt(p.size());
        for (Map.Entry<String, String> e : p.entrySet()) {
            str(o, e.getKey());
            str(o, e.getValue());
        }
        o.flush();
        return bo.toByteArray();
    }

    private static void str(DataOutputStream o, String s) throws Exception {
        if (s == null) { o.writeInt(-1); return; }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        o.writeInt(b.length);
        o.write(b);
    }

    @Test
    void theCanonicalFormIsTheReferenceBytes() throws Exception {
        Random r = new Random(42);
        String[] keys = {"action", "is_rpc", "cresco_sig_by", "cresco_sig_ts", "cresco_sig_to", "cresco_sig", "routepath-1", "linkcost-x",
                "srcroute", "ttl", "cresco_caller", "cresco_tenant", "payload", "Zeta", "alpha", "aB", "ab", "é-param", "k", "callId-r-a-p"};
        for (int i = 0; i < 2000; i++) {
            MsgEvent m = new MsgEvent(r.nextBoolean() ? MsgEvent.Type.EXEC : MsgEvent.Type.CONFIG, "r" + r.nextInt(3), "a" + r.nextInt(3),
                    r.nextBoolean() ? null : "plugin/" + r.nextInt(9), "rd", "ad", null, r.nextBoolean(), false);
            int n = r.nextInt(keys.length + 1);
            for (int j = 0; j < n; j++) {
                String k = keys[r.nextInt(keys.length)] + (r.nextInt(4) == 0 ? "_" + r.nextInt(100) : "");
                String v = r.nextInt(10) == 0 ? "" : (r.nextInt(5) == 0 ? "ünïcødé ✓ " + r.nextLong() : Long.toString(r.nextLong()));
                m.setParam(k, v);
            }
            assertArrayEquals(reference(m), MsgSigning.canonical(m), "message " + i + ": " + m.getParams().keySet());
        }
        MsgEvent bare = new MsgEvent();
        assertArrayEquals(reference(bare), MsgSigning.canonical(bare));
    }
}
