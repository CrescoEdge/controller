package io.cresco.agent.controller.communication;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Batch signatures (Merkle trees) for MsgEvents past a relay. Signing a message with an RSA-3072 identity key costs about
 * 2 ms of one core, so a node that originates many signed messages at once -- the global relaying API calls to agents of
 * other regions, an agent answering them -- was held to ~500 a second per core. Here every message still carries its
 * origin's signature, verifiable on its own by its receiver, but the messages waiting to be signed at the same moment share
 * ONE private-key operation: each is a leaf ({@code SHA-384(0x00 || canonical form)}) of a Merkle tree
 * ({@code SHA-384(0x01 || left || right)}, an odd last node is carried up unchanged), the node signs
 * {@code "cresco-msg-batch-v1" || 0 || count || root}, and each message carries the signature and its path to the root
 * ({@value #TREE}: {@code <index>/<count>:<sibling>,<sibling>...}). A message alone in its batch -- whenever the node is not
 * busy -- gets an ordinary single signature: no latency is added. The receiver verifies the root signature once per batch
 * (cached by its digest), and each message's own path; its replay memory keys each message by signature and leaf.
 *
 * <p>Only a receiver that understands batch signatures is sent one: every signed message says so ({@code cresco_sig_v=2},
 * under its signature), and a node sends batch signatures only to a node it has verified such a message from.
 */
final class MsgBatchSigner {

    /** The message parameter of a batch signature's path (not covered by the signature: it is what leads to it). */
    static final String TREE = "cresco_sig_tree";
    private static final byte[] LABEL = "cresco-msg-batch-v1".getBytes(StandardCharsets.UTF_8);
    /** At most this many messages share one signature. */
    static final int MAX_BATCH = 256;

    interface PrivateSigner { byte[] sign(byte[] data) throws Exception; }

    /** A signature for one message: the base64 signature, and its tree path (null: an ordinary single signature). */
    static final class Signed {
        final String sig, tree;
        Signed(String sig, String tree) { this.sig = sig; this.tree = tree; }
    }

    private static final class Pending {
        final byte[] canonical;
        final CompletableFuture<Signed> done = new CompletableFuture<>();
        Pending(byte[] canonical) { this.canonical = canonical; }
    }

    private final LinkedBlockingQueue<Pending> queue = new LinkedBlockingQueue<>();
    private final PrivateSigner signer;
    private final List<Thread> workers = new ArrayList<>();
    private volatile boolean closed;

    MsgBatchSigner(PrivateSigner signer, int threads, String name) {
        this.signer = signer;
        for (int i = 0; i < Math.max(1, threads); i++) {
            Thread t = new Thread(this::run, name + "-" + i);
            t.setDaemon(true);
            workers.add(t);
            t.start();
        }
    }

    /** Sign {@code canonical} (the message's canonical form), sharing the key operation with what waits beside it. */
    Signed sign(byte[] canonical) throws Exception {
        if (closed) throw new IllegalStateException("the message signer is stopped");
        Pending p = new Pending(canonical);
        queue.add(p);
        if (closed) failWaiting();
        try {
            return p.done.get();
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable c = e.getCause();
            throw c instanceof Exception ? (Exception) c : new IllegalStateException(c);
        }
    }

    /** Stop the workers; whatever still waits fails (its message goes unsigned, as when its key cannot sign). */
    void close() {
        closed = true;
        for (Thread t : workers) t.interrupt();
        failWaiting();
    }

    private void failWaiting() {
        for (Pending p; (p = queue.poll()) != null; ) p.done.completeExceptionally(new IllegalStateException("the message signer is stopped"));
    }

    private void run() {
        List<Pending> batch = new ArrayList<>(MAX_BATCH);
        while (!closed) {
            batch.clear();
            try {
                batch.add(queue.take());
            } catch (InterruptedException ie) {
                return;
            }
            queue.drainTo(batch, MAX_BATCH - 1);
            try {
                if (batch.size() == 1) {
                    Pending only = batch.get(0);
                    only.done.complete(new Signed(Base64.getEncoder().encodeToString(signer.sign(only.canonical)), null));
                    continue;
                }
                int n = batch.size();
                byte[][] leaves = new byte[n][];
                for (int i = 0; i < n; i++) leaves[i] = leaf(batch.get(i).canonical);
                List<byte[][]> levels = levels(leaves);
                byte[] root = levels.get(levels.size() - 1)[0];
                String sig = Base64.getEncoder().encodeToString(signer.sign(signedBytes(root, n)));
                for (int i = 0; i < n; i++) batch.get(i).done.complete(new Signed(sig, path(levels, i, n)));
            } catch (Throwable t) {
                for (Pending p : batch) p.done.completeExceptionally(t);
            }
        }
    }

    // ------------------------------------------------------------------ the tree (shared with the receiver)

    static byte[] leaf(byte[] canonical) {
        MessageDigest md = sha384();
        md.update((byte) 0);
        return md.digest(canonical);
    }

    private static byte[] node(byte[] left, byte[] right) {
        MessageDigest md = sha384();
        md.update((byte) 1);
        md.update(left);
        return md.digest(right);
    }

    /** What the node signs for a batch: the label, a zero byte, the count and the root. */
    static byte[] signedBytes(byte[] root, int count) {
        return ByteBuffer.allocate(LABEL.length + 1 + 4 + root.length).put(LABEL).put((byte) 0).putInt(count).put(root).array();
    }

    /** Every level of the tree, the leaves first, the root last. */
    static List<byte[][]> levels(byte[][] leaves) {
        List<byte[][]> out = new ArrayList<>();
        out.add(leaves);
        byte[][] cur = leaves;
        while (cur.length > 1) {
            byte[][] up = new byte[(cur.length + 1) / 2][];
            for (int j = 0; j < cur.length; j += 2) up[j / 2] = j + 1 < cur.length ? node(cur[j], cur[j + 1]) : cur[j];
            out.add(up);
            cur = up;
        }
        return out;
    }

    /** Leaf {@code i}'s path: {@code <i>/<n>:} and the siblings it is combined with, bottom up (none where it is carried up). */
    static String path(List<byte[][]> levels, int i, int n) {
        StringBuilder b = new StringBuilder().append(i).append('/').append(n).append(':');
        int idx = i;
        boolean first = true;
        for (int l = 0; l < levels.size() - 1; l++) {
            byte[][] level = levels.get(l);
            int sib = idx ^ 1;
            if (sib < level.length) {
                if (!first) b.append(',');
                b.append(Base64.getEncoder().encodeToString(level[sib]));
                first = false;
            }
            idx >>= 1;
        }
        return b.toString();
    }

    /**
     * The root leaf {@code leaf} leads to along {@code path} ({@link #path}), or null when the path is malformed. The count
     * is returned in {@code count[0]}.
     */
    static byte[] root(byte[] leaf, String path, int[] count) {
        try {
            int slash = path.indexOf('/'), colon = path.indexOf(':');
            if (slash <= 0 || colon <= slash) return null;
            int i = Integer.parseInt(path.substring(0, slash)), n = Integer.parseInt(path.substring(slash + 1, colon));
            if (n < 2 || n > MAX_BATCH || i < 0 || i >= n) return null;
            String rest = path.substring(colon + 1);
            String[] sibs = rest.isEmpty() ? new String[0] : rest.split(",", -1);
            byte[] cur = leaf;
            int idx = i, len = n, used = 0;
            while (len > 1) {
                int sib = idx ^ 1;
                if (sib < len) {
                    if (used >= sibs.length) return null;
                    byte[] s = Base64.getDecoder().decode(sibs[used++]);
                    if (s.length != 48) return null;
                    cur = (idx & 1) == 0 ? node(cur, s) : node(s, cur);
                }
                idx >>= 1;
                len = (len + 1) / 2;
            }
            if (used != sibs.length) return null;
            count[0] = n;
            return cur;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static MessageDigest sha384() {
        try {
            return MessageDigest.getInstance("SHA-384");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
