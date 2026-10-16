package io.cresco.agent.controller.statemachine;

import io.cresco.library.agent.ControllerMode;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.crypto.CryptoPolicy;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

/**
 * Carries the fabric-wide {@link CryptoPolicy} from the global controller to every region and agent.
 *
 * <p><b>Authority.</b> A controller in a GLOBAL mode or STANDALONE is the top of its fabric: it builds the policy
 * from its own config ({@code crypto_policy}, {@code crypto_allow_*}) and installs it. Every other node (region,
 * agent) takes its parent's policy and ignores those keys in its own config (it logs that it does), so no node runs
 * a weaker local override than the global's. A global whose policy config is invalid installs nothing: every
 * algorithm choice for a new object then refuses fabric-wide (fail closed) until the config is fixed.
 *
 * <p><b>Distribution.</b> The parent stamps the canonical policy on the replies it already sends: the
 * {@code region_enable} / {@code agent_enable} registration replies (always) and its pongs. A child's ping carries
 * the id of the policy it holds ({@link #ID_PARAM}); the pong carries the full policy ({@link #PARAM}) only when
 * that id differs, so the steady state adds one short id each way. A child adopts a policy it does not hold yet,
 * so a joining or reconnecting node gets the policy on registration and a change at the global reaches every node
 * within one ping interval per tier after the parent has it. A node holds no policy until it first hears from its
 * parent ({@link CryptoPolicy#requireForNew} refuses until then).
 */
public final class CryptoPolicySync {

    /** The canonical policy text, on registration replies and on a pong whose ping carried another id. */
    public static final String PARAM = "crypto_policy";
    /** The id of the policy the sender holds, on pings and pongs. */
    public static final String ID_PARAM = "crypto_policy_id";

    private static final java.util.concurrent.atomic.AtomicBoolean WARNED = new java.util.concurrent.atomic.AtomicBoolean();

    private CryptoPolicySync() {}

    /**
     * True for the modes in which a controller is the top of its fabric and its own config sets the policy. Not
     * STANDALONE_INIT: every node passes through it at start (ControllerSMHandler.stateInit), agents included, and an
     * agent must never hold its own local policy, not even until it registers.
     */
    public static boolean isAuthority(ControllerMode mode) {
        return mode != null && (mode.name().startsWith("GLOBAL") || mode == ControllerMode.STANDALONE);
    }

    /**
     * Global or standalone controller: build the policy from this node's config and install it.
     *
     * @return the installed policy, or null when the config is invalid (nothing installed: fail closed)
     */
    public static CryptoPolicy becomeAuthority(PluginBuilder plugin, CLogger logger, String who) {
        CryptoPolicy p;
        try {
            p = CryptoPolicy.fromConfig(k -> plugin.getConfig().getStringParam(k));
        } catch (IllegalArgumentException bad) {
            CryptoPolicy.uninstall();
            logger.error("crypto policy: the config of this global controller is invalid (" + bad.getMessage()
                    + "); NO policy is in force, so every algorithm choice for a new object refuses fabric-wide until it is fixed");
            return null;
        }
        if (CryptoPolicy.install(p, "config of the global controller " + who)) {
            logger.info("crypto policy " + p.describe() + " in force for this fabric (config of " + who + "): " + p.canonical());
        }
        return p;
    }

    /** Region or agent: its own crypto policy keys have no effect; say so (once per process). */
    public static void warnIfLocalPolicyIgnored(PluginBuilder plugin, CLogger logger) {
        if (CryptoPolicy.configSetsPolicy(k -> plugin.getConfig().getStringParam(k)) && WARNED.compareAndSet(false, true)) {
            logger.warn("crypto policy keys (" + CryptoPolicy.CONFIG_LEVEL + ", " + CryptoPolicy.CONFIG_ALLOW_PREFIX
                    + "*) in this node's config are IGNORED: only the global controller sets the fabric's crypto policy");
        }
    }

    /** Child side: put the id of the policy this node holds on a ping to its parent. */
    public static void advertise(MsgEvent ping) {
        CryptoPolicy p = CryptoPolicy.current();
        if (p != null) {
            ping.setParam(ID_PARAM, p.id());
        }
    }

    /**
     * Parent side: stamp a reply with this node's policy. {@code childId} is the id the request carried (read it
     * before the request is turned into the reply); the full policy goes only when it differs. Nothing is stamped
     * while this node holds no policy.
     */
    public static void stamp(MsgEvent reply, String childId) {
        CryptoPolicy p = CryptoPolicy.current();
        if (p == null) {
            return;
        }
        reply.setParam(ID_PARAM, p.id());
        if (!p.id().equals(childId)) {
            reply.setParam(PARAM, p.canonical());
        } else {
            reply.removeParam(PARAM);
        }
    }

    /**
     * Child side: adopt the policy a parent's reply carries. An authority (global/standalone) node never adopts;
     * a reply without a policy changes nothing; an unparsable one is logged and ignored (the node keeps what it holds).
     *
     * @return true when this node's policy changed
     */
    public static boolean adopt(MsgEvent reply, ControllerState cstate, String from, CLogger logger) {
        if (reply == null || cstate == null || isAuthority(cstate.getControllerState())) {
            return false;
        }
        String text = reply.getParam(PARAM);
        if (text == null) {
            return false;
        }
        CryptoPolicy p;
        try {
            p = CryptoPolicy.parse(text);
        } catch (IllegalArgumentException bad) {
            logger.error("crypto policy from " + from + " is unusable (" + bad.getMessage() + "); keeping "
                    + (CryptoPolicy.current() == null ? "none (algorithm choices for new objects stay refused)" : CryptoPolicy.current().describe()));
            return false;
        }
        CryptoPolicy before = CryptoPolicy.current();
        if (!CryptoPolicy.install(p, from)) {
            return false;
        }
        logger.info("crypto policy " + p.describe() + " adopted from " + from + " (was "
                + (before == null ? "none" : before.describe()) + ")");
        return true;
    }
}
