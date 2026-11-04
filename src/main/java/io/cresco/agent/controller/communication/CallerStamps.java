package io.cresco.agent.controller.communication;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;

import jakarta.jms.JMSException;
import jakarta.jms.Message;

/**
 * Where the verified caller ({@link Caller}) is written and read, on every delivery path (see
 * {@code docs/caller-authorization.md} sections 1-2):
 *
 * <ul>
 *   <li><b>In-JVM</b>: the controller strips whatever a sender wrote into the three reserved parameters and stamps the
 *       sender it knows -- itself ({@code controller:...}, {@code infra}), the calling plugin ({@code plugin:...},
 *       {@code plugin}), or an API user a listed bundle sends for ({@code api:...}).</li>
 *   <li><b>On the wire</b>: a sender copies its stamp into JMS string properties of the same names ({@link #toJms}); a
 *       message with no stamp carries an explicitly empty {@code cresco_caller}. The broker checks what a connection
 *       declares against the connection's own verified identity ({@link #acceptDeclared}). The receiver never reads the
 *       stamp from the message body: it strips the body values and takes the properties ({@link #fromJms}).</li>
 * </ul>
 *
 * <p>With {@code broker_security_enabled=false} (explicit insecure development mode) every caller is {@code anonymous},
 * with every role.
 */
public final class CallerStamps {

    private CallerStamps() { }

    /** JMS property names: the same as the reserved MsgEvent parameters. */
    public static final String P_CALLER = Caller.PARAM_CALLER, P_TENANT = Caller.PARAM_TENANT, P_ROLES = Caller.PARAM_ROLES;

    /** Strip whatever {@code msg} carries in the reserved parameters and stamp {@code caller} (null: leave it unstamped). */
    public static void stamp(MsgEvent msg, Caller caller) {
        stamp(msg, caller, null);
    }

    /**
     * As {@link #stamp(MsgEvent, Caller)}, with {@code regionAlias}: the former region this (node) caller answers at
     * ({@link Caller#PARAM_ALIAS}); null for none.
     */
    public static void stamp(MsgEvent msg, Caller caller, String regionAlias) {
        if (msg == null) return;
        if (msg.getParams() == null) msg.setParams(new java.util.HashMap<>());
        Caller.strip(msg);
        if (caller != null) {
            caller.stamp(msg);
            if (regionAlias != null) caller.stampRegionAlias(msg, regionAlias);
        }
    }

    /** JMS property: the former region of the caller's node ({@link Caller#PARAM_ALIAS}). */
    public static final String P_ALIAS = Caller.PARAM_ALIAS;

    /** JMS properties: the API user a bundle in {@code on_behalf_bundles} (wsapi) writes a dataplane message for. */
    public static final String P_WRITER = "cresco_writer", P_WRITER_ROLES = "cresco_writer_roles";

    /** The API user {@code jms} names as its writer (a well-formed {@code api:} caller), or null. */
    public static Caller userWriter(Message jms) {
        try {
            String p = jms.getStringProperty(P_WRITER);
            String roles = jms.getStringProperty(P_WRITER_ROLES);
            if (p == null || roles == null || !p.startsWith("api:")) return null;
            int slash = p.indexOf('/');
            if (slash < 0) return null;
            Caller c = Caller.parse(p, p.substring(4, slash), roles);
            return c != null && c.getKind() == Caller.Kind.API ? c : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Keep the API user a dataplane message names as its writer ({@link #P_WRITER}) only when {@code allowed} (the sending
     * bundle is in {@code on_behalf_bundles}) and the user may be sent for by a node of {@code nodeTenant} (the on-behalf
     * rule: its own tenant's users, any tenant's from a superuser tenant); otherwise remove it.
     */
    public static void filterUserWriter(Message jms, boolean allowed, String nodeTenant, java.util.Set<String> superuserTenants)
            throws JMSException {
        Caller w = allowed ? userWriter(jms) : null;
        boolean keep = w != null && (w.getTenant().equals(nodeTenant) || superuserTenants.contains(nodeTenant))
                && (w.getRoles().contains(Caller.ROLE_ADMIN) || w.getRoles().contains(Caller.ROLE_USER));
        if (keep) return;
        boolean had;
        try {
            had = jms.propertyExists(P_WRITER) || jms.propertyExists(P_WRITER_ROLES);
        } catch (JMSException e) {
            had = true;
        }
        if (!had) return;
        if (jms instanceof org.apache.activemq.command.ActiveMQMessage) {
            org.apache.activemq.command.ActiveMQMessage am = (org.apache.activemq.command.ActiveMQMessage) jms;
            try {
                am.removeProperty(P_WRITER);
                am.removeProperty(P_WRITER_ROLES);
                return;
            } catch (java.io.IOException e) {
                throw new JMSException("cannot remove the writer properties: " + e.getMessage());
            }
        }
        jms.setStringProperty(P_WRITER, "");
        jms.setStringProperty(P_WRITER_ROLES, "");
    }

    /**
     * Clear the file list of {@code msg}. A file list names files on the disk of the node that sends the message; one
     * that arrived in a message body, or from an API user, would make this node read and ship its own files.
     */
    public static void dropFiles(MsgEvent msg) {
        try {
            if ((msg.getFileList() != null && !msg.getFileList().isEmpty()) || msg.hasFiles()) msg.clearFileList();
        } catch (RuntimeException noMsgParams) {
            // a body without the msgparams map carries no file flag either
        }
    }

    /** Copy the stamp on {@code msg} (if any, with its region alias) to {@code jms}; an unstamped message declares an empty caller. */
    public static void toJms(MsgEvent msg, Message jms) throws JMSException {
        toJms(msg == null ? null : Caller.fromMsgEvent(msg), jms);
        String alias = msg == null ? null : Caller.regionAliasOf(msg);
        if (alias != null) jms.setStringProperty(P_ALIAS, alias);
    }

    /** Write {@code caller} (null: none, declared as an empty caller) as the JMS properties of {@code jms}. */
    public static void toJms(Caller caller, Message jms) throws JMSException {
        if (jms instanceof org.apache.activemq.command.ActiveMQMessage) {
            // a plugin may re-send a message it received (read-only properties): the stamp is ours to write
            ((org.apache.activemq.command.ActiveMQMessage) jms).setReadOnlyProperties(false);
        }
        jms.setStringProperty(P_CALLER, caller == null ? "" : caller.getPrincipal());
        jms.setStringProperty(P_TENANT, caller == null ? "" : caller.getTenant());
        jms.setStringProperty(P_ROLES, caller == null ? "" : caller.getRolesCsv());
        jms.setStringProperty(P_ALIAS, "");                 // the alias belongs to this stamp only (see toJms(MsgEvent, ..))
    }

    /** The caller the JMS properties of {@code jms} name, or null for none (absent, empty, or malformed). */
    public static Caller fromJmsProperties(Message jms) {
        if (jms == null) return null;
        try {
            return Caller.parse(jms.getStringProperty(P_CALLER), jms.getStringProperty(P_TENANT), jms.getStringProperty(P_ROLES));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * The receiving side of the wire: strip the reserved parameters a sender wrote into the body of {@code msg} and stamp
     * the caller the JMS properties name ({@code anonymous} instead when {@code securityOn} is false). A message whose
     * properties name nobody is left unstamped (and refused wherever a caller is required).
     */
    public static Caller fromJms(MsgEvent msg, Message jms, boolean securityOn) {
        Caller c = securityOn ? fromJmsProperties(jms) : Caller.anonymous();
        if (c != null && c.isAnonymous() && securityOn) c = null;      // anonymous never crosses the wire of a secured fabric
        String alias = null;
        if (c != null && securityOn) {
            try {
                alias = jms.getStringProperty(P_ALIAS);                 // set by the broker the caller's node is attached to
            } catch (Exception unreadable) {
                alias = null;
            }
        }
        stamp(msg, c, alias == null || alias.isEmpty() ? null : alias);
        return c;
    }

    /**
     * May a network connection whose verified identity is {@code connection} declare {@code declared} as the caller of
     * a message it sends? Only:
     * <ul>
     *   <li>{@code controller:} that very node;</li>
     *   <li>{@code plugin:} a plugin of that very node;</li>
     *   <li>{@code api:} a user of that node's tenant (or of any tenant, when the node's own tenant is a superuser
     *       tenant), with {@code admin}/{@code user} only.</li>
     * </ul>
     * Anything else -- another node's controller or plugin, {@code agent:}, {@code anonymous}, a user of another tenant
     * -- is a forged stamp: the broker strips it. So no node can claim another node's identity, nor raise a user of its
     * own tenant to another tenant's (or a superuser tenant's) admin.
     *
     * @return null when acceptable, else why not
     */
    public static String acceptDeclared(io.cresco.library.security.CrescoIdentity connection, Caller declared,
                                        java.util.Set<String> superuserTenants) {
        if (declared == null) return "no caller";
        if (connection == null || connection.getTenant() == null || connection.getRegion() == null || connection.getAgent() == null) {
            return "the connection has no verified node identity";
        }
        String t = connection.getTenant(), r = connection.getRegion(), a = connection.getAgent();
        switch (declared.getKind()) {
            case CONTROLLER:
                if (!declared.getRoles().equals(java.util.Collections.singleton(Caller.ROLE_INFRA))) return "a controller stamp with roles " + declared.getRolesCsv();
                return t.equals(declared.getTenant()) && r.equals(declared.getRegion()) && a.equals(declared.getAgent())
                        ? null : "a controller stamp for another node";
            case PLUGIN:
                if (!declared.getRoles().equals(java.util.Collections.singleton(Caller.ROLE_PLUGIN))) return "a plugin stamp with roles " + declared.getRolesCsv();
                return t.equals(declared.getTenant()) && r.equals(declared.getRegion()) && a.equals(declared.getAgent())
                        ? null : "a plugin stamp for another node's plugin";
            case API:
                for (String role : declared.getRoles()) {
                    if (!Caller.ROLE_ADMIN.equals(role) && !Caller.ROLE_USER.equals(role)) return "an API stamp with role " + role;
                }
                if (t.equals(declared.getTenant())) return null;
                return superuserTenants != null && superuserTenants.contains(t) ? null
                        : "an API stamp for tenant " + declared.getTenant() + " from a node of tenant " + t;
            default:
                return "a " + declared.getKind().name().toLowerCase(java.util.Locale.ROOT) + " stamp is never declared over the network";
        }
    }
}
