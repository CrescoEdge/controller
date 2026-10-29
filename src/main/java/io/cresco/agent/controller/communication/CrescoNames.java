package io.cresco.agent.controller.communication;

import io.cresco.library.security.CrescoIdentity;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.security.cert.X509Certificate;
import java.util.regex.Pattern;

/**
 * The one grammar for Cresco identity names (tenant, region, agent, uid), and the only ways identities are written
 * into and read out of certificates for security decisions.
 *
 * <p><b>Grammar:</b> 1 to 64 characters from {@code A-Z a-z 0-9 . -}, starting and ending with a letter or digit.
 * No whitespace, no {@code #}, no RFC 2253/4514 specials, no control characters, and no {@code _}: {@code _} joins
 * region and agent in an agent path ({@code <region>_<agent>}, the inbox queue name and the {@code cresco_to}
 * value), and Cresco already splits paths on it, so a name containing it is ambiguous. Every name a controller
 * issues, pins, records or compares is checked against this grammar; a certificate whose identity breaks it carries
 * no Cresco identity.
 *
 * <p><b>Why strict:</b> string DNs are re-parsed by different parsers (JDK RFC 2253, BouncyCastle X500Name) that
 * normalise differently (leading/trailing spaces trimmed, a leading {@code #} read as hex DER), so a guard that
 * compares the claimed string can pass while the issued certificate names somebody else. Names here never need
 * escaping, and certificates are built from the attribute values directly ({@link #x500}), never from a string DN.
 */
public final class CrescoNames {

    private CrescoNames() { }

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9](?:[A-Za-z0-9.-]{0,62}[A-Za-z0-9])?");

    /** Does {@code s} follow the identity name grammar? */
    public static boolean isValid(String s) {
        return s != null && NAME.matcher(s).matches();
    }

    /** Tenant, region and agent all valid, and the uid (when present) too. */
    public static boolean isValid(CrescoIdentity id) {
        return id != null && isValid(id.getTenant()) && isValid(id.getRegion()) && isValid(id.getAgent())
                && (id.getUid() == null || isValid(id.getUid()));
    }

    /** Why {@code s} is not a valid name (for logs), or null when it is. */
    public static String whyInvalid(String what, String s) {
        if (isValid(s)) return null;
        if (s == null) return what + " is missing";
        return what + " '" + printable(s) + "' is not a valid Cresco name (1-64 of A-Z a-z 0-9 . -, starting and ending"
                + " with a letter or digit; no '_', spaces, '#' or other special characters)";
    }

    /** Agent path {@code <region>_<agent>} of two valid names, or null. */
    public static String path(String region, String agent) {
        return (isValid(region) && isValid(agent)) ? region + "_" + agent : null;
    }

    /** Split an agent path into {region, agent}; null unless it is exactly two valid names joined by one '_'. */
    public static String[] splitPath(String path) {
        if (path == null) return null;
        int i = path.indexOf('_');
        if (i <= 0 || i != path.lastIndexOf('_')) return null;
        String region = path.substring(0, i), agent = path.substring(i + 1);
        return (isValid(region) && isValid(agent)) ? new String[]{region, agent} : null;
    }

    /**
     * The certificate subject for {@code id}, built from the attribute values (CN=agent, OU=region, O=tenant, UID=uid).
     * Throws when any name breaks the grammar: nothing is ever issued for a name that would need escaping.
     */
    public static X500Name x500(CrescoIdentity id) {
        if (id == null || !isValid(id.getAgent()) || !isValid(id.getRegion()) || !isValid(id.getTenant())
                || (id.getUid() != null && !isValid(id.getUid()))) {
            throw new IllegalArgumentException("refusing to build a certificate subject for an invalid identity: " + id);
        }
        X500NameBuilder b = new X500NameBuilder(BCStyle.INSTANCE);
        b.addRDN(BCStyle.CN, new DERUTF8String(id.getAgent()));
        b.addRDN(BCStyle.OU, new DERUTF8String(id.getRegion()));
        b.addRDN(BCStyle.O, new DERUTF8String(id.getTenant()));
        if (id.getUid() != null) b.addRDN(BCStyle.UID, new DERUTF8String(id.getUid()));
        return b.build();
    }

    /**
     * As {@link #x500}, for a subject that may omit components (a certificate that deliberately names no complete Cresco
     * identity). Every component present must still follow the grammar; enrollment uses {@link #x500}.
     */
    public static X500Name x500Partial(CrescoIdentity id) {
        if (id == null) throw new IllegalArgumentException("no identity");
        String[][] parts = {{"agent", id.getAgent()}, {"region", id.getRegion()}, {"tenant", id.getTenant()}, {"uid", id.getUid()}};
        boolean any = false;
        for (String[] p : parts) {
            if (p[1] != null) {
                any = true;
                if (!isValid(p[1])) throw new IllegalArgumentException("refusing to build a certificate subject: " + whyInvalid(p[0], p[1]));
            }
        }
        if (!any) throw new IllegalArgumentException("refusing to build an empty certificate subject");
        X500NameBuilder b = new X500NameBuilder(BCStyle.INSTANCE);
        if (id.getAgent() != null) b.addRDN(BCStyle.CN, new DERUTF8String(id.getAgent()));
        if (id.getRegion() != null) b.addRDN(BCStyle.OU, new DERUTF8String(id.getRegion()));
        if (id.getTenant() != null) b.addRDN(BCStyle.O, new DERUTF8String(id.getTenant()));
        if (id.getUid() != null) b.addRDN(BCStyle.UID, new DERUTF8String(id.getUid()));
        return b.build();
    }

    private static final String UID_OID = "0.9.2342.19200300.100.1.1";

    /**
     * The Cresco identity a certificate names, read strictly: the subject must be single-valued RDNs, CN, OU and O
     * exactly once each (UID at most once, nothing else), every value a valid name. Anything else is null: no
     * identity. This is the parser for every security decision (binding, pins, the broker's principal, enrollment).
     */
    public static CrescoIdentity identityOf(X509Certificate cert) {
        CrescoIdentity id = claimOf(cert);
        return (id == null || id.getRegion() == null || id.getTenant() == null) ? null : id;
    }

    /**
     * As {@link #identityOf}, but region and tenant may be absent (an agent's enrollment claim: its region CA stamps
     * both). Every attribute present is still read strictly; the agent name (CN) is required.
     */
    public static CrescoIdentity claimOf(X509Certificate cert) {
        if (cert == null) return null;
        try {
            LdapName ln = new LdapName(cert.getSubjectX500Principal().getName());
            String tenant = null, region = null, agent = null, uid = null;
            for (Rdn rdn : ln.getRdns()) {
                if (rdn.size() != 1) return null;                         // multi-valued RDN
                String type = rdn.getType();
                Object v = rdn.getValue();
                if (!(v instanceof String)) return null;                  // hex-encoded / binary value
                String value = (String) v;
                if (!isValid(value)) return null;
                if ("CN".equalsIgnoreCase(type)) { if (agent != null) return null; agent = value; }
                else if ("OU".equalsIgnoreCase(type)) { if (region != null) return null; region = value; }
                else if ("O".equalsIgnoreCase(type)) { if (tenant != null) return null; tenant = value; }
                else if ("UID".equalsIgnoreCase(type) || UID_OID.equals(type)) { if (uid != null) return null; uid = value; }
                else return null;                                         // an attribute Cresco never issues
            }
            if (agent == null) return null;
            return CrescoIdentity.of(tenant, region, agent, uid);
        } catch (Exception e) {
            return null;
        }
    }

    private static String printable(String s) {
        StringBuilder b = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c < 0x20 || c == 0x7f || c > 0x7e) b.append(String.format("\\u%04x", (int) c));
            else b.append(c);
            if (b.length() > 80) { b.append("..."); break; }
        }
        return b.toString();
    }
}
