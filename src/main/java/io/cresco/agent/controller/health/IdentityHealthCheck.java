package io.cresco.agent.controller.health;

import io.cresco.agent.controller.communication.CertificateManager;
import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Local check: this node's identity and peer trust. CRITICAL when the peer trust registry cannot lock its store (fenced:
 * nothing new is bound), is sealed, or holds revocations in force that are not yet recorded durably; when this node's
 * region-issued identity is in its last tenth (or expired) and re-enrollment keeps failing, or its certificate is expired or
 * not yet valid; when a parent's or child's clock is skewed past the certificate backdate. WARN when re-enrollment failed
 * earlier in the leaf's life or a clock is skewed by half the backdate. See {@link CertificateManager#identityHealth}.
 */
public class IdentityHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public IdentityHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            CertificateManager cm = ce.getCertificateManager();
            if (cm == null) return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "identity not yet initialised");
            String[] h = cm.identityHealth();
            Result.Status s = "CRITICAL".equals(h[0]) ? Result.Status.CRITICAL : "WARN".equals(h[0]) ? Result.Status.WARN : Result.Status.OK;
            return new Result(s, h[1]);
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "identity check error: " + t);
        }
    }
}
