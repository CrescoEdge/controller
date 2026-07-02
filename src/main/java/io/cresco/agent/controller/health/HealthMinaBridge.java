package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.agent.ControllerMode;
import io.cresco.library.utilities.CLogger;
import org.apache.felix.hc.api.Result;

import java.util.Set;

/**
 * The one-directional HC-&gt;MINA bridge. Subscribes to {@link CrescoHealthExecutor} effective-status
 * transitions; when the {@code link:parent} check crosses to {@code CRITICAL} (parent unreachable
 * beyond the grace window) it fires the corresponding MINA event. MINA remains the state authority —
 * this only feeds it a clean, grace-protected event, replacing the watchers' former direct calls.
 *
 * <p>The event is gated by the current controller mode so it matches the MINA transition table:
 * {@code AGENT -> regionalControllerLost}, {@code REGION_GLOBAL -> globalControllerLost}.
 */
public class HealthMinaBridge implements HealthListener {

    private final ControllerEngine ce;
    private final CLogger logger;

    public HealthMinaBridge(ControllerEngine ce) {
        this.ce = ce;
        this.logger = ce.getPluginBuilder().getLogger(HealthMinaBridge.class.getName(), CLogger.Level.Info);
    }

    @Override
    public void onHealthChange(String name, Set<String> tags, Result.Status oldStatus, Result.Status newStatus) {
        try {
            if (newStatus != Result.Status.CRITICAL) {
                return;
            }
            if (tags == null || !tags.contains(HealthTags.LINK_PARENT)) {
                return; // only the parent link drives a state transition
            }
            // Parent link is lost: drop any stale recorded parent health so recovery re-logs cleanly.
            if (ce.getMeshHealth() != null) {
                ce.getMeshHealth().resetParent();
            }
            ControllerMode mode = (ce.cstate != null) ? ce.cstate.getControllerState() : null;
            if (mode == ControllerMode.AGENT) {
                logger.error("HC bridge: link:parent CRITICAL in AGENT -> regionalControllerLost");
                ce.getControllerSM().regionalControllerLost("HC bridge: parent (regional) link CRITICAL");
            } else if (mode == ControllerMode.REGION_GLOBAL) {
                logger.error("HC bridge: link:parent CRITICAL in REGION_GLOBAL -> globalControllerLost");
                ce.getControllerSM().globalControllerLost("HC bridge: parent (global) link CRITICAL");
            } else {
                logger.warn("HC bridge: link:parent CRITICAL in {} (no state action)", mode);
            }
        } catch (Throwable t) {
            logger.error("HC bridge error: {}", String.valueOf(t));
        }
    }
}
