package io.cresco.agent.controller.health;

import org.apache.felix.hc.api.Result;

import java.util.Set;

/**
 * Notified by {@link CrescoHealthExecutor} whenever a check's <em>effective</em> status changes
 * (after grace + sticky). The HC->MINA bridge subscribes to this: when a {@code link}-tagged check
 * crosses to CRITICAL it fires the corresponding MINA event
 * ({@code regionalControllerLost}/{@code globalControllerLost}). MINA remains the state authority;
 * this is the one-directional health->state signal.
 */
public interface HealthListener {

    void onHealthChange(String name, Set<String> tags, Result.Status oldStatus, Result.Status newStatus);
}
