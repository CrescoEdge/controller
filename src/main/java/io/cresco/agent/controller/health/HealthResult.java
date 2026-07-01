package io.cresco.agent.controller.health;

import org.apache.felix.hc.api.Result;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Immutable snapshot of one check's cached verdict, as maintained by {@link CrescoHealthExecutor}.
 * {@link #status} is the <em>effective</em> status (after grace + sticky); {@link #rawStatus} is
 * the last value {@code HealthCheck.execute()} actually returned.
 */
public final class HealthResult {

    public final String name;
    public final Set<String> tags;
    public final Result.Status status;
    public final Result.Status rawStatus;
    public final String message;
    public final long lastRunTs;

    public HealthResult(String name, Set<String> tags, Result.Status status,
                        Result.Status rawStatus, String message, long lastRunTs) {
        this.name = name;
        this.tags = Collections.unmodifiableSet(new LinkedHashSet<>(tags));
        this.status = status;
        this.rawStatus = rawStatus;
        this.message = message;
        this.lastRunTs = lastRunTs;
    }
}
