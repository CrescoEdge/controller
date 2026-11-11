package io.cresco.agent.controller.measurement;

import io.cresco.agent.controller.communication.ControllerActionRoles;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.security.ActionAuthorizer;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Authority follows the tree, in the unified metric inventory ({@code getmetricinventory}, group {@value #GROUP}): the
 * calls this node allowed in {@code authority_direction=log} that {@code enforce} would refuse. An operator reads them
 * before turning {@code enforce} on: each is a call that will then be answered 403.
 *
 * <p>As {@link MsgMetrics} names its meters (the inventory names a meter by its name alone), the action is the last part
 * of the name, not a tag: {@code authz.direction.would_refuse} (every one), {@code authz.direction.would_refuse.<action>}
 * for every action the controller's tiers and the privileged floor of every bundle give a direction ({@code watchdog} and
 * {@code kpi} for those whole types), and {@code authz.direction.would_refuse.other} for an action a plugin gave a
 * direction of its own. Every meter reads a count that is kept anyway.
 */
public final class AuthorityMetrics {

    private AuthorityMetrics() { }

    public static final String GROUP = "authz";
    public static final String WOULD_REFUSE = "authz.direction.would_refuse";

    /**
     * The actions counted under their own name: every action a controller tier declares DOWN or UP (a whole type under the
     * type's name), and the privileged floor of every bundle (DOWN unless its plugin declares otherwise).
     */
    public static Set<String> actions() {
        Set<String> out = new TreeSet<>();
        ActionAuthorizer[] tiers = {
                ControllerActionRoles.agentTier(new ActionAuthorizer(ActionAuthorizer.CONTROLLER_BUNDLE)),
                ControllerActionRoles.regionalTier(), ControllerActionRoles.globalTier()};
        for (ActionAuthorizer tier : tiers) {
            for (Map.Entry<String, ActionAuthorizer.Direction> d : tier.directions().entrySet()) {
                if (d.getValue() == ActionAuthorizer.Direction.ANY) continue;
                int bar = d.getKey().indexOf('|');
                String type = d.getKey().substring(0, bar), action = d.getKey().substring(bar + 1);
                out.add(action.equals("*") ? type : action);
            }
        }
        for (Set<String> floor : ActionAuthorizer.PRIVILEGED.values()) out.addAll(floor);
        return out;
    }

    /** Registers every meter on {@code registry}; returns each meter's name with its inventory group, in registration order. */
    public static Map<String, String> register(MeterRegistry registry, ControllerEngine ce) {
        Map<String, String> names = new LinkedHashMap<>();
        FunctionCounter.builder(WOULD_REFUSE, ce, e -> e.authority().wouldRefuseCount())
                .description("Calls this node allowed (authority_direction=log) that enforce would refuse: their speaker is not where"
                        + " the action is honoured from.").register(registry);
        names.put(WOULD_REFUSE, GROUP);
        final Set<String> actions = actions();
        for (String action : actions) {
            String name = WOULD_REFUSE + "." + action.toLowerCase(Locale.ROOT);
            FunctionCounter.builder(name, ce, e -> e.authority().wouldRefuseCount(action))
                    .description("Would-refuse calls of " + action + ".").register(registry);
            names.put(name, GROUP);
        }
        FunctionCounter.builder(WOULD_REFUSE + ".other", ce, e -> e.authority().wouldRefuseCountBesides(actions))
                .description("Would-refuse calls of an action a plugin gave a direction of its own.").register(registry);
        names.put(WOULD_REFUSE + ".other", GROUP);
        return names;
    }
}
