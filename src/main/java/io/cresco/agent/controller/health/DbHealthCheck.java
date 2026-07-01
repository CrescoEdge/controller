package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.db.DBInterfaceImpl;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Local check: the controller database (Derby, via the gdb interface) is reachable. Probes with a
 * benign read of this node's own agent record — any result (including null pre-registration) proves
 * the DB is up; only a thrown exception is CRITICAL. (The DBManagerActive flag was NOT used: it
 * tracks a separate replication manager that is legitimately inactive on a global, not Derby.)
 */
public class DbHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public DbHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            DBInterfaceImpl gdb = ce.getGDB();
            if (gdb == null) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "db handle not ready");
            }
            String agent = (ce.cstate != null) ? ce.cstate.getAgent() : null;
            if (agent == null) {
                return new Result(Result.Status.OK, "db handle present (pre-registration)");
            }
            gdb.getANode(agent); // benign read; throws if Derby is unreachable
            return new Result(Result.Status.OK, "db reachable");
        } catch (Throwable t) {
            return new Result(Result.Status.CRITICAL, "db query failed: " + t);
        }
    }
}
