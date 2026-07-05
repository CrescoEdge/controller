package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.data.DataPlaneServiceImpl;
import io.cresco.library.data.DataPlaneService;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Local check for the embedded Complex-Event-Processing engine (Siddhi runs in-process in the
 * controller's DataPlaneService; the standalone cep plugin was removed). Reports the active CEP
 * query count, self-guarding to TEMPORARILY_UNAVAILABLE while the dataplane/Siddhi are still
 * coming up so a slow init never looks fatal. Registered as a Felix HealthCheck (tag=local),
 * discovered by {@link CrescoHealthExecutor} exactly like the broker/db/disk/plugin checks — the
 * same central health system every Cresco plugin uses.
 */
public class CepHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public CepHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            DataPlaneService dps = ce.getDataPlaneService();
            if (!(dps instanceof DataPlaneServiceImpl)) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "dataplane not ready");
            }
            DataPlaneServiceImpl impl = (DataPlaneServiceImpl) dps;
            if (!impl.isCEPReady()) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "cep engine initializing");
            }
            int active = impl.getActiveCEPCount();
            return new Result(Result.Status.OK, "cep OK: " + active + " active quer" + (active == 1 ? "y" : "ies"));
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "cep check error: " + t);
        }
    }
}
