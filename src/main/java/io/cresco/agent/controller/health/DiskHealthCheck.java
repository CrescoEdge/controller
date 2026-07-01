package io.cresco.agent.controller.health;

import io.cresco.agent.controller.core.ControllerEngine;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

import java.io.File;

/**
 * Local check: usable free space under the Cresco data directory (KahaDB + Derby live here).
 * CRITICAL below {@code health_disk_floor_mb} (default 100), WARN below 2x floor. Pure JDK.
 */
public class DiskHealthCheck implements HealthCheck {

    private final ControllerEngine ce;

    public DiskHealthCheck(ControllerEngine ce) {
        this.ce = ce;
    }

    @Override
    public Result execute() {
        try {
            long floorMb = ce.getPluginBuilder().getConfig().getLongParam("health_disk_floor_mb", 100L);
            File dir = resolveDataDir();
            long freeMb = dir.getUsableSpace() / (1024L * 1024L);
            if (freeMb < floorMb) {
                return new Result(Result.Status.CRITICAL,
                        "low disk: " + freeMb + "MB free < floor " + floorMb + "MB (" + dir + ")");
            }
            if (freeMb < floorMb * 2) {
                return new Result(Result.Status.WARN,
                        "disk approaching floor: " + freeMb + "MB free (" + dir + ")");
            }
            return new Result(Result.Status.OK, freeMb + "MB free (" + dir + ")");
        } catch (Throwable t) {
            return new Result(Result.Status.HEALTH_CHECK_ERROR, "disk check error: " + t);
        }
    }

    private static File resolveDataDir() {
        String loc = System.getProperty("cresco_data_location");
        File dir = (loc != null) ? new File(loc) : new File("cresco-data");
        while (dir != null && !dir.exists()) {
            dir = dir.getParentFile();
        }
        return (dir != null) ? dir : new File(".");
    }
}
