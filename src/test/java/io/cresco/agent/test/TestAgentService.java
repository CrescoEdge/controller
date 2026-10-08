package io.cresco.agent.test;

import io.cresco.agent.core.AgentServiceImpl;
import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.agent.ControllerState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

/**
 * An AgentService for unit tests: region r1, agent a1 (agent path r1_a1), real CLoggers, and a
 * chosen agent data directory. Nothing else of the controller is started.
 */
public final class TestAgentService implements AgentService {

    public static final String AGENT_PATH = "r1_a1";

    private final AgentServiceImpl loggers = new AgentServiceImpl();
    private final String dataDirectory;
    private final AgentState state = new AgentState((ControllerState) Proxy.newProxyInstance(
            ControllerState.class.getClassLoader(), new Class<?>[]{ControllerState.class},
            (p, m, a) -> {
                switch (m.getName()) {
                    case "getRegion": return "r1";
                    case "getAgent": return "a1";
                    case "getAgentPath": return AGENT_PATH;
                    case "isActive": return Boolean.TRUE;
                    default: return m.getReturnType() == boolean.class ? Boolean.FALSE : null;
                }
            }));

    public TestAgentService(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    /** A PluginBuilder over this service with the given agent config (values as strings, like agent.ini). */
    public static PluginBuilder plugin(String dataDirectory, Map<String, Object> cfg) {
        return new PluginBuilder(new TestAgentService(dataDirectory), AgentServiceImpl.class.getName(),
                new MockBundleContext(), new HashMap<>(cfg));
    }

    @Override public AgentState getAgentState() { return state; }
    @Override public DataPlaneService getDataPlaneService() { return null; }
    @Override public CLogger getCLogger(PluginBuilder pb, String b, String i, CLogger.Level l) { return loggers.getCLogger(pb, b, i, l); }
    @Override public CLogger getCLogger(PluginBuilder pb, String b, String i) { return loggers.getCLogger(pb, b, i); }
    @Override public void msgOut(String id, MsgEvent msg) { }
    @Override public void setLogLevel(String logId, CLogger.Level level) { }
    @Override public String getAgentDataDirectory() { return dataDirectory; }
}
