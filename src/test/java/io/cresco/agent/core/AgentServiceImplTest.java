package io.cresco.agent.core;

import io.cresco.agent.test.MockBundleContext;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class AgentServiceImplTest {

    @Test
    void getCLogger() {

        try {
            MockBundleContext bundleContext = new MockBundleContext();
            Map<String, Object> configParams = new HashMap<>();
            AgentServiceImpl agentService = new AgentServiceImpl();
            PluginBuilder pluginBuilder = new PluginBuilder(agentService, agentService.getClass().getName(), bundleContext, configParams);
            CLogger logger = agentService.getCLogger(pluginBuilder, agentService.getClass().getName(), agentService.getClass().getName() + "test", CLogger.Level.Info);
            logger.info("info test");
            logger.error("error test");
            logger.debug("debug test");
            logger.trace("trace test");
            logger.warn("warn test");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    void activate() {

        try {
            MockBundleContext bundleContext = new MockBundleContext();
            AgentServiceImpl agentService = new AgentServiceImpl();
            agentService.activate(bundleContext);

            String state = "unknown";
            int i = 0;
            while(!state.equals("GLOBAL")) {
                state = agentService.getAgentState().getControllerState().name();
                System.out.println("AGENT STATE: " + state);
                Thread.sleep(1000);
            }

            agentService.deactivate(bundleContext);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Test
    void deactivate() {
        try {
            MockBundleContext bundleContext = new MockBundleContext();
            AgentServiceImpl agentService = new AgentServiceImpl();
            agentService.activate(bundleContext);
            agentService.deactivate(bundleContext);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    void modified() {

        try {
            MockBundleContext bundleContext = new MockBundleContext();
            AgentServiceImpl agentService = new AgentServiceImpl();
            agentService.activate(bundleContext);
            Map<String, Object> configParams = new HashMap<>();
            agentService.modified(bundleContext,configParams);
            agentService.deactivate(bundleContext);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Test
    void setLogLevel() {

        try {
            MockBundleContext bundleContext = new MockBundleContext();
            AgentServiceImpl agentService = new AgentServiceImpl();
            agentService.activate(bundleContext);
            agentService.setLogLevel("test", CLogger.Level.Info);
            agentService.deactivate(bundleContext);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Test
    void getAgentState() {
        MockBundleContext bundleContext = new MockBundleContext();
        AgentServiceImpl agentService = new AgentServiceImpl();
        agentService.activate(bundleContext);
        agentService.getAgentState();
        agentService.deactivate(bundleContext);
    }

    @Test
    void msgOut() {
    }

    @Test
    void getDataPlaneService() {
    }
}