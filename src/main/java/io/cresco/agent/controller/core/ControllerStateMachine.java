package io.cresco.agent.controller.core;

import io.cresco.library.agent.ControllerMode;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

public class ControllerStateMachine {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    public ControllerStateMachine(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ControllerStateMachine.class.getName(),CLogger.Level.Trace);
    }

    public int getState() {
        int state;
        try {
            //isAgent
            String iA = "0";
            if(plugin.getConfig().getBooleanParam("is_agent",false)) {
                iA = "1";
            }

            //is agent has host
            String iAh = "0";
            if(plugin.getConfig().getBooleanParam("regional_controller_host") != null) {
                iAh = "1";
            }

            //isRegion
            String iR = "0";
            if(plugin.getConfig().getBooleanParam("is_region",false)) {
                iR = "1";
            }

            //is region has host
            String iRh = "0";
            if(plugin.getConfig().getBooleanParam("global_controller_host") != null) {
                iRh = "1";
            }

            //isRegion
            String iG = "0";
            if(plugin.getConfig().getBooleanParam("is_global",false)) {
                iG = "1";
            }

            ControllerMode currentMode = controllerEngine.cstate.getControllerState();

            String iPi = "0";
            if(currentMode == ControllerMode.PRE_INIT) {
                iPi = "1";
            }

            String iSi = "0";
            if(currentMode == ControllerMode.STANDALONE_INIT) {
                iSi = "1";
            }

            String iS = "0";
            if(currentMode == ControllerMode.STANDALONE) {
                iS = "1";
            }

            String iSf = "0";
            if(currentMode == ControllerMode.STANDALONE_FAILED) {
                iSf = "1";
            }

            String iSs = "0";
            if(currentMode == ControllerMode.STANDALONE_SHUTDOWN) {
                iSs = "1";
            }

            String iAi = "0";
            if(currentMode == ControllerMode.AGENT_INIT) {
                iAi = "1";
            }

            String iAA = "0";
            if(currentMode == ControllerMode.AGENT) {
                iAA = "1";
            }

            String iAf = "0";
            if(currentMode == ControllerMode.AGENT_FAILED) {
                iAf = "1";
            }

            String iAs = "0";
            if(currentMode == ControllerMode.AGENT_SHUTDOWN) {
                iAs = "1";
            }

            String iRi = "0";
            if(currentMode == ControllerMode.REGION_INIT) {
                iRi = "1";
            }

            String iRf = "0";
            if(currentMode == ControllerMode.REGION_FAILED) {
                iRf = "1";
            }

            String iRR = "0";
            if(currentMode == ControllerMode.REGION) {
                iRR = "1";
            }

            String iRGi = "0";
            if(currentMode == ControllerMode.REGION_GLOBAL_INIT) {
                iRGi = "1";
            }

            String iRGf = "0";
            if(currentMode == ControllerMode.REGION_GLOBAL_FAILED) {
                iRGf = "1";
            }

            String iRG = "0";
            if(currentMode == ControllerMode.REGION_GLOBAL) {
                iRG = "1";
            }

            String iRs = "0";
            if(currentMode == ControllerMode.REGION_SHUTDOWN) {
                iRs = "1";
            }

            String iGi = "0";
            if(currentMode == ControllerMode.GLOBAL_INIT) {
                iGi = "1";
            }

            String iGG = "0";
            if(currentMode == ControllerMode.GLOBAL) {
                iGG = "1";
            }

            String iGf = "0";
            if(currentMode == ControllerMode.GLOBAL_FAILED) {
                iGf = "1";
            }

            String iGs = "0";
            if(currentMode == ControllerMode.GLOBAL_SHUTDOWN) {
                iGs = "1";
            }

            String routeString = iGs + iGf + iGG + iGi + iRs + iRG + iRGf + iRGi + iRR + iRf + iRi + iAs + iAf + iAA + iAi + iSs + iSf + iS + iSi + iPi + iG + iRh + iR + iAh + iA;
            state = Integer.parseInt(routeString, 2);

        } catch (Exception ex) {
                logger.error("ControllerStateMachine : getState Error: " + ex.getMessage());
            state = -1;
        }

        return state;
    }


}
