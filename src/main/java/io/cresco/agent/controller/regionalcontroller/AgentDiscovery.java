package io.cresco.agent.controller.regionalcontroller;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.messaging.RPC;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

public class AgentDiscovery {
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private RPC rpc;
    private GlobalExecutor gce;

    public AgentDiscovery(ControllerEngine controllerEngine) {
        //this.agentcontroller = agentcontroller;
        //logger = new CLogger(AgentDiscovery.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Debug);
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(AgentDiscovery.class.getName(),CLogger.Level.Info);


        gce = new GlobalExecutor(controllerEngine);
        //rpc = new RPC(agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), null);
    }

    private void globalSend(MsgEvent ge) {
        try {
            if(!controllerEngine.cstate.isGlobalController()) {
                    ge.setParam("dst_region",controllerEngine.cstate.getGlobalRegion());
                    ge.setParam("dst_agent",controllerEngine.cstate.getGlobalAgent());
                    //ge.setParam("dst_plugin",controllerEngine.cstate.getControllerId());
                    ge.setParam("globalcmd", Boolean.TRUE.toString());
                    controllerEngine.getActiveClient().sendAPMessage(ge);

            }
        }
        catch (Exception ex) {
            logger.error("globalSend : " + ex.getMessage());
        }
    }

    public void discover(MsgEvent le) {
        try {

            String discoverString = le.getParam("src_region") + "-" + le.getParam("src_agent") + "-" + le.getParam("src_plugin");
            logger.trace("MsgType: [" + le.getMsgType() + "] Params: [" + le.getParams() + "]");
            if (controllerEngine.getDiscoveryMap().containsKey(discoverString)) {
                logger.debug("Discovery underway for : discoverString=" + discoverString);
            } else {

                controllerEngine.getDiscoveryMap().put(discoverString, System.currentTimeMillis());

                logger.debug("WATCHDOG : AGENTDISCOVER: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                le.setParam("mode","AGENT");
                controllerEngine.getGDB().nodeUpdate(le);


                controllerEngine.getDiscoveryMap().remove(discoverString); //remove discovery block

            }

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.debug("Controller : AgentDiscovery run() : " + ex.toString());

        }
    }

}
