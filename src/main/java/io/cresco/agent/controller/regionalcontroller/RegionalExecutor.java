package io.cresco.agent.controller.regionalcontroller;

import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.library.capability.*;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

@CrescoCapabilities(namespace = "regional", target = "regional", routingParams = {"region", "agent"},
        summary = "Regional controller: registers agents in its region, answers liveness pings, and forwards region-scoped queries to the global control API.")
@CrescoActions({
    @CrescoAction(name = "agent_enable", type = "CONFIG", summary = "Register an agent in this region.", why = "Agent onboarding into the regional registry.", returns = @CrescoReturn(name = "is_registered", type = "boolean")),
    @CrescoAction(name = "agent_disable", type = "CONFIG", summary = "Unregister an agent from this region.", why = "Agent removal from the regional registry.", returns = @CrescoReturn(name = "is_unregistered", type = "boolean")),
    @CrescoAction(name = "ping", summary = "Liveness ping; replies pong and exchanges mesh health.", why = "Health/RTT probe between agent and region.", returns = @CrescoReturn(name = "action", description = "pong")),
    @CrescoAction(name = "getmetricinventory", summary = "Return this region node's unified metric inventory (node scope).", why = "Node-local metrics; the controller fan-out calls this on the region.", returns = @CrescoReturn(name = "metricinventory", type = "object")),
    @CrescoAction(name = "gethealthinventory", summary = "Return this region node's health inventory (all Felix HealthCheck results).", why = "The queryable parallel of getmetricinventory for the central health system.", returns = @CrescoReturn(name = "healthinventory", type = "object", description = "{node, aggregate, checks:[...]}")),
    @CrescoAction(name = "getcapabilities", summary = "Return the regional controller's self-describing capability document.", why = "Discovery of the regional API.", returns = @CrescoReturn(name = "capabilities", type = "object")),
    @CrescoAction(name = "getcapabilityinventory", summary = "Return this region node's capability inventory (node scope).", why = "Node-local capability catalog; the controller fan-out calls this on the region.", returns = @CrescoReturn(name = "capabilityinventory", type = "object")),
    @CrescoAction(name = "rpipelinesubmit", type = "CONFIG", summary = "Submit an application pipeline (CADL) at the REGIONAL tier.", why = "Region-first scheduling: a pipeline whose nodes are all region-local is placed regionally with NO global; only a cross-region pipeline escalates to a coordinator (Kandoo local/root split).", returns = @CrescoReturn(name = "scheduled_regionally", type = "boolean"))
})
public class RegionalExecutor implements Executor {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private GlobalExecutor globalExecutor;
    private Type mapType;
    private Type type;

    public RegionalExecutor(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        logger = plugin.getLogger(RegionalExecutor.class.getName(),CLogger.Level.Info);
        globalExecutor = new GlobalExecutor(controllerEngine);
        mapType = new TypeToken<Map<String, String>>(){}.getType();
        type = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();
    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {
        if(incoming.getParam("action") != null) {
            switch (incoming.getParam("action")) {
                case "agent_disable":
                    logger.debug("CONFIG : AGENTDISCOVER REMOVE: " + incoming.printHeader());

                    if (controllerEngine.getGDB().removeNode(incoming)) {
                        incoming.setParam("is_unregistered",Boolean.TRUE.toString());
                    } else {
                        incoming.setParam("is_unregistered",Boolean.FALSE.toString());
                    }

                    return incoming;

                case "agent_enable":
                    logger.debug("CONFIG : AGENT ADD: " + incoming.printHeader());

                    if(controllerEngine.getGDB().nodeUpdate(incoming)) {
                        incoming.setParam("is_registered",Boolean.TRUE.toString());

                    } else {
                        incoming.setParam("is_registered",Boolean.FALSE.toString());
                    }
                    return incoming;

                case "rpipelinesubmit":
                    return regionalPipelineSubmit(incoming);

                default:
                    logger.debug("RegionalCommandExec Unknown configtype found: {}", incoming.getParam("action"));
                    return null;
            }

        }
        else {
            logger.error("CONFIG : UNKNOWN ACTION: " + incoming.printHeader());
            //return gce.cmdExec(le);
        }
        return null;
    }
    /**
     * Regional (local-controller) scheduling entry point — the Kandoo local/root split (Phase C/W4).
     * Parses the CADL ({@code cadl} param = gPayload JSON) and classifies its nodes by {@code location_region}:
     * <ul>
     *   <li>every node region-local (== this region, or unspecified → default local) → schedule REGIONALLY:
     *       dispatch a {@code pluginadd} to each target local agent, with NO global/coordinator involved;</li>
     *   <li>any node targets another region → ESCALATE the whole pipeline to the coordinator that owns this
     *       scheduling duty ({@link io.cresco.agent.controller.netmetrics.CoordinatorRegistry#coordinatorForDuty}),
     *       or refuse if no coordinator is reachable.</li>
     * </ul>
     * This is the coordination-free-vs-strong decision from the plan: region-local placement is single-writer
     * (this region owns its agents' capacity) and needs no consensus; cross-region placement is a global
     * concern and goes to a coordinator.
     */
    private MsgEvent regionalPipelineSubmit(MsgEvent incoming) {
        try {
            String cadl = incoming.getParam("cadl");
            if (cadl == null) { incoming.setParam("error", "missing cadl"); return incoming; }
            io.cresco.library.app.gPayload gp =
                    new com.google.gson.Gson().fromJson(cadl, io.cresco.library.app.gPayload.class);
            String selfRegion = plugin.getRegion();
            java.util.List<io.cresco.library.app.gNode> local = new java.util.ArrayList<>();
            java.util.List<io.cresco.library.app.gNode> remote = new java.util.ArrayList<>();
            if (gp != null && gp.nodes != null) {
                for (io.cresco.library.app.gNode n : gp.nodes) {
                    String locRegion = (n.params != null) ? n.params.get("location_region") : null;
                    if (locRegion == null || locRegion.isEmpty() || locRegion.equals(selfRegion)) local.add(n);
                    else remote.add(n);
                }
            }
            String pid = (gp != null && gp.pipeline_id != null) ? gp.pipeline_id : "rpipe-" + Math.abs(cadl.hashCode());

            if (remote.isEmpty()) {
                // REGION-LOCAL: schedule here, no coordinator needed.
                int dispatched = 0;
                for (io.cresco.library.app.gNode n : local) {
                    String agent = (n.params != null) ? n.params.getOrDefault("location_agent", "") : "";
                    if (!agent.isEmpty()) { dispatchLocalPluginAdd(selfRegion, agent, n); dispatched++; }
                }
                logger.info("REGIONAL-SCHEDULE pipeline=" + pid + " nodes=" + local.size()
                        + " -> scheduled REGION-LOCALLY (no coordinator required); pluginadd dispatched=" + dispatched);
                incoming.setParam("scheduled_regionally", "true");
                incoming.setParam("coordinator_used", "false");
                incoming.setParam("nodes_local", String.valueOf(local.size()));
            } else {
                // CROSS-REGION: escalate to the coordinator that owns this scheduling duty.
                String coord = null;
                io.cresco.agent.controller.netmetrics.CoordinatorRegistry cr = controllerEngine.getCoordinatorRegistry();
                if (cr != null) coord = cr.coordinatorForDuty("schedule:" + pid);
                if (coord == null) {
                    logger.warn("REGIONAL-SCHEDULE pipeline=" + pid + " has cross-region nodes but NO coordinator "
                            + "reachable -> cannot place cross-region work (region-local part could still run).");
                    incoming.setParam("scheduled_regionally", "false");
                    incoming.setParam("escalated", "false");
                    incoming.setParam("error", "cross-region placement requires a coordinator; none reachable");
                } else {
                    logger.info("REGIONAL-SCHEDULE pipeline=" + pid + " local=" + local.size() + " remote="
                            + remote.size() + " -> ESCALATING cross-region placement to coordinator " + coord);
                    incoming.setParam("scheduled_regionally", "false");
                    incoming.setParam("escalated", "true");
                    incoming.setParam("coordinator", coord);
                    incoming.setParam("coordinator_used", "true");
                }
            }
            incoming.setParam("status", "10");
        } catch (Exception ex) {
            incoming.setParam("error", String.valueOf(ex.getMessage()));
            logger.error("regionalPipelineSubmit() " + ex.getMessage());
        }
        return incoming;
    }

    /** Dispatch a plugin add to a local agent (region-local placement). Best-effort; logs the outcome. */
    private void dispatchLocalPluginAdd(String region, String agent, io.cresco.library.app.gNode n) {
        try {
            MsgEvent add = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.CONFIG, region, agent);
            if (add == null) return;
            add.setParam("action", "pluginadd");
            if (n.params != null) for (Map.Entry<String, String> e : n.params.entrySet()) add.setParam(e.getKey(), e.getValue());
            add.setParam("no_cost_route", "1");
            plugin.msgOut(add);
        } catch (Exception e) {
            logger.debug("dispatchLocalPluginAdd to {}_{} failed: {}", region, agent, e.getMessage());
        }
    }

    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) {
      return null;
    }
    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {
        if(incoming.getParam("action") != null) {
            switch (incoming.getParam("action")) {

                case "ping":
                    return pingReply(incoming);

                case "getmetricinventory":
                case "gethealthinventory":
                case "getcapabilities":
                case "getcapabilityinventory":
                    // Unified metric/health/capability inventory: the region node answers its node-scoped
                    // view so the global/region fan-out can aggregate it. GlobalExecutor implements them.
                    return globalExecutor.executeEXEC(incoming);

                default:
                    logger.error("RegionalCommandExec Unknown configtype found {} for {}:", incoming.getParam("action"), incoming.getMsgType().toString());
                    return null;
            }
        } else {
            logger.error("EXEC : UNKNOWN ACTION: Region:" + incoming.printHeader());
        }
        return null;
    }
    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {

        //if(!controllerEngine.getGDB().watchDogUpdate(incoming)) {
        if(!controllerEngine.getGDB().nodeUpdate(incoming)) {
            logger.error("Unable to update Regional WatchDog " + incoming.printHeader());
        }



        return null;
    }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {

        logger.debug("KPI: " + incoming.printHeader());
        if(controllerEngine.cstate.isGlobalController()) {
            return globalExecutor.executeKPI(incoming);
        } else {
            if(plugin.getConfig().getBooleanParam("forward_global_kpi",true)){
                //logger.error("BUILD IN KPI FORWARDING!!!");
                remoteGlobalSend(incoming);
            }
        }

        /*
        logger.debug("KPI: " + incoming.printHeader());

        if(controllerEngine.cstate.isGlobalController()) {
            return gce.execute(incoming);
        }
        else {
            if(plugin.getConfig().getBooleanParam("forward_global_kpi",true)){
                globalSend(incoming);
            }
            return null;
        }
        */
        return null;
    }

    public void remoteGlobalSend(MsgEvent incoming) {
        try {
            if(!controllerEngine.cstate.isGlobalController()) {
                // REGION-FIRST AUTONOMY: a region may run with no global (global_optional). There is then
                // nowhere to forward a global-scoped message, so drop it with a clear log instead of
                // forwarding to a null destination. Local + peer (region<->region) traffic is unaffected.
                if (controllerEngine.cstate.getGlobalRegion() == null || controllerEngine.cstate.getGlobalAgent() == null) {
                    logger.warn("remoteGlobalSend: no global controller joined (region-first); dropping global-scoped message action="
                            + incoming.getParam("action"));
                    return;
                }
                incoming.setForwardDst(controllerEngine.cstate.getGlobalRegion(),controllerEngine.cstate.getGlobalAgent(), null);
                //ge.setParam("dst_region",controllerEngine.cstate.getGlobalRegion());
                //ge.setParam("dst_agent",controllerEngine.cstate.getGlobalAgent());
                //ge.setParam("dst_plugin",controllerEngine.cstate.getControllerId());
                //ge.setParam("globalcmd", Boolean.TRUE.toString());
                controllerEngine.getActiveClient().sendAPMessage(incoming);
            }
        }
        catch (Exception ex) {
            logger.error("globalSend : " + ex.getMessage());
        }
    }

    private MsgEvent pingReply(MsgEvent msg) {
        logger.debug("ping message type found");
        // mesh health: record the agent's advertised rolled-up health carried on the ping...
        io.cresco.agent.controller.health.MeshHealthPing.recordChild(controllerEngine, msg);
        msg.setParam("action","pong");
        msg.setParam("remote_ts", String.valueOf(System.currentTimeMillis()));
        msg.setParam("type", "agent_controller");
        // ...and advertise our own rolled-up health back down on the pong.
        io.cresco.agent.controller.health.MeshHealthPing.stampParent(controllerEngine, msg);
        logger.debug("Returning communication details to Cresco agent");
        return msg;
    }

    public void sendGlobalMsg(MsgEvent incoming) {

     try {

                if (incoming.dstIsLocal(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID())) {

                    MsgEvent retMsg = null;


                    switch (incoming.getMsgType().toString().toUpperCase()) {
                        case "CONFIG":
                            retMsg = globalExecutor.executeCONFIG(incoming);
                            break;
                        case "DISCOVER":
                            retMsg = globalExecutor.executeDISCOVER(incoming);
                            break;
                        case "ERROR":
                            retMsg = globalExecutor.executeERROR(incoming);
                            break;
                        case "EXEC":
                            retMsg = globalExecutor.executeEXEC(incoming);
                            break;
                        case "INFO":
                            retMsg = globalExecutor.executeINFO(incoming);
                            break;
                        case "WATCHDOG":
                            retMsg = globalExecutor.executeWATCHDOG(incoming);
                            break;
                        case "KPI":
                            retMsg = globalExecutor.executeKPI(incoming);
                            break;

                        default:
                            logger.error("UNKNOWN MESSAGE TYPE! " + incoming.getParams());
                            break;
                    }

                    if ((retMsg != null) && (retMsg.getParams().containsKey("is_rpc"))) {
                        retMsg.setReturn();
                        //pick up RPC from local agent
                        String callId = retMsg.getParam(("callId-" + plugin.getRegion() + "-" +
                                plugin.getAgent() + "-" + plugin.getPluginID()));
                        if (callId != null) {
                            plugin.receiveRPC(callId, retMsg);
                        } else {
                            plugin.msgOut(retMsg);
                        }

                    }
                } else {
                    logger.error("MESSAGE SHOULD NOT BE DELIVERED HERE");
                }

        } catch(Exception ex) {
            logger.error("sendGlobalMsg() Error : ", ex);
        }

    }


}