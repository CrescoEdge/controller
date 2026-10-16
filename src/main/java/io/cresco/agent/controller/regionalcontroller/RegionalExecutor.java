package io.cresco.agent.controller.regionalcontroller;

import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.communication.ControllerTiers;
import io.cresco.agent.controller.communication.MsgRouter;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.controller.globalcontroller.GlobalExecutor;
import io.cresco.agent.controller.globalscheduler.PluginAddRequest;
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
                        // the fabric's crypto policy rides the registration reply (CryptoPolicySync)
                        io.cresco.agent.controller.statemachine.CryptoPolicySync.stamp(incoming, null);
                        // the (re-)registered agent and its plugins are visible here now; a region under a
                        // global pushes the change up at once (diff-gated, coalesced) instead of on its next tick
                        if (controllerEngine.cstate.getControllerState() == io.cresco.library.agent.ControllerMode.REGION_GLOBAL) {
                            controllerEngine.requestStateUpdate();
                        }

                    } else {
                        incoming.setParam("is_registered",Boolean.FALSE.toString());
                    }
                    return incoming;

                case "rpipelinesubmit":
                    return regionalPipelineSubmit(incoming);

                default:
                    // Agent-tier actions (e.g. pluginlist) addressed to a region node: answer them
                    // about THIS node via its own agent executor instead of dropping (RPC timeout).
                    logger.debug("RegionalExec unknown CONFIG action '{}' -> agent executor", incoming.getParam("action"));
                    return controllerEngine.getAgentExecutor().executeCONFIG(incoming);
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

    /**
     * Dispatch a plugin add to a local agent (region-local placement). Best-effort; logs the outcome.
     * The request is built exactly as the global scheduler builds it ({@link PluginAddRequest}): the node's
     * config in compressed {@code configparams}, its edges in compressed {@code edges}, never spread over the
     * message as top-level params (the agent's pluginadd would not find its config there).
     */
    private void dispatchLocalPluginAdd(String region, String agent, io.cresco.library.app.gNode n) {
        try {
            MsgEvent add = plugin.getGlobalAgentMsgEvent(MsgEvent.Type.CONFIG, region, agent);
            if (add == null) return;
            PluginAddRequest.fill(add, n.params);
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
                    logger.debug("RegionalExec unknown EXEC action '{}' -> agent executor", incoming.getParam("action"));
                    return controllerEngine.getAgentExecutor().executeEXEC(incoming);
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
                    if (MsgRouter.isRpcRequest(incoming) && !"ping".equals(incoming.getParam("action"))) {
                        MsgRouter.answerWithError(plugin, incoming, MsgRouter.ROUTE_UNAVAILABLE, "REMOTE_GLOBAL",
                                "route unavailable at " + plugin.getRegion() + "_" + plugin.getAgent()
                                        + ": no global controller joined");
                    }
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
        String childPolicyId = msg.getParam(io.cresco.agent.controller.statemachine.CryptoPolicySync.ID_PARAM);
        logger.debug("ping message type found");
        // an agent of this region we hold as not ACTIVE (LOST but it kept its session, or no row) is asked
        // for its full state on the pong (StateResync); one primary-key lookup per ping
        String childRegion = msg.getSrcRegion();
        String childAgent = msg.getSrcAgent();
        if (childAgent != null && plugin.getRegion().equals(childRegion) && !plugin.getAgent().equals(childAgent)) {
            int status = controllerEngine.getGDB().getNodeStatusCode(childRegion, childAgent);
            if (io.cresco.agent.controller.statemachine.StateResync.stampPong(msg, status)) {
                logger.info("ping from agent {} which is status {} here: asking it for its full state", childAgent, status);
            }
        }
        // mesh health: record the agent's advertised rolled-up health carried on the ping...
        io.cresco.agent.controller.health.MeshHealthPing.recordChild(controllerEngine, msg);
        msg.setParam("action","pong");
        msg.setParam("remote_ts", String.valueOf(System.currentTimeMillis()));
        msg.setParam("type", "agent_controller");
        // ...and advertise our own rolled-up health back down on the pong.
        io.cresco.agent.controller.health.MeshHealthPing.stampParent(controllerEngine, msg);
        // the fabric's crypto policy: its id always, the policy itself when the agent holds another one
        io.cresco.agent.controller.statemachine.CryptoPolicySync.stamp(msg, childPolicyId);
        logger.debug("Returning communication details to Cresco agent");
        return msg;
    }

    /**
     * Run a message the router handed to this node's regional controller (MsgRouter LOCAL_REGIONAL_CONTROLLER).
     * The function's tier decides ({@link ControllerTiers}): a message for the global runs here when this node is
     * the global (whatever node it was addressed to), else goes on to the global; a regional-only function sent
     * with the global flag is refused with an error; any other message runs on this executor when it is
     * addressed to this node, and an RPC gets its reply.
     *
     * <p>This used to live on RegionHealthWatcher, which the state machine nulls and rebuilds on every
     * global-link loss (isRegionalGlobalShutdown -> stateInit) and creates only once a global is joined. A
     * region keeps serving its agents through that window, so the router now reaches this method through
     * the engine's single RegionalExecutor ({@code ControllerEngine.getRegionalExecutor()}), which is never
     * replaced: an agent ping arriving mid-rebuild is answered instead of NPE-ing in the router.
     */
    public void dispatch(MsgEvent incoming) {
        try {
            // a reply to an RPC this controller sent (e.g. a regional controller message to another region's
            // controller comes back here addressed to this node): hand it to the waiting call, never run it again
            String ownCallId = incoming.getParam("callId-" + plugin.getRegion() + "-" + plugin.getAgent() + "-" + plugin.getPluginID());
            if (ownCallId != null && ControllerTiers.isReply(incoming)) {
                plugin.receiveRPC(ownCallId, incoming);
                return;
            }
            // the controller tier of the FUNCTION decides (ControllerTiers), not the address or the flag alone
            String refusal = ControllerTiers.refusal(incoming);
            if (refusal != null) {
                logger.warn("regional dispatch: refused {}: {}", incoming.printHeader(), refusal);
                if (MsgRouter.isRpcRequest(incoming)) {
                    MsgRouter.answerWithError(plugin, incoming, MsgRouter.ROUTE_REFUSED, "tier",
                            "refused at " + plugin.getRegion() + "_" + plugin.getAgent() + ": " + refusal);
                }
                return;
            }
            if (ControllerTiers.targetsGlobal(incoming)) {
                if (controllerEngine.cstate.isGlobalController()) {
                    if (!incoming.dstIsLocal(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID())) {
                        // a global function addressed to another node: it runs here, on the global
                        logger.debug("regional dispatch: global function {} addressed to {} runs on this global",
                                incoming.getParam("action"), incoming.getForwardDst());
                        incoming.setForwardDst(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
                    }
                    sendGlobalMsg(incoming);
                } else {
                    remoteGlobalSend(incoming);
                }
            } else if (incoming.dstIsLocal(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID())) {
                MsgEvent retMsg = null;
                switch (incoming.getMsgType().toString().toUpperCase()) {
                    case "CONFIG":
                        retMsg = executeCONFIG(incoming);
                        break;
                    case "DISCOVER":
                        retMsg = executeDISCOVER(incoming);
                        break;
                    case "ERROR":
                        retMsg = executeERROR(incoming);
                        break;
                    case "EXEC":
                        retMsg = executeEXEC(incoming);
                        break;
                    case "INFO":
                        retMsg = executeINFO(incoming);
                        break;
                    case "WATCHDOG":
                        retMsg = executeWATCHDOG(incoming);
                        break;
                    case "KPI":
                        retMsg = executeKPI(incoming);
                        break;
                    default:
                        logger.error("UNKNOWN MESSAGE TYPE! {}", incoming.getParams());
                        break;
                }

                if ((retMsg != null) && (retMsg.getParams().containsKey("is_rpc"))) {
                    retMsg.setReturn();
                    String callId = retMsg.getParam(("callId-" + plugin.getRegion() + "-" +
                            plugin.getAgent() + "-" + plugin.getPluginID()));
                    if (callId != null) {
                        plugin.receiveRPC(callId, retMsg);
                    } else {
                        plugin.msgOut(retMsg);
                    }
                }
            } else {
                logger.error("RegionalController received message not destined for it: {}", incoming.printHeader());
            }
        } catch (Exception ex) {
            logger.error("regional dispatch error: {}", ex.getMessage(), ex);
        }
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