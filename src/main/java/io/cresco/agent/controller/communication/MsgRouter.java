package io.cresco.agent.controller.communication;


import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

public class MsgRouter {

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    public MsgRouter(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(MsgRouter.class.getName(),CLogger.Level.Info);
    }

    private void forwardToLocalAgent(MsgEvent rm) {
                    controllerEngine.getPluginBuilder().msgIn(rm);
    }

    private void forwardToLocalPlugin(MsgEvent rm) {
                    controllerEngine.getPluginAdmin().msgIn(rm);
    }

    private void forwardToLocalRegionalController(MsgEvent rm) {
                controllerEngine.getRegionHealthWatcher().sendRegionalMsg(rm);
    }

    private void forwardToRemoteRegionalController(MsgEvent rm) {

        //set remote regional controller address
        rm.setForwardDst(controllerEngine.cstate.getRegionalRegion(),controllerEngine.cstate.getRegionalAgent(),null);
        controllerEngine.getActiveProducer().sendMessage(rm);
        /*
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-rc")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteRegionalController() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
        */
    }

    private void forwardToLocalRegion(MsgEvent rm) {
        controllerEngine.getActiveProducer().sendMessage(rm);
        /*
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-region")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalRegion() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
        */
        
    }

    private void forwardToRemoteRegion(MsgEvent rm) {

        controllerEngine.getActiveProducer().sendMessage(rm);
        /*
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-region")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteRegion() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
        */

    }

    private void forwardToLocalGlobal(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-global")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalGlobal() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
       
    }

    private void forwardToRemoteGlobal(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-global")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteGlobal(rm) BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    public void route(MsgEvent rm) {
        long messageTimeStamp = System.nanoTime();
        try {

            rm = getTTL(rm);


            //me.setParam("action", "pluginadd");


            if(rm != null) {
                int routePath = getRoutePath(rm);
                rm.setParam("routepath-" + plugin.getAgent(), String.valueOf(routePath));

                switch (routePath) {

                    case 463:
                        logger.debug("remote agent sending message to local agent 463");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 655:
                        logger.debug("Local agent sending message to remote global agent 655");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 671:
                        logger.debug("Local agent sending message to remote global agentcontroller 671");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 687:
                        logger.debug("Local agentcontroller sending message to remote global agent 687");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 703:
                        logger.debug("Local agentcontroller sending message to remote global agentcontroller 703");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 719:
                        logger.debug("Local agent sending message to remote regional agent 719");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 735:
                        logger.debug("Local agent sending message to remote regional agentcontroller 735");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 751:
                        logger.debug("Local agentcontroller sending message to remote regional agent 751");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 767:
                        logger.debug("Local agentcontroller sending message to remote regional agentcontroller 767");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 991:
                        logger.debug("Local agent sending message to local agentcontroller 991");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 975:
                        logger.debug("Local agent sending message to self 1007");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 1007:
                        logger.debug("Local agentcontroller sending message to local agent 1007");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 1023:
                        logger.debug("Local agentcontroller sending message to local agentcontroller 1023");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 4751:
                        logger.debug("Local agentcontroller sending message to remote global agent 4751");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 4767:
                        logger.debug("Local agentcontroller sending message to remote global agent 4767");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 4783:
                        logger.debug("Local agentcontroller sending message to remote global agent 4783");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 4799:
                        logger.debug("Local agentcontroller sending message to remote global agentcontroller 4799");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteGlobal(rm);
                        break;

                    case 4815:
                        logger.debug("Local agent sending message to local regional agent 4815");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 4831:
                        logger.debug("Local agent sending message to local regional agentcontroller 4831");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;


                    case 4847:
                        logger.debug("Local agentcontroller sending message to local regional agent 4847");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 4863:
                        logger.debug("Local agentcontroller sending message to local regional agentcontroller 4863");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 5071:
                        logger.debug("Local agent sending message to self 5071");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 5087:
                        logger.debug("Local agent sending message to local agentcontroller 5087");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 5103:
                        logger.debug("Local agentcontroller sending message to local agent 5103");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 5119:
                        logger.debug("Local agentcontroller sending message to local agentcontroller 5119");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 12767:
                        logger.debug("remote agent sending message to local plugin 12767");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 12943:
                        logger.debug("Local agent sending message to local global agent 12943");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalGlobal(rm);
                        break;

                    case 12959:
                        logger.debug("Local agent sending message to local regional agentcontroller 12959");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalGlobal(rm);
                        break;

                    case 12975:
                        logger.debug("Local agentcontroller sending message to local global agent 12975");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalGlobal(rm);
                        break;

                    case 12991:
                        logger.debug("Local agentcontroller sending message to local global agentcontroller 12991");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalGlobal(rm);
                        break;

                    case 13007:
                        logger.debug("Local agent sending message to local regional agent 13007");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 13023:
                        logger.debug("Local agent sending message to local regional agentcontroller 13023");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 13039:
                        logger.debug("Local agentcontroller sending message to local regional agent 13039");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 13055:
                        logger.debug("Local agentcontroller sending message to local regional agentcontroller 13055");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 13279:
                        logger.debug("Local agent sending message to local agentcontroller 13279");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 13311:
                        logger.debug("Local agentcontroller sending message to local agentcontroller 13311");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 13263:
                        logger.debug("Local agent sending message to self 13263");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 13295:
                        logger.debug("Local agentcontroller sending message to local agent 13295");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalAgent(rm);
                        break;

                    case 17359:
                        logger.debug("Local agentcontroller sending message to remote regional or global controller 17359");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegionalController(rm);
                        break;

                    case 17391:
                        logger.debug("Local agentcontroller sending message to remote regional or global controller 17391");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegionalController(rm);
                        break;

                    case 20943:
                        logger.debug("remote agent sending message to local regional controller 20943");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 20975:
                        logger.debug("remote plugin sending message to local regional controller 20975");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 21135:
                        logger.debug("Local region sending message to remote global controller 21135");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 21199:
                        logger.debug("Local region sending message to local regional controller 21199");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegion(rm);
                        break;

                    case 21455:
                        logger.debug("Local agent sending message to local regional or global controller 21455");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 21487:
                        logger.debug("Local agent controller sending message to local regional or remote global controller 21487");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29007:
                        logger.debug("Remote regional controller sending message to local global controller 29007");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29039:
                        logger.debug("Remote plugin sending message to local global controller 29039");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;


                    case 29135:
                        logger.debug("Remote agent sending message to local regional controller 29135");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29167:
                        logger.debug("Remote agent sending message to local global controller 29167");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29327:
                        logger.debug("Local global controller sending message to remote regional controller 29327");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 29391:
                        logger.debug("Local agent sending message to remote agent 29391");
                        logger.trace(rm.getParams().toString());
                        forwardToRemoteRegion(rm);
                        break;

                    case 29647:
                        logger.debug("Local or remote agent sending message to local regional controller 29647");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    case 29663:
                        logger.debug("Local regional or local global controller 29663 sending message back to plugin");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalPlugin(rm);
                        break;

                    case 29679:
                        logger.debug("Local agentcontroller sending message to local global controller 29679");
                        logger.trace(rm.getParams().toString());
                        forwardToLocalRegionalController(rm);
                        break;

                    default:
                        //System.out.println("CONTROLLER ROUTE CASE " + routePath + " " + rm.getParams());
                        logger.error("DEFAULT ROUTE CASE " + routePath + " " + rm.getParam("desc") + rm.getParams());
                        break;
                }

            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Controller : MsgRoute : Route Failed " + ex.toString() + " " + rm.getParams().toString());

        }
        finally
        {
            controllerEngine.getMeasurementEngine().updateTimer("message.transaction.time", messageTimeStamp);
        }

    }

    private int getRoutePath(MsgEvent rm) {
        int routePath;
        try {
            String RC = "0";
            if(controllerEngine.cstate.isRegionalController()) {
                RC = "1";
            }

            String GC = "0";
            if(controllerEngine.cstate.isGlobalController()) {
                GC = "1";
            }

            String RM = "0";
            if(rm.isRegional()) {
                RM = "1";
            }
            String GM = "0";
            if(rm.isGlobal()) {
                RM = "1";
            }

            String RXre = "0";
            String RXr = "0";
            String RXae = "0";
            String RXa = "0";
            String RXp = "0";
            String RXpe = "0";


            String TXr = "0";
            String TXre = "0";
            String TXa = "0";
            String TXae = "0";
            String TXp = "0";
            String TXpe = "0";

            if (rm.getParam("dst_region") != null) {
                RXre = "1";
                if (rm.getParam("dst_region").equals(/*PluginEngine.region*/plugin.getRegion())) {
                    RXr = "1";
                }
            }
            if (rm.getParam("dst_agent") != null) {
                RXae = "1";
                if (rm.getParam("dst_agent").equals(/*PluginEngine.agent*/plugin.getAgent())) {
                    RXa = "1";
                }
            }
            if (rm.getParam("dst_plugin") != null) {
                RXpe = "1";
                if (rm.getParam("dst_plugin").equals(/*PluginEngine.agentcontroller*/plugin.getPluginID())) {
                    RXp = "1";
                }
            }


            if (rm.getParam("src_region") != null) {
                TXre = "1";
                if (rm.getParam("src_region").equals(/*PluginEngine.region*/plugin.getRegion())) {
                    TXr = "1";
                }
            }
            if (rm.getParam("src_agent") != null) {
                TXae = "1";
                if (rm.getParam("src_agent").equals(/*PluginEngine.agent*/plugin.getAgent())) {
                    TXa = "1";
                }
            }
            if (rm.getParam("src_plugin") != null) {
                TXpe = "1";
                if (rm.getParam("src_plugin").equals(/*PluginEngine.agentcontroller*/plugin.getPluginID())) {
                    TXp = "1";
                }
            }

            // 001011 10 11 11
            String routeString = GM + RM + GC + RC + TXp + RXp + TXa + RXa + TXr + RXr + TXpe + RXpe + TXae + RXae + TXre + RXre;
            routePath = Integer.parseInt(routeString, 2);
            //System.out.println("desc:" + rm.getParam("desc") + "\nroutePath:" + routePath + " RouteString:\n" + routeString + "\n" + rm.getParams());
        } catch (Exception ex) {
            if(rm != null) {
                logger.error("Controller : MsgRoute : getRoutePath Error: " + ex.getMessage() + " " + rm.getParams().toString());
            } else {
                logger.error("Controller : MsgRoute : getRoutePath Error: " + ex.getMessage() + " RM=NULL");
            }
            ex.printStackTrace();
            routePath = -1;
        }
        //System.out.println("REGIONAL CONTROLLER ROUTEPATH=" + routePath + " MsgType=" + rm.getMsgType() + " Params=" + rm.getParams());

        return routePath;
    }

    private MsgEvent getTTL(MsgEvent rm) {

        boolean isValid = true;
        try {
            if (rm.getParam("ttl") != null) {
                int ttlCount = Integer.valueOf(rm.getParam("ttl"));

                if (ttlCount > 10) {
                    System.out.println("**Controller : MsgRoute : High Loop Count**");
                    System.out.println("MsgType=" + rm.getMsgType().toString());
                    System.out.println("params=" + rm.getParams());
                    isValid = false;
                }

                ttlCount++;
                rm.setParam("ttl", String.valueOf(ttlCount));
            } else {
                rm.setParam("ttl", "0");
            }
        } catch (Exception ex) {
            isValid = false;
        }
        if(isValid) {
            return rm;
        } else {
            return null;
        }

    }

    /*

    private void forwardToLocalAgent(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-agent")) {
                try {
                    controllerEngine.getPluginBuilder().msgIn(rm);
                    isOk = true;
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalAgent() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToLocalPlugin(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-plugin")) {
                try {
                    controllerEngine.getPluginAdmin().msgIn(rm);
                    isOk = true;
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalPlugin() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToLocalRegionalController(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-rc")) {
                controllerEngine.getRegionHealthWatcher().sendRegionalMsg(rm);
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalRegionalController() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToRemoteRegionalController(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-rc")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteRegionalController() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToLocalRegion(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-region")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalRegion() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }

    }

    private void forwardToRemoteRegion(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-region")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteRegion() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

    private void forwardToLocalGlobal(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-global")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToLocalGlobal() BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }

    }

    private void forwardToRemoteGlobal(MsgEvent rm) {
        boolean isOk = false;
        if(rm.getParam("desc") != null) {
            if(rm.getParam("desc").startsWith("to-global")) {
                isOk = true;
            }
        }

        if(!isOk) {
            System.out.println("forwardToRemoteGlobal(rm) BAD MESSAGE : " + rm.getParams() + " RouteCase :" + getRoutePath(rm));
        }
    }

     */


}
