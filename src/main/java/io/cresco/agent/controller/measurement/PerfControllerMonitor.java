package io.cresco.agent.controller.measurement;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

public class PerfControllerMonitor {


    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Type crescoType;

    private Gson gson;

    private ControllerInfoBuilder builder;

    public PerfControllerMonitor(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PerfControllerMonitor.class.getName(),CLogger.Level.Info);

        this.builder = new ControllerInfoBuilder(controllerEngine);

        gson = new Gson();
        this.crescoType = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();


    }

    public String getResourceInfo(String actionRegion, String actionAgent) {
        String queryReturn = null;
        try
        {
            if((actionRegion != null) && (actionAgent != null)) {
                queryReturn = getAgentResourceInfo(actionRegion,actionAgent);
            } else if (actionRegion != null) {
                queryReturn = getRegionResourceInfo(actionRegion);
            } else {
                queryReturn = getRegionResourceInfo(null);
            }

        } catch(Exception ex) {
            logger.error("getResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    private String getRegionResourceInfo(String actionRegion) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;


        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();


            //List<String> inodeKPIList = dbe.getINodeKPIList(actionRegion,null);
            List<String> agentList = controllerEngine.getGDB().getNodeList(actionRegion, null);
            for(String agent : agentList) {

                String sysInfoJson= getSysInfo(actionRegion,agent);
                if(sysInfoJson != null) {

                    Type type = new TypeToken<Map<String, List<Map<String, String>>>>() {
                    }.getType();

                    Map<String, List<Map<String, String>>> myMap = gson.fromJson(sysInfoJson, type);

                    cpu_core_count += Long.parseLong(myMap.get("cpu").get(0).get("cpu-logical-count"));

                    memoryAvailable += Long.parseLong(myMap.get("mem").get(0).get("memory-available"));
                    memoryTotal += Long.parseLong(myMap.get("mem").get(0).get("memory-total"));

                    for (Map<String, String> fsMap : myMap.get("fs")) {
                        diskAvailable += Long.parseLong(fsMap.get("available-space"));
                        diskTotal += Long.parseLong(fsMap.get("total-space"));
                    }
                }

            }

            Map<String,String> resourceTotal = new HashMap<>();
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));
            regionArray.add(resourceTotal);
            queryMap.put("regionresourceinfo",regionArray);

            queryReturn = gson.toJson(queryMap);


        } catch(Exception ex) {
            logger.error("getRegionResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    private String getAgentResourceInfo(String actionRegion, String actionAgent) {
        String queryReturn = null;

        Map<String, List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();



            Map<String,String> resourceTotal = new HashMap<>();
            String perfString = getSysInfo(actionRegion,actionAgent);
            if(perfString != null) {
                resourceTotal.put("perf", perfString);
            }
            regionArray.add(resourceTotal);


            /*
            List<String> inodeKPIList = dbe.getINodeKPIList(actionRegion,actionAgent);
            for(String str : inodeKPIList) {
                Map<String,String> resourceTotal = new HashMap<>();
                resourceTotal.put("perf", dbe.uncompressString(str));
                regionArray.add(resourceTotal);

            }
            */

            queryMap.put("agentresourceinfo",regionArray);
            queryReturn = gson.toJson(queryMap);

        } catch(Exception ex) {
            logger.error("getAgentResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    public String getSysInfo(String regionId, String agentId) {
        String response = null;
        try {

            String returnString = controllerEngine.getGDB().getPluginListByType("pluginname", "io.cresco.sysinfo");

            Map<String, List<Map<String, String>>> myRepoMap = gson.fromJson(returnString, crescoType);

            if (myRepoMap != null) {

                    if (myRepoMap.containsKey("plugins")) {

                        for (Map<String, String> repoMap : myRepoMap.get("plugins")) {

                            String region = repoMap.get("region");
                            String agent = repoMap.get("agent");
                            String pluginID = repoMap.get("pluginid");

                            if(regionId.equals(region) && agentId.equals(agent)) {

                            //logger.error("SEND :" + region + " " + agent + " " + pluginID + " data");
                                /*
                                MsgEvent benchInfoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                                benchInfoRequest.setParam("action", "getbenchmark");
                                MsgEvent benchInfoResponse = plugin.sendRPC(benchInfoRequest);
                                String benchString = benchInfoResponse.getCompressedParam("bench");
                                logger.error(benchString);
                            */
                            MsgEvent sysInfoRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                            sysInfoRequest.setParam("action", "getsysinfo");

                            MsgEvent sysInfoResponse = plugin.sendRPC(sysInfoRequest);

                            if (sysInfoResponse != null) {
                                String perfString = sysInfoResponse.getCompressedParam("perf");
                                if (perfString != null) {
                                    //logger.info("perfString: " + perfString);
                                    response = perfString;
                                } else {
                                    response = "{Error}";
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }
        return response;
    }

    public String getIsAttachedMetrics(String actionRegion, String actionAgent, String actionPluginId) {
        String returnString = null;
        returnString = getKPIInfo(actionRegion,actionAgent,actionPluginId);
        logger.debug("String getIsAttachedMetrics(String actionRegion, String actionAgent, String actionPluginId) " + returnString);

        return returnString;
    }

    public String getKPIInfo(String regionId, String agentId, String pluginId) {
        String response = null;
        try {

            //response = kpiCache.getIfPresent(regionId + "." + agentId + "." + pluginId);
            //response = kpiCache.getIfPresent(regionId + "." + agentId);
            response = "{ERROR NO LONGER IMPLEMENTED}";
            response = builder.getControllerInfoMap();

        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return response;
    }


}
