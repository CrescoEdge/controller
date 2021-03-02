package io.cresco.agent.controller.measurement;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

public class PerfControllerMonitor {


    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private Type crescoType;
    private MeasurementEngine me;
    private Gson gson;

    public PerfControllerMonitor(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PerfControllerMonitor.class.getName(),CLogger.Level.Info);
        this.me = controllerEngine.getMeasurementEngine();

        gson = new Gson();
        this.crescoType = new TypeToken<Map<String, List<Map<String, String>>>>() {
        }.getType();

        //start metrics for controller
        initJVMMetrics();
        initControllerMetrics();
        initRegionalMetrics();
        initGlobalMetrics();

    }

    public void shutdown() {
        try {

            me.shutdown();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

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
            response = getControllerInfoMap();

        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return response;
    }

    public String getControllerInfoMap() {

        String returnStr = null;
        try {

            Map<String,List<Map<String,String>>> info = new HashMap<>();
            info.put("controller", controllerEngine.getMeasurementEngine().getMetricGroupList("controller"));

            Map<String,String> metricsMap = new HashMap<>();
            metricsMap.put("name","controller_group");
            metricsMap.put("metrics",gson.toJson(info));

            List<Map<String,String>> metricsList = new ArrayList<>();
            metricsList.add(metricsMap);

            returnStr = gson.toJson(metricsList);

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return returnStr;
    }

    private void initJVMMetrics() {

        new ClassLoaderMetrics().bindTo(me.getCrescoMeterRegistry());
        new JvmMemoryMetrics().bindTo(me.getCrescoMeterRegistry());
        //not sure why this is disabled, perhaps not useful
        //new JvmGcMetrics().bindTo(me.getCrescoMeterRegistry());
        new ProcessorMetrics().bindTo(me.getCrescoMeterRegistry());
        new JvmThreadMetrics().bindTo(me.getCrescoMeterRegistry());

        Map<String,String> internalMap = new HashMap<>();

        internalMap.put("jvm.memory.max", "jvm");
        internalMap.put("jvm.memory.used", "jvm");
        internalMap.put("jvm.memory.committed", "jvm");
        internalMap.put("jvm.buffer.memory.used", "jvm");
        internalMap.put("jvm.threads.daemon", "jvm");
        internalMap.put("jvm.threads.live", "jvm");
        internalMap.put("jvm.threads.peak", "jvm");
        internalMap.put("jvm.classes.loaded", "jvm");
        internalMap.put("jvm.classes.unloaded", "jvm");
        internalMap.put("jvm.buffer.total.capacity", "jvm");
        internalMap.put("jvm.buffer.count", "jvm");
        internalMap.put("system.load.average.1m", "jvm");
        internalMap.put("system.cpu.count", "jvm");
        internalMap.put("system.cpu.usage", "jvm");
        internalMap.put("process.cpu.usage", "jvm");

        for (Map.Entry<String, String> entry : internalMap.entrySet()) {
            String name = entry.getKey();
            String group = entry.getValue();
            me.setExisting(name,group);
        }

    }

    private void initControllerMetrics() {
        me.setTimer("message.transaction.time", "The timer for messages", "controller");
    }

    public void initRegionalMetrics() {

        if(controllerEngine.getBrokeredAgents() != null) {

            /*
            Gauge.builder("brokered.agent.count", controllerEngine.getBrokeredAgents(), ConcurrentHashMap::size)
                    .description("The number of currently brokered agents.")
                    .register(crescoMeterRegistry);

             */
        }
    }

    public void initGlobalMetrics() {

        /*
        if(controllerEngine.getResourceScheduleQueue() != null) {
            Gauge.builder("incoming.resource.queue", controllerEngine.getResourceScheduleQueue(), BlockingQueue::size)
                    .description("The number of queued incoming resources to be scheduled.")
                    .register(crescoMeterRegistry);
        }


        if(controllerEngine.getAppScheduleQueue() != null) {
            Gauge.builder("incoming.application.queue", controllerEngine.getAppScheduleQueue(), BlockingQueue::size)
                    .description("The number of queued incoming applications to be scheduled.")
                    .register(crescoMeterRegistry);
        }
        */

    }


}
