package io.cresco.agent.controller.measurement;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.*;
import java.io.*;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class PerfControllerMonitor {
    private ControllerInfoBuilder builder;
    private Timer timer;
    private Timer cleanUpTimer;

    private boolean running = false;

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    private Cache<String, String> sysInfoCache;
    private Cache<String, String> kpiCache;
    private Cache<String, String> kpiCacheType;

    private String DBListnerId;

    private Gson gson;

    public PerfControllerMonitor(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PerfControllerMonitor.class.getName(),CLogger.Level.Info);
        builder = new ControllerInfoBuilder(controllerEngine);

        gson = new Gson();

        sysInfoCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .softValues()
                //.maximumSize(10)
                .expireAfterWrite(15, TimeUnit.MINUTES)
                .build();

        kpiCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .softValues()
                //.maximumSize(10)
                .expireAfterWrite(15, TimeUnit.MINUTES)
                .build();

        kpiCacheType = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .softValues()
                //.maximumSize(10)
                .expireAfterWrite(15, TimeUnit.MINUTES)
                .build();

        Long interval = plugin.getConfig().getLongParam("perftimer", 5000L);

        timer = new Timer();
        timer.scheduleAtFixedRate(new PerfMonitorTask(plugin), 500L, interval);

        //call cleanup timer otherwise
        cleanUpTimer = new Timer();
        cleanUpTimer.scheduleAtFixedRate(new CleanUpTask(plugin, logger, sysInfoCache, kpiCache, kpiCacheType), 300000L, 300000L);


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

                String sysInfoJson= controllerEngine.getPerfControllerMonitor().getSysInfo(actionRegion,agent);
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
            String perfString = controllerEngine.getPerfControllerMonitor().getSysInfo(actionRegion,actionAgent);
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


    public String getIsAttachedMetrics(String actionRegion, String actionAgent, String actionPluginId) {
        String returnString = null;
        returnString = getKPIInfo(actionRegion,actionAgent,actionPluginId);
        logger.debug("String getIsAttachedMetrics(String actionRegion, String actionAgent, String actionPluginId) " + returnString);

        return returnString;
    }

    public PerfControllerMonitor start() {
        if (this.running) return this;
        Long interval = plugin.getConfig().getLongParam("perftimer", 5000L);

        timer = new Timer();
        timer.scheduleAtFixedRate(new PerfMonitorTask(plugin), 500L, interval);
        return this;

    }

    public String getSysInfo(String regionId, String agentId) {
        String response = null;
        try {

            response = sysInfoCache.getIfPresent(regionId + "." + agentId);

        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return response;
    }

    public String getKPIInfo(String regionId, String agentId, String pluginId) {
        String response = null;
        try {

            response = kpiCache.getIfPresent(regionId + "." + agentId + "." + pluginId);
            //response = kpiCache.getIfPresent(regionId + "." + agentId);



        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return response;
    }

    public void removeKpiListener() {

        try {

            if(DBListnerId != null) {

                String DBListnerIdTmp = DBListnerId;
                DBListnerId = null;
                plugin.getAgentService().getDataPlaneService().removeMessageListener(DBListnerIdTmp);

            }

        } catch (Exception ex) {
            logger.error("removeKpiListener() " + ex.getMessage());
        }

    }

    public void setKpiListener() {

        //logger.info("SET KPI LISTENER");
        //setTestListener();

        MessageListener ml = new MessageListener() {

            public void onMessage(Message msg) {
                try {

                    if (msg instanceof MapMessage) {

                        MapMessage mapMessage = (MapMessage)msg;

                        if (mapMessage.getString("perf") != null) {
                            String key = mapMessage.getStringProperty("region_id") + "." + mapMessage.getStringProperty("agent_id");
                            String messageType = mapMessage.getStringProperty("pluginname");
                            if(messageType.equals("io.cresco.sysinfo")) {
                                sysInfoCache.put(key, mapMessage.getString("perf"));
                                //logger.debug("insert " + mapMessage.getStringProperty("pluginname") + " metric for " + key);

                            } else {

                                if(mapMessage.getStringProperty("plugin_id") != null) {
                                    key = key + "." + mapMessage.getStringProperty("plugin_id");
                                }
                                //add plugin Id
                                kpiCache.put(key, mapMessage.getString("perf"));
                                kpiCacheType.put(key, messageType);
                                //logger.error("insert " + mapMessage.getStringProperty("pluginname") + " metric for " + key);
                            }
                        }

                    }

                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL and plugin_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");
        DBListnerId = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");

    }

    /*
    public void setTestListener() {
        logger.info("SET TEST LISTENER");

        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {

                    if (msg instanceof BlobMessage) {

                        BlobMessage blobMessage = (BlobMessage)msg;
                        InputStream inputStream = blobMessage.getInputStream();

                        BufferedInputStream bis = new BufferedInputStream(inputStream);
                        ByteArrayOutputStream buf = new ByteArrayOutputStream();
                        int result = bis.read();
                        while(result != -1) {
                            buf.write((byte) result);
                            result = bis.read();
                        }

                        String returnString = buf.toString("UTF-8");
                        logger.info("WHHHHOO : " + returnString);

                    }
                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        //plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL and plugin_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");
        plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"");

    }
    */

    /*
    public void setSysInfoListener() {

        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {


                    if (msg instanceof MapMessage) {

                        MapMessage mapMessage = (MapMessage)msg;

                        String key = mapMessage.getStringProperty("region_id") + "." + mapMessage.getStringProperty("agent_id");
                        sysInfoCache.put(key,mapMessage.getString("perf"));

                    }
                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"pluginname = 'io.cresco.sysinfo'");

    }
    */

    public PerfControllerMonitor restart() {
        if (running) timer.cancel();
        running = false;
        return start();
    }

    public void stop() {
        timer.cancel();
        cleanUpTimer.cancel();
        removeKpiListener();
        running = false;
    }


    private class CleanUpTask extends TimerTask {
        private PluginBuilder plugin;
        private CLogger logger;
        private Cache<String, String> sysInfoCache;
        private Cache<String, String> kpiCache;
        private Cache<String, String> kpiCacheType;

        CleanUpTask(PluginBuilder plugin, CLogger logger, Cache<String, String> sysInfoCache, Cache<String, String> kpiCache, Cache<String, String> kpiCacheType) {
            this.plugin = plugin;
            this.logger = logger;
            this.sysInfoCache = sysInfoCache;
            this.kpiCache = kpiCache;
            this.kpiCacheType = kpiCacheType;
        }

        public void run() {

            try {

                if(sysInfoCache != null) {
                    //sysInfoCache.cleanUp();
                    logger.debug("CleanUpTask() sysInfoCache Cleaned");
                }

                if(kpiCache != null) {
                    //kpiCache.cleanUp();
                    logger.debug("CleanUpTask() kpiCache Cleaned");
                }

                if(kpiCacheType != null) {
                    //kpiCacheType.cleanUp();
                    logger.debug("CleanUpTask() kpiCacheType Cleaned");
                }

            } catch(Exception ex) {
                logger.error("CleanUpTask() " + ex.getMessage());
            }


        }
    }


    private class PerfMonitorTask extends TimerTask {
        private PluginBuilder plugin;

        PerfMonitorTask(PluginBuilder plugin) {
            this.plugin = plugin;
        }

        public void run() {

            /*
            MsgEvent tick = plugin.getKPIMsgEvent();
            tick.setParam("resource_id",plugin.getConfig().getStringParam("resource_id","controllerinfo_resource"));
            tick.setParam("inode_id",plugin.getConfig().getStringParam("inode_id","controllerinfo_inode"));

            tick.setCompressedParam("perf",builder.getControllerInfoMap());
            plugin.msgOut(tick);
            */
            try {
                MapMessage mapMessage = controllerEngine.getDataPlaneService().createMapMessage();



                mapMessage.setString("perf", builder.getControllerInfoMap());

                //set property
                mapMessage.setStringProperty("pluginname", "io.cresco.agent");
                mapMessage.setStringProperty("region_id", plugin.getRegion());
                mapMessage.setStringProperty("agent_id", plugin.getAgent());

                controllerEngine.getDataPlaneService().sendMessage(TopicType.AGENT, mapMessage);

//                InputStream targetStream = new ByteArrayInputStream("test message".getBytes());

//                BlobMessage tmessage = controllerEngine.getDataPlaneService().createBlobMessage(targetStream);
//                controllerEngine.getDataPlaneService().sendMessage(TopicType.AGENT, tmessage);

                //tmessage.setObjectProperty("data_stream",targetStream);
                //tmessage.setObject((Object)targetStream);

                //controllerEngine.getDataPlaneService().sendMessage(TopicType.AGENT, tmessage);
                //BlobMessage message = sess.createBlobMessage(targetStream);
                //producer.send(message, DeliveryMode.NON_PERSISTENT, pri, 0);




            } catch(Exception ex) {
                logger.error("PerfMonitorTask() " + ex.getMessage());
            }


        }
    }

}
