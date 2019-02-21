package io.cresco.agent.controller.measurement;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class PerfControllerMonitor {
    private ControllerInfoBuilder builder;
    private Timer timer;
    private boolean running = false;

    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    private Cache<String, String> sysInfoCache;
    private Cache<String, String> kpiCache;
    private Cache<String, String> kpiCacheType;




    public PerfControllerMonitor(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(PerfControllerMonitor.class.getName(),CLogger.Level.Info);
        builder = new ControllerInfoBuilder(controllerEngine);

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

    public void setKpiListener() {

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
                                //logger.error("insert " + mapMessage.getStringProperty("pluginname") + " metric for " + key);

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
        plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"region_id IS NOT NULL AND agent_id IS NOT NULL AND pluginname LIKE 'io.cresco.%'");

    }

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
        running = false;
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


            } catch(Exception ex) {
                logger.error("PerfMonitorTask() " + ex.getMessage());
            }


        }
    }
}
