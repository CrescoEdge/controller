package io.cresco.agent.controller.measurement;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Map;
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

            MsgEvent tick = plugin.getKPIMsgEvent();
            tick.setParam("resource_id",plugin.getConfig().getStringParam("resource_id","controllerinfo_resource"));
            tick.setParam("inode_id",plugin.getConfig().getStringParam("inode_id","controllerinfo_inode"));

            tick.setCompressedParam("perf",builder.getControllerInfoMap());
            plugin.msgOut(tick);


        }
    }
}
