package io.cresco.agent.data;


import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.TextMessage;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Cresco logger
 * @author V.K. Cody Bumgardner
 * @author Caylin Hickey
 * @since 0.1.0
 */
public class DataPlaneLogger {

    private NavigableMap<String, CLogger.Level> loggerMap;

    private AtomicBoolean lockMap = new AtomicBoolean();

    private AtomicBoolean isEnabled = new AtomicBoolean(false);

    private PluginBuilder pluginBuilder;
    private CLogger logger;

    public DataPlaneLogger(PluginBuilder pluginBuilder) {
        this.pluginBuilder = pluginBuilder;
        logger = pluginBuilder.getLogger(DataPlaneLogger.class.getName(),CLogger.Level.Info);
        loggerMap = Collections.synchronizedNavigableMap(new TreeMap<>());

    }

    public void setIsEnabled(boolean isEnabledSet) {
        isEnabled.set(isEnabledSet);
    }

    public boolean getIsEnabled() {
        return isEnabled.get();
    }

    public void logToDataPlane(CLogger.Level level, String logIdent, String message) {
        if(isEnabled.get()) {
            try {
                if (pluginBuilder.getAgentService().getAgentState() != null) {
                    if (pluginBuilder.getAgentService().getAgentState().isActive()) {
                        if (pluginBuilder.getAgentService().getDataPlaneService().isFaultURIActive()) {
                            //we can sent do dataplane
                            if (level.getValue() <= getLogLevel(logIdent).getValue()) {
                                TextMessage textMessage = pluginBuilder.getAgentService().getDataPlaneService().createTextMessage();
                                textMessage.setStringProperty("event", "logger");
                                textMessage.setStringProperty("pluginname", pluginBuilder.getConfig().getStringParam("pluginname"));
                                textMessage.setStringProperty("region_id", pluginBuilder.getRegion());
                                textMessage.setStringProperty("agent_id", pluginBuilder.getAgent());
                                textMessage.setStringProperty("plugin_id", pluginBuilder.getPluginID());
                                textMessage.setStringProperty("loglevel", level.name());
                                textMessage.setText(message);

                                pluginBuilder.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, textMessage);
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                logger.error("logToDataPlane() Error : " + ex.getMessage());
            }
        }
    }

    public CLogger.Level getLogLevel(String logId) {

        SortedMap<String, CLogger.Level> searchmap = null;

        synchronized (lockMap) {

            if (loggerMap.containsKey(logId)) {
                //direct hit
                return loggerMap.get(logId);
            } else {

                String prefix = "";
                if(logId.contains(":")) {
                    //limit search key to location
                    prefix = logId.split(":")[0];
                }

                //build search map
                searchmap = loggerMap.subMap(prefix, logId);
            }
        }

        if(searchmap != null) {

            if(searchmap.size() > 0) {
                return searchmap.get(searchmap.lastKey());
            }
        }

        return CLogger.Level.Info;

    }

    public void setLogLevel(String logId, CLogger.Level level) {
        synchronized (lockMap) {
            if((loggerMap.containsKey(logId)) && (level == CLogger.Level.Info)) {
                loggerMap.remove(logId);
            } else {
                loggerMap.put(logId, level);
            }
        }
    }

}