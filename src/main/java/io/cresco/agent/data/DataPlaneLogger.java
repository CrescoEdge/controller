package io.cresco.agent.data;


import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Cresco logger
 * @author V.K. Cody Bumgardner
 * @author Caylin Hickey
 * @since 0.1.0
 */
public class DataPlaneLogger {

    //private NavigableMap<String, CLogger.Level> loggerMap;


    private Map<String,NavigableMap<String, CLogger.Level>> loggerMap;
    private Map<String,CLogger.Level> defaultLoggerMap;
    private Map<String,Boolean> loggerEnabledMap;

    private AtomicBoolean lockMap = new AtomicBoolean();

    private PluginBuilder pluginBuilder;
    private CLogger logger;

    public DataPlaneLogger(PluginBuilder pluginBuilder) {
        this.pluginBuilder = pluginBuilder;
        logger = pluginBuilder.getLogger(DataPlaneLogger.class.getName(),CLogger.Level.Info);
        //loggerMap = Collections.synchronizedNavigableMap(new TreeMap<>());
        this.loggerMap = Collections.synchronizedMap(new HashMap<>());
        this.loggerEnabledMap = Collections.synchronizedMap(new HashMap<>());
        this.defaultLoggerMap = Collections.synchronizedMap(new HashMap<>());
    }

    private CLogger.Level defaultLogLevel = CLogger.Level.Info;

    public void shutdown() {

        synchronized (lockMap) {
            loggerEnabledMap.clear();
        }

    }

    public void setIsEnabled(String sessionId, boolean isEnabledSet) {
        synchronized (lockMap) {
            if(isEnabledSet) {
                //create if needed
                loggerEnabledMap.put(sessionId,isEnabledSet);
            } else {
                //remove if needed
                loggerEnabledMap.remove(sessionId);
                loggerMap.remove(sessionId);
            }
        }
    }

    public boolean getIsEnabled(String sessionId) {
        synchronized (lockMap) {
            if(loggerEnabledMap.containsKey(sessionId)) {
                return loggerEnabledMap.get(sessionId);
            }
        }
        return false;
    }

    public void logToDataPlane(CLogger.Level level, String logIdent, String message) {
        boolean isEnabled = false;

        synchronized (lockMap) {
            if(loggerEnabledMap.size() > 0) {
                isEnabled = true;
            }
        }

        if(isEnabled) {
            try {
                if (pluginBuilder.getAgentService().getAgentState() != null) {
                    if (pluginBuilder.getAgentService().getAgentState().isActive()) {
                        if (pluginBuilder.getAgentService().getDataPlaneService().isFaultURIActive()) {
                            //we can sent do dataplane
                            //go over each session
                            List<String> activeSessions = getActiveSessions();
                            for(String sessionId : activeSessions) {
                                if (level.getValue() <= getLogLevel(sessionId,logIdent).getValue()) {

                                    TextMessage textMessage = pluginBuilder.getAgentService().getDataPlaneService().createTextMessage();
                                    textMessage.setStringProperty("event", "logger");
                                    textMessage.setStringProperty("pluginname", pluginBuilder.getConfig().getStringParam("pluginname"));
                                    textMessage.setStringProperty("region_id", pluginBuilder.getRegion());
                                    textMessage.setStringProperty("agent_id", pluginBuilder.getAgent());
                                    textMessage.setStringProperty("plugin_id", pluginBuilder.getPluginID());
                                    textMessage.setStringProperty("loglevel", level.name());
                                    textMessage.setStringProperty("logid", logIdent);
                                    textMessage.setStringProperty("session_id", sessionId);
                                    textMessage.setText(message);

                                    pluginBuilder.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, textMessage);
                                }
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

    public List<String> getActiveSessions() {

        List<String> enabledSessions = new ArrayList<>();
        try {
            synchronized (lockMap) {
                for (Map.Entry<String, Boolean> entry : loggerEnabledMap.entrySet()) {
                    String sessionId = entry.getKey();
                    boolean isEnabled = entry.getValue();
                    if (entry.getValue()) {
                        enabledSessions.add(sessionId);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        return enabledSessions;
    }

    public CLogger.Level getLogLevel(String sessionId, String logId) {

        SortedMap<String, CLogger.Level> searchmap = null;
        CLogger.Level sessionDefault = defaultLogLevel;

        synchronized (lockMap) {

            if(loggerMap.containsKey(sessionId)) {

                if (loggerMap.get(sessionId).containsKey(logId)) {
                    //direct hit
                    return loggerMap.get(sessionId).get(logId);
                } else {

                    String prefix = "";
                    if(logId.contains(":")) {
                        //limit search key to location
                        prefix = logId.split(":")[0];
                    }

                    //build search map
                    searchmap = loggerMap.get(sessionId).subMap(prefix, logId);

                }

            } else {
                if(defaultLoggerMap.containsKey(sessionId)) {
                    sessionDefault = defaultLoggerMap.get(sessionId);
                }
            }

        }

        if(searchmap != null) {

            if(searchmap.size() > 0) {
                String canidateKey = searchmap.lastKey();
                if(logId.startsWith(canidateKey)) {
                    return searchmap.get(canidateKey);
                }
            }
        }

        return sessionDefault;

    }

    public void removeLogLevel(String sessionId, String logId) {
        synchronized (lockMap) {
            if(loggerMap.containsKey(sessionId)) {
                if (loggerMap.get(sessionId).containsKey(logId)) {
                    logger.info("LogDP: Removing LogId: " + logId);
                    loggerMap.get(sessionId).remove(logId);
                }
            }
        }
    }

    public boolean setLogLevel(String sessionId, String logId, CLogger.Level level) {
            boolean isSet = false;
            try {
                synchronized (lockMap) {

                    if (logId.equals("default")) {
                        logger.info("LogDP: Setting defaultLogLevel to " + level.name() + " for session: " + sessionId);
                        if (loggerEnabledMap.containsKey(sessionId)) {
                            defaultLoggerMap.put(sessionId, level);
                            isSet = true;
                        }
                    } else {
                        if (loggerEnabledMap.containsKey(sessionId)) {
                            logger.info("LogDP: Setting LogLevel to " + level.name() + " for LogId: " + logId + " for session: " + sessionId);

                            //logger.error(loggerMap.get(sessionId).getClass().toString());
                            if(!loggerMap.containsKey(sessionId)) {
                                NavigableMap<String, CLogger.Level> nm = new TreeMap<>();
                                nm.put(logId,level);
                                loggerMap.put(sessionId, nm);
                            } else {
                                loggerMap.get(sessionId).put(logId, level);
                            }
                            isSet = true;
                        } else {
                            logger.error("!loggerEnabledMap.containsKey(sessionId)");
                        }
                    }

                }
            } catch (Exception ex) {
                logger.error("setLogLevel(): " + ex.getMessage());
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                String sStackTrace = sw.toString(); // stack trace as a string
                logger.error(sStackTrace);
            }
        return isSet;
    }

}