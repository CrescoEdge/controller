package io.cresco.agent.core;


import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.TextMessage;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Cresco logger
 * @author V.K. Cody Bumgardner
 * @author Caylin Hickey
 * @since 0.1.0
 */
public class CLoggerImpl implements CLogger {

    //private Level level;
    private String issuingClassName;
    private String baseClassName;
    private PluginBuilder pluginBuilder;
    private Logger logService;
    private String source;
    private String logIdent;
    private ConcurrentHashMap<String, CLogger.Level> loggerMap;


    public CLoggerImpl(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName, ConcurrentHashMap<String, Level> loggerMap) {
        this.pluginBuilder = pluginBuilder;
        this.baseClassName = baseClassName;
        this.issuingClassName = issuingClassName.substring(baseClassName.length() +1) ;
        this.loggerMap = loggerMap;

        if(pluginBuilder.getPluginID() != null) {
            source = pluginBuilder.getPluginID();
        } else {
            source = "agent";
        }

        logIdent = source  + ":" + issuingClassName;
        logIdent = logIdent.toLowerCase();

        //setLogLevel(level);

        logService = LoggerFactory.getLogger(logIdent);


    }


    public void error(String logMessage) {
        log(logMessage, Level.Error);
    }

    public void error(String logMessage, Object ... params) {
        error(replaceBrackets(logMessage, params));
    }

    public void warn(String logMessage) {
        log(logMessage, Level.Warn);
    }

    public void warn(String logMessage, Object ... params) {
        warn(replaceBrackets(logMessage, params));
    }

    public void info(String logMessage) {
        log(logMessage, Level.Info);
    }

    public void info(String logMessage, Object ... params) {
        info(replaceBrackets(logMessage, params));
    }

    public void debug(String logMessage) {
        log(logMessage, Level.Debug);
    }

    public void debug(String logMessage, Object ... params) {
        debug(replaceBrackets(logMessage, params));
    }

    public void trace(String logMessage) { log(logMessage, Level.Trace); }

    public void trace(String logMessage, Object ... params) {
        trace(replaceBrackets(logMessage, params));
    }



    public void log(String messageBody, Level level) {


        String logMessage = "[" + source + ": " + baseClassName + "]";
            logMessage = logMessage + "[" + formatClassName(issuingClassName) + "]";
        logMessage = logMessage + " " + messageBody;

        String levelString = level.name();

        switch (levelString) {
            case "Trace":  logService.trace(logMessage);
                break;
            case "Debug":  logService.debug(logMessage);
                break;
            case "Info":  logService.info(logMessage);
                break;
            case "Warn":  logService.warn(logMessage);
                break;
            case "Error":  logService.error(logMessage);
                break;
            default: logService.error(logMessage);
                break;
        }

        logToDataPlane(level, logMessage);

    }

    private void logToDataPlane(Level level, String message) {
        try {
            boolean logDP = false;

            if(level.getValue() <= getLogLevel().getValue()) {
                logDP = true;
            }

            if(logDP) {
                if (pluginBuilder.getAgentService().getAgentState() != null) {
                    if (pluginBuilder.getAgentService().getAgentState().isActive()) {
                        if (pluginBuilder.getAgentService().getDataPlaneService().isFaultURIActive()) {
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
        }


    }

    private String formatClassName(String className) {
        String newName = "";
        int lastIndex = 0;
        int nextIndex = className.indexOf(".", lastIndex + 1);
        while (nextIndex != -1) {
            newName = newName + className.substring(lastIndex, lastIndex + 1) + ".";
            lastIndex = nextIndex + 1;
            nextIndex = className.indexOf(".", lastIndex + 1);
        }
        return newName + className.substring(lastIndex);
    }

    public Level getLogLevel() {
        if(loggerMap.contains(logIdent)) {
            return loggerMap.get(logIdent);
        } else {
            return Level.Info;
        }
    }

    public void setLogLevel(Level level) {
        //this.level = level;
        pluginBuilder.setLogLevel(logIdent,level);
    }

    private String replaceBrackets(String logMessage, Object ... params) {
        int replaced = 0;
        while (logMessage.contains("{}") && replaced < params.length) {
            logMessage = logMessage.replaceFirst("\\{\\}", String.valueOf(params[replaced]));
            replaced++;
        }
        return logMessage;
    }
}