package io.cresco.agent.core;


import io.cresco.agent.data.DataPlaneLogger;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    private DataPlaneLogger dataPlaneLogger;


    public CLoggerImpl(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName, DataPlaneLogger dataPlaneLogger) {
        this.pluginBuilder = pluginBuilder;
        this.baseClassName = baseClassName;
        this.issuingClassName = issuingClassName.substring(baseClassName.length() +1) ;
        this.dataPlaneLogger = dataPlaneLogger;

        if(pluginBuilder.getPluginID() != null) {
            source = pluginBuilder.getPluginID();
        } else {
            source = "agent";
        }

        logIdent = source  + ":" + issuingClassName;
        logIdent = logIdent.toLowerCase();

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

        if(dataPlaneLogger != null) {
            dataPlaneLogger.logToDataPlane(level, logIdent, messageBody);
        }

    }

    private String formatClassName(String className) {
        StringBuilder newName = new StringBuilder();
        int lastIndex = 0;
        int nextIndex = className.indexOf(".", lastIndex + 1);
        while (nextIndex != -1) {
            newName.append(className.substring(lastIndex, lastIndex + 1)).append(".");
            lastIndex = nextIndex + 1;
            nextIndex = className.indexOf(".", lastIndex + 1);
        }
        return newName + className.substring(lastIndex);
    }

    public void setLogLevel(Level level) {
        //this.level = level;
        pluginBuilder.setLogLevel(logIdent,level);
    }

    private String replaceBrackets(String logMessage, Object ... params) {
        int replaced = 0;
        while (logMessage.contains("{}") && replaced < params.length) {
            if(!logMessage.equals("Text Message: {}")) {
                try {
                    logMessage = logMessage.replaceFirst("\\{\\}", String.valueOf(params[replaced]));
                } catch (Exception ex) {
                    logService.error("CORE LOGGER REGEX ERROR: MESSAGE:[" + logMessage + "]");
                }
            }
            replaced++;
        }
        return logMessage;
    }
}