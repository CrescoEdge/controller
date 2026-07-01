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
    // Fail-safe monitor: true only when a real SLF4J backend is bound. When the
    // backend is missing (NOP) or throws, formal logging silently drops the message
    // and we are BLIND -- so we mirror to System.err (the guaranteed last-resort sink).
    private final boolean backendActive;
    // Warn at most once per JVM that the backend is dead, so stderr is not flooded.
    private static final java.util.concurrent.atomic.AtomicBoolean BACKEND_WARNED =
            new java.util.concurrent.atomic.AtomicBoolean(false);


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

        this.backendActive = isBackendActive(logService);
        if (!backendActive && BACKEND_WARNED.compareAndSet(false, true)) {
            System.err.println("[CRESCO-LOGGING-MONITOR] No active SLF4J logging backend ("
                    + (logService == null ? "null" : logService.getClass().getName())
                    + "); mirroring all log output to System.err so nothing is lost.");
        }
    }

    // SLF4J's no-op binding (org.slf4j.helpers.NOPLogger) silently discards every call.
    // Treat only that case as dead; a real Pax/log4j2 binding (or the transient
    // SubstituteLogger during init) is considered active so we do not double-log.
    private static boolean isBackendActive(Logger l) {
        return l != null && !l.getClass().getName().startsWith("org.slf4j.helpers.NOP");
    }

    // Guaranteed last-resort sink. Used only when the formal backend is dead or throws.
    private void emitFailsafe(String logMessage, Level level, Throwable throwable) {
        System.err.println("[" + level.name().toUpperCase() + "] " + logMessage);
        if (throwable != null) {
            throwable.printStackTrace();
        }
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

    // Exception-aware overloads (replace ex.printStackTrace()): the full stack trace is logged
    // through the logging framework (log4j2 -> console + main.log), not dumped to stderr.
    public void error(String logMessage, Throwable throwable) { log(logMessage, Level.Error, throwable); }
    public void warn(String logMessage, Throwable throwable)  { log(logMessage, Level.Warn, throwable); }
    public void info(String logMessage, Throwable throwable)  { log(logMessage, Level.Info, throwable); }
    public void debug(String logMessage, Throwable throwable) { log(logMessage, Level.Debug, throwable); }
    public void trace(String logMessage, Throwable throwable) { log(logMessage, Level.Trace, throwable); }

    public void log(String messageBody, Level level, Throwable throwable) {

        String logMessage = "[" + source + ": " + baseClassName + "]"
                + "[" + formatClassName(issuingClassName) + "] " + messageBody;

        try {
            switch (level.name()) {
                case "Trace": logService.trace(logMessage, throwable); break;
                case "Debug": logService.debug(logMessage, throwable); break;
                case "Info":  logService.info(logMessage, throwable); break;
                case "Warn":  logService.warn(logMessage, throwable); break;
                default:      logService.error(logMessage, throwable); break;
            }
            if (!backendActive) emitFailsafe(logMessage, level, throwable);
        } catch (Throwable logFailure) {
            // The logging pipeline itself failed -- do not let it swallow the message or crash the caller.
            emitFailsafe(logMessage, level, throwable);
            emitFailsafe("logging backend threw: " + logFailure, Level.Error, logFailure);
        }

        // DataPlane mirroring is best-effort: it must never crash the caller or mask the real log.
        try {
            if (dataPlaneLogger != null) {
                String dpBody = (throwable != null) ? messageBody + " : " + throwable : messageBody;
                dataPlaneLogger.logToDataPlane(level, logIdent, dpBody);
            }
        } catch (Throwable dpFailure) {
            // primary sink above already handled the message; swallow dataplane failure
        }
    }

    public void log(String messageBody, Level level) {


        String logMessage = "[" + source + ": " + baseClassName + "]";
            logMessage = logMessage + "[" + formatClassName(issuingClassName) + "]";
        logMessage = logMessage + " " + messageBody;

        String levelString = level.name();

        try {
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
            if (!backendActive) emitFailsafe(logMessage, level, null);
        } catch (Throwable logFailure) {
            emitFailsafe(logMessage, level, null);
            emitFailsafe("logging backend threw: " + logFailure, Level.Error, logFailure);
        }

        try {
            if(dataPlaneLogger != null) {
                dataPlaneLogger.logToDataPlane(level, logIdent, messageBody);
            }
        } catch (Throwable dpFailure) {
            // best-effort dataplane mirror; primary sink already handled the message
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