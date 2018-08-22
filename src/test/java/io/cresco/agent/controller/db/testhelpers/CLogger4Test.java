package io.cresco.agent.controller.db.testhelpers;
import java.util.logging.Logger;

public class CLogger4Test {

        public enum Level {

            None(-1), Error(0), Warn(1), Info(2), Debug(4), Trace(8);

            private final int level;
            Level(int level) { this.level = level; }
            public int getValue() { return level; }
        }


        private Level level;
        private String issuingClassName;
        private String baseClassName;

        private Logger logService;
        private String source="testing";

        public CLogger4Test(String baseClassName, String issuingClassName, Level level) {
            this.baseClassName = baseClassName;
            this.issuingClassName = issuingClassName;
            this.level = level;

            String logIdent = source  + ":" + issuingClassName;
            logIdent = logIdent.toLowerCase();
            this.logService = Logger.getLogger(logIdent);
            this.logService.setLevel(java.util.logging.Level.ALL);
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

        private java.util.logging.Level convertLevel(Level level) {
            java.util.logging.Level l2 = null;

        /*
FINEST  -> TRACE
FINER   -> DEBUG
FINE    -> DEBUG
CONFIG  -> INFO
INFO    -> INFO
WARNING -> WARN
SEVERE  -> ERROR
         */

            try {
                //Error(1), Warn(2), Info(3), Debug(4), Trace(4);
                String levelString = level.name();

                switch (levelString) {
                    case "Trace":  l2 = java.util.logging.Level.FINEST;
                        break;
                    case "Debug":  l2 = java.util.logging.Level.FINER;
                        break;
                    case "Info":  l2 = java.util.logging.Level.INFO;
                        break;
                    case "Warn":  l2 = java.util.logging.Level.WARNING;
                        break;
                    case "Error":  l2 = java.util.logging.Level.SEVERE;
                        break;
                    default: l2 = java.util.logging.Level.SEVERE;
                        break;
                }
            } catch(Exception ex) {
                ex.printStackTrace();
            }
            if(l2 == null) {
                l2 = java.util.logging.Level.SEVERE;
            }
            return l2;
        }

        public void log(String messageBody, Level level) {

            java.util.logging.Level l2 = convertLevel(level);

            String logMessage = "[" + source + ": " + baseClassName + "]";
            logMessage = logMessage + "[" + formatClassName(issuingClassName) + "]";
            logMessage = logMessage + " " + messageBody;

            logService.log(l2,logMessage);

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
            return level;
        }

        public void setLogLevel(Level level) {
            this.level = level;
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
