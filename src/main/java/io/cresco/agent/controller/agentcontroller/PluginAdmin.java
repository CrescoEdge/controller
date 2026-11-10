package io.cresco.agent.controller.agentcontroller;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.data.DataPlaneLogger;
import io.cresco.agent.db.ConfigRedaction;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.app.gEdge;
import io.cresco.library.app.pNode;
import io.cresco.library.core.CoreState;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import io.cresco.library.utilities.CLogger;
import org.apache.commons.io.FileUtils;
import org.osgi.framework.*;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.util.tracker.ServiceTracker;

import java.io.*;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

public class PluginAdmin {

    private Gson gson;

    private DBInterfaceImpl gdb;
    private PluginBuilder plugin;

    private int PLUGINLIMIT = 10000;
    private int TRYCOUNT = 300;

    private AgentService agentService;
    private BundleContext context;
    // ConfigurationAdmin and CoreState are consumed via ServiceTrackers so a restart of those
    // service providers does not leave us holding a stale, invalid reference.
    private ServiceTracker<ConfigurationAdmin, ConfigurationAdmin> confAdminTracker;
    private ServiceTracker<CoreState, CoreState> coreStateTracker;
    private static final long SERVICE_WAIT_MS = 30000L;
    private Map<String,Configuration> configMap;
    /**
     * The plugins this agent runs. Read with no lock on every message in (msgIn) and every send out (bundleRunsPlugin):
     * a ConcurrentHashMap, whose iteration is safe without a lock. {@link #lockPlugin} only serializes the compound
     * changes (add, stop/remove); nothing holding it calls a plugin or waits.
     */
    private final Map<String,PluginNode> pluginMap = new java.util.concurrent.ConcurrentHashMap<>();
    private final JarIntake jarIntake;
    private Map<Long,List<String>> bundleMap;
    private Map<String,List<String>> jarRepoSyncMap;

    private String agentEmbeddedJarPath = null;
    private CLogger logger;

    private AtomicBoolean lockConfig = new AtomicBoolean();
    private AtomicBoolean lockPlugin = new AtomicBoolean();
    private AtomicBoolean lockBundle = new AtomicBoolean();
    private AtomicBoolean lockJarRepoSync = new AtomicBoolean();

    private DataPlaneLogger dataPlaneLogger;

    private long lastRepoUpdate = 0;

    private Cache<String, List<pNode>> repoCache;

    public int pluginCount() {

        synchronized (lockConfig) {
            return configMap.size();
        }
    }


    public PluginAdmin(AgentService agentService, PluginBuilder plugin, AgentState agentState, DBInterfaceImpl gdb, BundleContext context, DataPlaneLogger dataPlaneLogger) {
        this.agentService = agentService;
        this.plugin = plugin;
        this.gdb = gdb;
        this.gson = new Gson();
        this.configMap = Collections.synchronizedMap(new HashMap<>());
        this.bundleMap = Collections.synchronizedMap(new HashMap<>());
        this.jarRepoSyncMap = Collections.synchronizedMap(new HashMap<>());

        this.context = context;
        this.logger = plugin.getLogger(PluginAdmin.class.getName(), CLogger.Level.Info);
        this.jarIntake = new JarIntake(plugin);

        this.dataPlaneLogger = dataPlaneLogger;

        repoCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .softValues()
                .maximumSize(1000)
                .expireAfterWrite(5, TimeUnit.SECONDS)
                .build();


        this.confAdminTracker = new ServiceTracker<>(context, ConfigurationAdmin.class, null);
        this.confAdminTracker.open();
        this.coreStateTracker = new ServiceTracker<>(context, CoreState.class, null);
        this.coreStateTracker.open();

    }

    /**
     * Current ConfigurationAdmin from the ServiceTracker, waiting briefly if it is momentarily
     * unavailable (e.g. its provider bundle is restarting). Returns null only if it never appears.
     */
    private ConfigurationAdmin getConfigurationAdmin() {
        ConfigurationAdmin ca = confAdminTracker.getService();
        if (ca == null) {
            try {
                ca = confAdminTracker.waitForService(SERVICE_WAIT_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        if (ca == null) {
            logger.error("ConfigurationAdmin service is not available");
        }
        return ca;
    }

    /** Close the service trackers; call when the controller is shutting down. */
    public void close() {
        try { if (confAdminTracker != null) confAdminTracker.close(); } catch (Exception ex) { /* ignore */ }
        try { if (coreStateTracker != null) coreStateTracker.close(); } catch (Exception ex) { /* ignore */ }
    }

    public void clearDataPlaneLogger() {
        dataPlaneLogger.shutdown();
    }

    public CoreState getCoreState() {
        try {
            CoreState coreState = coreStateTracker.getService();
            if (coreState == null) {
                coreState = coreStateTracker.waitForService(SERVICE_WAIT_MS);
            }
            if (coreState == null) {
                logger.error("CoreState service is not available");
            }
            return coreState;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public boolean logDPSetEnabled(String sessionId, boolean isEnabled) {
        boolean isSet = false;
        try {

            //set log level for DP
            if(dataPlaneLogger != null) {
                dataPlaneLogger.setIsEnabled(sessionId, isEnabled);
                isSet = true;
            }

        } catch (Exception ex) {
            logger.error("logDPSetEnabled() " + ex.getMessage(), ex);
        }
        return isSet;
    }

    public boolean logDPIsEnabled(String sessionId) {
        boolean isSet = false;
        try {

            //set log level for DP
            if(dataPlaneLogger != null) {
                isSet = dataPlaneLogger.getIsEnabled(sessionId);
            }

        } catch (Exception ex) {
            logger.error("logDPIsEnabled() " + ex.getMessage(), ex);
        }
        return isSet;
    }

    public boolean setDPLogLevel(String sessionId, String logId, CLogger.Level level) {
        boolean isSet = false;
        try {

            //set log level for DP
            if(dataPlaneLogger != null) {
                //logger.error("dataPlaneLogger != NULL");
                isSet = dataPlaneLogger.setLogLevel(sessionId, logId, level);
                if(isSet) {
                    setLogLevel(logId,level);
                }
            } else {
                logger.error("dataPlaneLogger == NULL");
            }

        } catch (Exception ex) {
            logger.error("setDPLogLevel " + ex.getMessage(), ex);
        }
        return isSet;
    }

    public boolean setRootLogLevel(CLogger.Level level) {
        boolean isSet = false;
        try {


            //set log level for file (Pax Logging 2.x / log4j2 key)
            Configuration logConfig = getConfigurationAdmin().getConfiguration("org.ops4j.pax.logging", null);

            Dictionary<String, Object> log4jProps = logConfig.getProperties();
            log4jProps.put( "log4j2.rootLogger.level", level.name().toUpperCase() );

            logConfig.updateIfDifferent(log4jProps);
            isSet = true;

            logger.info("Set RootLoglevel: " + level.name());

        } catch (Exception ex) {
            logger.error("setLogLevel() " + ex.getMessage(), ex);
        }
        return isSet;
    }

    public boolean setLogLevel(String logId, CLogger.Level level) {
        boolean isSet = false;
        try {


            //set log level for file (Pax Logging 2.x / log4j2 logger entry)
            Configuration logConfig = getConfigurationAdmin().getConfiguration("org.ops4j.pax.logging", null);

            Dictionary<String, Object> log4jProps = logConfig.getProperties();
            String id = paxLoggerId(logId);
            log4jProps.put("log4j2.logger." + id + ".name", logId);
            log4jProps.put("log4j2.logger." + id + ".level", level.name().toUpperCase());

            logConfig.updateIfDifferent(log4jProps);
            isSet = true;

            logger.info("Set loglevel: " + level.name()  + " for log_id : " + logId);

        } catch (Exception ex) {
            logger.error("setLogLevel() " + ex.getMessage(), ex);
        }
        return isSet;
    }

    public boolean removeLogLevel(String sessionId, String logId) {
        boolean isSet = false;
        try {

            //set log level for DP
            if(dataPlaneLogger != null) {
                dataPlaneLogger.removeLogLevel(sessionId, logId);
            }

            //set log level for file
            Configuration logConfig = getConfigurationAdmin().getConfiguration("org.ops4j.pax.logging", null);

            Dictionary<String, Object> log4jProps = logConfig.getProperties();

            String id = paxLoggerId(logId);
            List<String> keys = Collections.list(log4jProps.keys());
            boolean changed = false;
            if (keys.contains("log4j2.logger." + id + ".name")) {
                log4jProps.remove("log4j2.logger." + id + ".name");
                changed = true;
            }
            if (keys.contains("log4j2.logger." + id + ".level")) {
                log4jProps.remove("log4j2.logger." + id + ".level");
                changed = true;
            }
            if (changed) {
                logConfig.updateIfDifferent(log4jProps);
            }

            isSet = true;

            logger.info("Log for log_id : " + logId + " removed.");

        } catch (Exception ex) {
            logger.error("removeLogLevel " + ex.getMessage(), ex);
        }
        return isSet;
    }

    public void setLogLevels(Map<String,String> logMap) {

        try {
            for (Map.Entry<String, String> entry : logMap.entrySet()) {
                String logId = entry.getKey();
                CLogger.Level logLevel = toCLoggerLevel(entry.getValue());
                if (logLevel != null) {
                    setLogLevel(logId, logLevel);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

    }

    /** Sanitize a logger name into a valid log4j2 property-key id segment (no dots/colons). */
    private static String paxLoggerId(String logId) {
        return logId.replaceAll("[^a-zA-Z0-9]", "_");
    }

    /**
     * Map a level string to a CLogger.Level, tolerant of case and of log4j-style names
     * (so getLogLevels() output can be fed straight back into setLogLevels()).
     */
    private static CLogger.Level toCLoggerLevel(String s) {
        if (s == null) {
            return null;
        }
        for (CLogger.Level l : CLogger.Level.values()) {
            if (l.name().equalsIgnoreCase(s)) {
                return l;
            }
        }
        switch (s.trim().toUpperCase()) {
            case "OFF":
            case "FATAL":
                return CLogger.Level.None;
            case "ALL":
                return CLogger.Level.Trace;
            default:
                return CLogger.Level.Info;
        }
    }

    public Map<String,String> getLogLevels() {

        Map<String,String> logMap = null;
        try {

            logMap = new HashMap<>();

            Configuration logConfig = getConfigurationAdmin().getConfiguration("org.ops4j.pax.logging", null);

            Dictionary<String, Object> log4jProps = logConfig.getProperties();

            // log4j2 logger entries are a pair: log4j2.logger.<id>.name and log4j2.logger.<id>.level
            if (log4jProps != null) {
                Enumeration<String> e = log4jProps.keys();
                while (e.hasMoreElements()) {
                    String key = e.nextElement();
                    if (key.startsWith("log4j2.logger.") && key.endsWith(".name")) {
                        String id = key.substring("log4j2.logger.".length(), key.length() - ".name".length());
                        String name = (String) log4jProps.get(key);
                        Object level = log4jProps.get("log4j2.logger." + id + ".level");
                        if (name != null && level != null) {
                            logMap.put(name, level.toString());
                        }
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("getLogLevels() " + ex.getMessage(), ex);
        }
        return logMap;

    }

    public boolean pluginTypeActive(String pluginName) {
        boolean exists = false;
        try {
            synchronized (lockConfig) {

                Iterator it = configMap.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();

                    String pluginID = (String) pair.getKey();
                    Configuration config = (Configuration) pair.getValue();

                    if(config.getFactoryPid().equals(pluginName + ".Plugin")) {
                        return true;
                    }
                } }
        } catch (Exception ex) {
            logger.error("pluginTypeActive() ", ex);
        }
        return exists;
    }

    public boolean serviceExist(String serviceName) {
        boolean exists = false;
        try {

            ServiceReference sr = context.getServiceReference(serviceName);
            if(sr != null) {
                exists = true;
                context.ungetService(sr);
            }
        } catch(Exception ex) {
            logger.error("serviceExist() ", ex);
        }
        return exists;
    }

    public Map<String,Object> jarIsBundle(Map<String,Object> map) {
        Map<String,Object> returnMap = null;

        try {

            if(context != null) {

                Bundle[] bundleList = context.getBundles();

                for(Bundle b : bundleList) {

                    String eBundleId = String.valueOf(b.getBundleId());
                    String eName = b.getSymbolicName();
                    String eVersion = b.getVersion().toString();
                    String jarLocation = b.getLocation();

                    if(jarLocation != null) {
                        if (jarLocation.contains("!")) {
                            jarLocation = "jar:" + jarLocation;
                        }
                    }

                    String requestedName = (String) map.get("pluginname");

                    if((eName != null) && (eVersion != null)) {

                        if (map.containsKey("version")) {

                            String requestedVersion = (String) map.get("version");

                                if ((eName.equals(requestedName) && (eVersion.equals(requestedVersion)))) {

                                    returnMap = new HashMap<>(map);
                                    returnMap.put("jarstatus", "bundle");
                                    returnMap.put("bundle_id", eBundleId);
                                    if(jarLocation != null) {
                                        returnMap.put("jarfile",jarLocation);
                                    }
                                }

                        } else {
                            if (eName.equals(requestedName)) {

                                returnMap = new HashMap<>(map);
                                returnMap.put("version", eVersion);
                                returnMap.put("jarstatus", "bundle");
                                returnMap.put("bundle_id", eBundleId);
                                if(jarLocation != null) {
                                    returnMap.put("jarfile",jarLocation);
                                }
                            }
                        }

                    }

                }

            } else {
                logger.error("jarIsBundle OSGi context is NULL");
            }

        } catch (Exception ex) {
            logger.error("jarIsBundle() " + ex.getMessage(), ex);

        }
        return returnMap;
    }


    public Map<String,Object> jarIsEmbedded(Map<String,Object> map) {
        Map<String,Object> returnMap = null;
        try {

            String requestedJarPath = (String) map.get("jarfile");

            if(requestedJarPath != null) {

                if (agentEmbeddedJarPath == null) {

                    Bundle systemBundle = context.getBundle(0);
                    String basepath = new File(systemBundle.getClass().getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
                    if(basepath.contains("\\")) {
                        agentEmbeddedJarPath = "file:/" + basepath;
                        agentEmbeddedJarPath = agentEmbeddedJarPath.replace("\\","/");
                    } else {
                        agentEmbeddedJarPath = "file:" + basepath;
                    }
                }

                if (agentEmbeddedJarPath != null) {
                    String jarURLString = "jar:" + agentEmbeddedJarPath + "!/" + requestedJarPath;
                    URL inputURL = new URL(jarURLString);
                    Manifest manifest = null;

                    if (inputURL != null) {

                        try {
                            JarURLConnection conn = (JarURLConnection) inputURL.openConnection();
                            InputStream in = conn.getInputStream();

                            manifest = new JarInputStream(in).getManifest();
                            Attributes mainAttributess = manifest.getMainAttributes();
                            String eName = mainAttributess.getValue("Bundle-SymbolicName");
                            String eVersion = mainAttributess.getValue("Bundle-Version");
                            String eMD5 = plugin.getMD5(in);
                            if (in != null) {
                                in.close();
                            }

                            String requestedName = (String) map.get("pluginname");
                            if (map.containsKey("version")) {
                                String requestedVersion = (String) map.get("version");
                                String requestedMD5 = (String) map.get("md5");

                                if ((eName.equals(requestedName) && (eVersion.equals(requestedVersion)) && (eMD5.equals(requestedMD5)))) {
                                    returnMap = new HashMap<>(map);
                                    returnMap.put("jarstatus", "embedded");
                                }
                            } else {
                                if (eName.equals(requestedName)) {
                                    returnMap = new HashMap<>(map);
                                    returnMap.put("version", eVersion);
                                    returnMap.put("md5", eMD5);
                                    returnMap.put("jarstatus", "embedded");
                                }
                            }
                        }catch (Exception ex) {
                            //gobble exception
                            //ex.printStackTrace();
                        }
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("jarIsEmbedded-Agent()", ex);
        }
        return returnMap;
    }


    public Map<String,Object> jarIsAbsolutePath(Map<String,Object> map) {
        Map<String,Object> returnMap = null;
        try {


            if(map.containsKey("jarfile")) {
                //absolute file path was given
                Path checkFile = Paths.get((String) map.get("jarfile"));


                if (checkFile.toFile().isFile()) {


                    try (JarInputStream jarInputStream = new JarInputStream(new FileInputStream(checkFile.toFile()))) {

                        //Manifest manifest = new JarInputStream(new FileInputStream(checkFile.toFile())).getManifest();
                        Manifest manifest = jarInputStream.getManifest();

                        Attributes mainAttributess = manifest.getMainAttributes();
                        String aName = mainAttributess.getValue("Bundle-SymbolicName");
                        String aVersion = mainAttributess.getValue("Bundle-Version");
                        String aMD5 = plugin.getMD5((String) map.get("jarfile"));

                        String requestedName = (String) map.get("pluginname");
                        if (map.containsKey("version")) {
                            String requestedVersion = (String) map.get("version");
                            String requestedMD5 = (String) map.get("md5");
                            if ((aName.equals(requestedName) && (aVersion.equals(requestedVersion)) && (aMD5.equals(requestedMD5)))) {
                                returnMap = new HashMap<>(map);
                                returnMap.put("jarstatus", "absolutepath");
                            }
                        } else {
                            if (aName.equals(requestedName)) {
                                returnMap = new HashMap<>(map);
                                returnMap.put("version", aVersion);
                                returnMap.put("md5", aMD5);
                                returnMap.put("jarstatus", "absolutepath");
                            }
                        }

                    } catch (Exception e) {
                        logger.error("jarIsAbsolutePath()");
                    } finally {
                     //do nothing null will return
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("jarIsAbsolutePath()", ex);
        }
        return returnMap;
    }

    public Map<String,Object> getJarFromLocalCache(Map<String,Object> map)  {
        Map<String,Object> returnMap = null;
        try {

            Path repoCacheDir = getRepoCacheDir();
            if (repoCacheDir != null) {

                List<Map<String, String>> pluginList = plugin.getPluginInventory(repoCacheDir.toFile().getAbsolutePath());
                if (pluginList != null) {
                    for (Map<String, String> params : pluginList) {

                        //{pluginname=io.cresco.controller,
                        // jarfile=055e77b10431968300a81dcf14560d37,
                        // version=1.1.0.SNAPSHOT-2021-02-27T181634Z,
                        // md5=055e77b10431968300a81dcf14560d37}


                        String lName = params.get("pluginname");
                        String lVersion = params.get("version");
                        String lMD5 = params.get("md5");

                        String requestedName = (String) map.get("pluginname");
                        if(map.containsKey("version")) {
                            String requestedVersion = (String) map.get("version");
                            String requestedMD5 = (String) map.get("md5");

                            if((lName.equals(requestedName) && (lVersion.equals(requestedVersion)) && (lMD5.equals(requestedMD5)))) {
                                returnMap = new HashMap<>(map);
                                returnMap.put("jarfile",repoCacheDir.toFile().getAbsolutePath() + System.getProperty("file.separator") + params.get("jarfile"));
                                returnMap.put("jarstatus","localcache");
                            }
                        } else {

                            if(lName.equals(requestedName)) {
                                returnMap = new HashMap<>(map);
                                returnMap.put("jarfile",repoCacheDir.toFile().getAbsolutePath() + System.getProperty("file.separator") + params.get("jarfile"));
                                returnMap.put("version",lVersion);
                                returnMap.put("md5",lMD5);
                                returnMap.put("jarstatus","localcache");
                            }
                        }


                    }
                }
            }
        } catch (Exception ex) {
            logger.error("getJarFromLocalCache()", ex);
        }

        return returnMap;
    }

    public boolean getJarFromRepo(Map<String,Object> map)  {
        boolean isFound = false;
        try {

            pNode node = getPnode(map);
            if(node != null) {
                Path jarPath = getPlugin(node);
                if(jarPath != null) {
                    if (jarPath.toFile().isFile()) {
                        isFound = true;
                    } else {
                        logger.error("pnode ! file");
                    }
                } else {
                    logger.error("Unable to retreve pnode from repo!");
                }
            } else {
                logger.error("Unable to find pnode in repo(s)!");
            }

        } catch (Exception ex) {
            logger.error("getJarFromRepo()", ex);
        }

        return isFound;
    }

    public Map<String,Object> validatePluginMap(Map<String,Object> map) {
        Map<String,Object> validatedMap = null;
        try {

            //check if plugin exist locally
            validatedMap = localPluginMap(map);
            if(validatedMap != null) {
                return validatedMap;
            } else {
                //plugin does not exist, we should try and download it
                validatedMap = remotePluginMap(map);
            }


        } catch(Exception ex) {
            logger.error("validatePluginMap()", ex);
        }
        return validatedMap;
    }

    public Map<String,Object> localPluginMap(Map<String,Object> map) {
        Map<String,Object> validatedMap = null;
        try {

            //see if config is currently running
            validatedMap = jarIsBundle(map);

            //if explicitly defined use first
            if(validatedMap != null) {
                return validatedMap;
            } else {
                validatedMap = jarIsAbsolutePath(map);
            }

            if(validatedMap != null) {
                return validatedMap;
            } else {
                validatedMap = getJarFromLocalCache(map);
            }

            if(validatedMap != null) {
                return validatedMap;
            } else {
                validatedMap = jarIsEmbedded(map);
            }

        } catch(Exception ex) {
            logger.error("localPluginMap()", ex);
        }
        return validatedMap;
    }

    public Map<String,Object> remotePluginMap(Map<String,Object> map) {
        Map<String,Object> validatedMap = null;
        try {

            String requestedName = (String) map.get("pluginname");
            String requestedMD5 = (String) map.get("md5");
            if (requestedMD5 == null) {
                requestedMD5 = "null";
            }

            boolean repoSyncActive = false;

            synchronized (lockJarRepoSync) {
                if (jarRepoSyncMap.containsKey(requestedName)) {
                    if (jarRepoSyncMap.get(requestedName).contains(requestedMD5)) {
                        repoSyncActive = true;
                    } else {
                        jarRepoSyncMap.get(requestedName).add(requestedMD5);
                        logger.debug("SET LOCK ON EXISTING PLUGIN NAME: " + requestedName + " MD5: " + requestedMD5);
                    }
                } else {
                    jarRepoSyncMap.put(requestedName, new ArrayList<>());
                    jarRepoSyncMap.get(requestedName).add(requestedMD5);
                    logger.debug("SET LOCK ON NEW PLUGIN NAME: " + requestedName + " MD5: " + requestedMD5);
                }
            }

            //check if download is in progress
            while (repoSyncActive) {

                synchronized (lockJarRepoSync) {
                    if (jarRepoSyncMap.containsKey(requestedName)) {
                        if (jarRepoSyncMap.get(requestedName).contains(requestedMD5)) {
                            logger.info("Waiting on repoSync to complete for pluginName: " + requestedName + " MD5: " + requestedMD5);
                        } else {
                            repoSyncActive = false;
                        }
                    } else {
                        repoSyncActive = false;
                    }
                }
                Thread.sleep(1000);
            }

            validatedMap = getJarFromLocalCache(map);
            if(validatedMap == null) {
                if (getJarFromRepo(map)) {
                    validatedMap = getJarFromLocalCache(map);
                }
            }

            synchronized (lockJarRepoSync) {
                if (jarRepoSyncMap.containsKey(requestedName)) {
                    jarRepoSyncMap.get(requestedName).remove(requestedMD5);
                    if(jarRepoSyncMap.get(requestedName).size() == 0) {
                        jarRepoSyncMap.remove(requestedName);
                    }
                }
            }



        } catch(Exception ex) {
            logger.error("remotePluginMap()", ex);
        }
        return validatedMap;
    }

    public long addBundle(Map<String,Object> map) {
        long bundleID = -1;
        try {

            Bundle bundle = null;
            String jarStatus = (String)map.get("jarstatus");

            switch(jarStatus)
            {
                case "embedded":

                    String requestedJarPath = (String) map.get("jarfile");
                    String jarURLString = "jar:" + agentEmbeddedJarPath + "!/" + requestedJarPath;
                    URL inputURL = new URL(jarURLString);

                    if (inputURL != null) {
                        JarURLConnection conn = (JarURLConnection)inputURL.openConnection();
                        InputStream in = conn.getInputStream();
                        String bundlePath = inputURL.getPath();
                        bundle = context.installBundle(bundlePath, in);
                        installedHere(bundle);
                        if (in != null) {
                            in.close();
                        }
                    }


                    break;
                case "absolutepath":

                    bundle = context.getBundle((String) map.get("jarfile"));

                    if (bundle == null) {
                        bundle = installVerified(Paths.get((String) map.get("jarfile")), "file:" + map.get("jarfile"));
                    }

                    break;

                case "localcache":
                    Path jarPath = Paths.get((String)map.get("jarfile"));
                    if(jarPath.toFile().isFile()) {
                        bundle = context.getBundle(jarPath.toFile().getAbsolutePath());

                        if (bundle == null) {
                            bundle = installVerified(jarPath, "file:" + jarPath.toFile().getAbsolutePath());
                        }
                    }
                    break;


                case "bundle":

                    if(map.containsKey("bundle_id")) {
                        bundleID = Long.parseLong((String)map.get("bundle_id"));

                    } else {
                        logger.error("addBundle() Missing Bundle Id");
                    }

                    break;

                default:
                    logger.error("addBundle: Invalid Jar Status");
            }

            if(bundle != null) {
                bundleID = bundle.getBundleId();
            }



        } catch(Exception ex) {
            logger.error("addBundle()", ex);
        }
        return bundleID;
    }

    /**
     * Install the jar at {@code jar} (not embedded in the agent) from a private copy that verifies ({@link JarIntake});
     * null (logged) when it does not.
     */
    private Bundle installVerified(Path jar, String location) throws Exception {
        Path copy;
        try {
            copy = jarIntake.verifiedCopy(jar);
        } catch (io.cresco.library.security.JarSignatures.Rejected rejected) {
            logger.error("addBundle: REFUSED to install " + jar + ": " + rejected.getMessage());
            return null;
        }
        try (InputStream in = Files.newInputStream(copy)) {
            return installedHere(context.installBundle(location, in));
        } finally {
            try { Files.deleteIfExists(copy); } catch (Exception ignore) { }
        }
    }

    /** Verifies every jar that did not come embedded in the agent before it is cached or installed. */
    public JarIntake getJarIntake() { return jarIntake; }

    public boolean startBundle(long bundleID, String pid) {
        boolean isStarted = false;
        try {
            //context.getBundle(bundleID).start();
            Bundle b = context.getBundle(bundleID);

            if(b.getState() != 32) {
                b.start();
            }

            if(b.getState() == 32) {
                isStarted = true;
            } else {
                logger.error("Bundle: " + bundleID + " pluginname: " + pid + " state:" + b.getState());
            }


        } catch(Exception ex) {
            logger.error("startBundle() ", ex);
        }
        return  isStarted;
    }

    public boolean stopBundle(long bundleID) {
        boolean isStopped = false;
        try {
            context.getBundle(bundleID).stop();
            isStopped = true;
        } catch(Exception ex) {
            logger.error("stopBundle() ", ex);
        }
        return  isStopped;
    }

    /** Bundles this controller installed itself: embedded in the agent jar, or from a copy that verified (signed). */
    private final java.util.Set<Long> installedHere = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private Bundle installedHere(Bundle b) {
        if (b != null) installedHere.add(b.getBundleId());
        return b;
    }

    /**
     * Did this controller install bundle {@code bundleId} itself (from the agent jar or a verified signed copy)? A bundle a
     * plugin installed through its own BundleContext did not -- whatever symbolic name it chose (review F5): only such
     * bundles hold a capability bound to a name (on_behalf_bundles, the executor's stdin).
     */
    public boolean installedByController(long bundleId) {
        return installedHere.contains(bundleId);
    }

    public boolean removeBundle(long bundleID) {
        boolean isRemoved = false;
        try {
            context.getBundle(bundleID).uninstall();
            isRemoved = true;
        } catch(Exception ex) {
            logger.error("removeBundle() ", ex);
        }
        return  isRemoved;
    }

    /**
     * {@code plugin_data_retention_days} (default 0: never): delete the data directory of a plugin that is no longer
     * configured on this agent (not running, no inventory row) once the directory has not changed for that many days.
     * Never on a stop. Returns the directories deleted.
     */
    public List<String> sweepStalePluginData(long retentionDays) {
        List<String> deleted = new ArrayList<>();
        if (retentionDays <= 0) return deleted;
        File base = new File(agentService.getAgentDataDirectory() + "/plugin-data");
        File[] dirs = base.listFiles(File::isDirectory);
        if (dirs == null) return deleted;
        long cutoff = System.currentTimeMillis() - retentionDays * 24L * 3600 * 1000;
        for (File d : dirs) {
            String id = d.getName();
            try {
                boolean configured = pluginMap.containsKey(id);
                if (configured || pendingOwners.containsKey(id) || gdb.nodeExist(plugin.getRegion(), plugin.getAgent(), id)) continue;
                if (newestChange(d.toPath()) > cutoff) continue;
                String refused = PluginIds.purge(base.toPath(), id);
                if (refused != null) {
                    logger.error("plugin-data sweep: NOT deleting " + d + ": " + refused);
                    continue;
                }
                deleted.add(id);
                logger.warn("Deleted the plugin-data of " + id + ": no longer configured here and unchanged for "
                        + retentionDays + " days (plugin_data_retention_days)");
            } catch (Exception ex) {
                logger.error("sweepStalePluginData " + id + ": " + ex.getMessage());
            }
        }
        return deleted;
    }

    private static long newestChange(java.nio.file.Path dir) throws java.io.IOException {
        try (java.util.stream.Stream<java.nio.file.Path> all = Files.walk(dir)) {
            return all.mapToLong(p -> {
                try { return Files.getLastModifiedTime(p, java.nio.file.LinkOption.NOFOLLOW_LINKS).toMillis(); }
                catch (java.io.IOException e) { return Long.MAX_VALUE; }
            }).max().orElse(Long.MAX_VALUE);
        }
    }

    public void stopAllPlugins() {

        Set<String> keys = new HashSet<>(pluginMap.keySet());

        for(String pid : keys) {
            stopPlugin(pid);
        }

    }

    /**
     * Stop a plugin for an agent SHUTDOWN: persistent plugins (persistence_code &gt; 9: pipeline-placed 10,
     * system 20) keep their pnode row so StaticPluginLoader restarts them under the same id.
     */
    public boolean stopPlugin(String pluginId) {
        return stopPlugin(pluginId, false);
    }

    /**
     * Explicitly REMOVE a plugin (pluginremove): stop it and drop its pnode row whatever its persistence
     * code. The local DB is the inventory this agent exports upstream (stateUpdate pluginconfigs) and, on
     * the global's own agent, the very table the global's listplugins reads -- keeping the row of a
     * removed persistent plugin (e.g. the system stunnel, code 20) left it listed fabric-wide forever, so
     * clients' deploy_plugin believed it was still loaded, and a removed code-10 plugin was resurrected
     * on the next agent restart. The plugin-data of a persistent plugin is kept (identity-keyed state).
     */
    public boolean removePlugin(String pluginId) {
        return removePlugin(pluginId, false);
    }

    /**
     * {@link #removePlugin(String)}; {@code purgeData}: also delete the plugin's data directory
     * ({@code plugin-data/<id>}). Only an explicit pluginremove with {@code purge_data=true} from an admin purges; a
     * plugin's data otherwise outlives its removal (and every stop) -- a plugin's data may be the only copy of what it
     * holds (a file repository).
     */
    public boolean removePlugin(String pluginId, boolean purgeData) {
        return stopPlugin(pluginId, true, purgeData);
    }

    private boolean stopPlugin(String pluginId, boolean isRemove) {
        return stopPlugin(pluginId, isRemove, false);
    }

    /** The data directory of plugin {@code pluginId}. */
    File pluginDataDirectory(String pluginId) {
        return new File(agentService.getAgentDataDirectory() + "/plugin-data/" + pluginId);
    }

    /** The stops under way: plugin id -> completed when that stop has ended. */
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<Void>> stopsUnderWay =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * One stop of a plugin at a time: a second stop of the same plugin (an agent shutdown and a pluginremove) begins when
     * the one under way has ended, and finds what it left (a plugin no longer running: a remove still drops its row).
     */
    private boolean stopPlugin(String pluginId, boolean isRemove, boolean purgeData) {
        if (pluginId == null) return stopPluginOnce(null, isRemove, purgeData);
        while (true) {
            java.util.concurrent.CompletableFuture<Void> mine = new java.util.concurrent.CompletableFuture<>();
            java.util.concurrent.CompletableFuture<Void> other = stopsUnderWay.putIfAbsent(pluginId, mine);
            if (other == null) {
                try {
                    return stopPluginOnce(pluginId, isRemove, purgeData);
                } finally {
                    stopsUnderWay.remove(pluginId, mine);
                    mine.complete(null);
                }
            }
            other.join();
        }
    }

    private boolean stopPluginOnce(String pluginId, boolean isRemove, boolean purgeData) {
        boolean isStopped = false;
        try {

            logger.debug("stopPlugin: " + pluginId);

            String jarFilePath = null;
            String pid = null;
            boolean isPluginStopped = false;
            long bundleID = -1;

            PluginNode stopping = pluginId == null ? null : pluginMap.get(pluginId);
            boolean isKnown = stopping != null;
            PluginService service = null;
            if (isKnown) {
                jarFilePath = stopping.getJarPath();
                service = stopping.getPluginService();
                bundleID = stopping.getBundleID();
                // the plugin's own stop runs under no lock: a stop may send (bundleRunsPlugin) and wait for replies
                // (msgIn), and the agent goes on answering (pluginlist) and delivering to its other plugins meanwhile.
                // Until it is removed below, the stopping plugin is still in the map: it sends as itself.
                // a plugin whose start never completed has no service: there is nothing of its own to stop, and it must
                // still be removable (it used to fail here on a null, so it could be neither removed nor added again)
                isPluginStopped = service == null || service.isStopped();
            }
            if (!isKnown) {
                logger.error("stopPlugin() no key found for pluginid: " + pluginId);
                if (isRemove) {
                    // not running here, but a leftover row (e.g. a plugin that failed to start)
                    // would otherwise stay in the exported inventory after a "successful" remove
                    gdb.removeNode(plugin.getRegion(), plugin.getAgent(), pluginId);
                }
                //not found return true
                return true;
            }

            logger.debug("stopPlugin jarfilePath: " + jarFilePath);
            logger.debug("stopPlugin ispluginstopped: " + isPluginStopped);
            logger.debug("stopPlugin bundleId: " + bundleID);

            synchronized (lockConfig) {
                pid = configMap.get(pluginId).getPid();
                logger.debug("stopPlugin pid: " + pid);
            }

            if(isPluginStopped) {

                //stop bundle if only active plugin using it
                boolean stopBundle = false;
                synchronized (lockBundle) {
                    if(bundleMap.containsKey(bundleID)) {
                        logger.debug("stopPlugin bundleMap contains bundleID: " + bundleID);
                        logger.debug("stopPlugin removing plugin: " + pluginId + " from bundleMap: " + bundleID);
                        bundleMap.get(bundleID).remove(pluginId);
                        logger.debug("stopPlugin bundleMapSize: " + bundleMap.get(bundleID).size());
                        if(bundleMap.get(bundleID).size() == 0) {
                            logger.debug("stopPlugin removing bundle: " + bundleID + " from bundleMap");
                            bundleMap.remove(bundleID);
                            stopBundle = true;
                        }
                    }
                }

                logger.debug("bundleID: " + bundleID + " isStopped: " + stopBundle);


                if(stopBundle) {
                    logger.debug("stopping bundleid: " + bundleID);
                    stopBundle(bundleID);
                    logger.debug("removing bundleid:" + bundleID);
                    removeBundle(bundleID);
                }



                if (pid != null) {

                    Configuration pluginConfig = getConfigurationAdmin().getConfiguration(pid);
                    if(pluginConfig != null) {

                        pluginConfig.delete();

                        synchronized (lockPlugin) {
                            pluginMap.remove(pluginId, stopping);
                        }
                        // a delivery that looked the plugin up before its removal and has not handed the message over
                        // yet finds it inactive; from here on, its id is no longer owned (bundleRunsPlugin)
                        stopping.deactivate();
                        synchronized (lockConfig) {
                            configMap.remove(pluginId);
                        }

                        int pluginPersistenceCode = gdb.getPNodePersistenceCode(pluginId);

                        try {
                            if (isRemove && pluginPersistenceCode > 9) {
                                // explicit remove of a persistent plugin: drop it from the inventory
                                // (and from restart-on-boot), keep its plugin-data
                                gdb.removeNode(plugin.getRegion(), plugin.getAgent(), pluginId);
                            } else if (pluginPersistenceCode <= 9) {

                                //remove from database
                                gdb.removeNode(plugin.getRegion(), plugin.getAgent(), pluginId);
                            }
                            // a plugin's data is never deleted on a stop (a graceful agent shutdown stops every plugin)
                            // nor on a remove without an explicit purge: it may be the only copy of what the plugin held
                            File folder = pluginDataDirectory(pluginId);
                            if (isRemove && purgeData) {
                                // only a valid id, only a real directory strictly inside plugin-data, never through a link
                                String refused = PluginIds.purge(new File(agentService.getAgentDataDirectory(), "plugin-data").toPath(), pluginId);
                                if (refused == null) {
                                    logger.warn("PURGED the plugin-data of removed plugin " + pluginId);
                                } else {
                                    logger.error("NOT purging plugin-data: " + refused);
                                }
                            } else if (folder.isDirectory()) {
                                if (isRemove) {
                                    logger.info("Plugin " + pluginId + " removed; its plugin-data is kept at " + folder.getAbsolutePath()
                                            + " (pluginremove with purge_data=true from an admin deletes it)");
                                }
                            }
                        } catch (Exception ex) {
                            logger.error("stopPlugin DB ERROR ON REMOVE " +  ex.getMessage());
                        }

                        isStopped = true;
                    }
                } else {

                    logger.error("pid is NULL");

                    PluginNode n = pluginMap.get(pluginId);
                    if (n != null) logger.debug(n.toString());

                }
            } else {
                logger.error("stopPlugin() could not stop plugin!");
            }

        } catch(Exception ex) {
            logger.error("stopPlugin() ", ex);
        }
        return  isStopped;
    }

    /*
    public boolean stopBundle(String pluginId) {
        boolean isStopped = false;
        try {

            String jarFilePath = null;
            String pid = null;
            boolean isPluginStopped = false;

            synchronized (lockPlugin) {
                if (pluginMap.containsKey(pluginId)) {
                    jarFilePath = pluginMap.get(pluginId).getJarPath();
                    isPluginStopped = pluginMap.get(pluginId).getPluginService().isStopped();
                }
            }

            synchronized (lockConfig) {
                pid = configMap.get(pluginId).getPid();
            }

            System.out.println("jarfilepath:" + jarFilePath + " factorypid:" + pid + " ispluginstopped:" + isPluginStopped);

            if(isPluginStopped) {

                if ((jarFilePath != null) && (pid != null)) {

                    Configuration pluginConfig = getConfigurationAdmin().getConfiguration(pid);
                    if(pluginConfig != null) {

                        pluginConfig.delete();

                        long bundleID = addBundle(jarFilePath);
                        System.out.println("bundleID " + bundleID);
                        if (bundleID != -1) {
                            System.out.println("bundleID pre-stop state " + context.getBundle(bundleID).getState());
                            context.getBundle(bundleID).stop();
                            //context.getBundle(bundleID).uninstall();
                            System.out.println("bundleID pre-stop state " + context.getBundle(bundleID).getState());
                            context.getBundle(bundleID).uninstall();

                            synchronized (lockPlugin) {
                                pluginMap.remove(pluginId);
                            }
                            synchronized (lockConfig) {
                                configMap.remove(pluginId);
                            }

                            isStopped = true;
                        }
                    }
                }
            }

        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return  isStopped;
    }
*/

    /**
     * Does the bundle {@code bundleId} run the plugin {@code pluginId}? The controller sends a message as a plugin only for
     * the bundle that runs it: one bundle cannot speak as another bundle's plugin.
     */
    public boolean bundleRunsPlugin(long bundleId, String pluginId) {
        if (pluginId == null) return false;
        Long pending = pendingOwners.get(pluginId);
        if (pending != null && pending == bundleId) return true;
        // no lock: one read of the plugin map, on every send of every plugin
        PluginNode n = pluginMap.get(pluginId);
        return n != null && n.getBundleID() == bundleId;
    }

    /** Plugins being added: id -> bundle, from before their component can activate until they are in pluginMap. */
    private final Map<String, Long> pendingOwners = new java.util.concurrent.ConcurrentHashMap<>();

    /** The plugin ids the bundle {@code bundleId} runs, sorted. */
    public java.util.SortedSet<String> pluginIdsOf(long bundleId) {
        java.util.SortedSet<String> ids = new java.util.TreeSet<>();
        for (Map.Entry<String, PluginNode> e : pluginMap.entrySet()) {
            if (e.getValue().getBundleID() == bundleId) ids.add(e.getKey());
        }
        return ids;
    }

    /**
     * Deliver {@code msg} to its plugin. No lock: the plugin's inMsg may run its handler right here (its bounded queue is
     * full: CallerRuns) and that handler may send (bundleRunsPlugin) and wait for a reply that another thread delivers
     * here; one plugin that is slow to take a message holds up no other plugin's messages or sends. A plugin not (or no
     * longer) in the map gets nothing; one that is not active (not started yet, or being removed) gets nothing (logged).
     */
    public void msgIn(MsgEvent msg) {

        String pluginID = msg.getDstPlugin();
        PluginNode node = pluginID == null ? null : pluginMap.get(pluginID);
        if (node == null) return;
        PluginService service = node.getActive() ? node.getPluginService() : null;
        if (service != null) {
            service.inMsg(msg);
        } else {
            logger.error("MESSAGE SENT TO INACTIVE PLUGIN");
        }
    }

    public String addPlugin(Map<String,Object> map) {
        return addPlugin(map, null);
    }

    public String addPlugin(Map<String,Object> incomingMap, String edges) {

        String returnPluginID = null;
        if(pluginCount() < PLUGINLIMIT) {
            try {

                String pluginID = (String)incomingMap.get("inode_id");

                if(pluginID == null) {
                    pluginID = "plugin-" + UUID.randomUUID().toString();
                }
                // the id names a directory (plugin-data/<id>) and a registry row: one plain segment only (review F6)
                String badId = PluginIds.refusal(pluginID);
                if (badId != null) {
                    logger.error("addPlugin REFUSED: " + badId);
                    return null;
                }

                String pluginName = (String)incomingMap.get("pluginname");

                Map<String,Object> map = validatePluginMap(incomingMap);

                if(map != null) {

                    logger.debug("Incoming plugin map: " + map);

                    long bundleID = addBundle(map);
                    if (bundleID != -1) {

                        if (edges != null) {
                            map.put("edges", edges);
                        }

                        //String pluginID = addConfig(pluginName, map);
                        // the plugin may send as soon as its component activates (addConfig/startBundle), before it is in
                        // pluginMap: it already speaks as pluginID of this bundle (bundleRunsPlugin)
                        pendingOwners.put(pluginID, bundleID);
                        try {
                        if (addConfig(pluginID, map)) {

                            if (startBundle(bundleID, (String)map.get("pluginname"))) {

                                    PluginNode pluginNode = null;
                                    if (edges != null) {
                                        Type type = new TypeToken<List<gEdge>>() {
                                        }.getType();
                                        List<gEdge> edgeList = gson.fromJson(edges, type);

                                        pluginNode = new PluginNode(plugin, gdb, bundleID, pluginID, map, edgeList);
                                    } else {
                                        pluginNode = new PluginNode(plugin, gdb, bundleID, pluginID, map, null);
                                    }

                                    logger.debug("pluginNode Created: " + pluginNode);

                                    synchronized (lockPlugin) {
                                        pluginMap.put(pluginID, pluginNode);
                                    }
                                    synchronized (lockBundle) {
                                        if (!bundleMap.containsKey(bundleID)) {
                                            bundleMap.put(bundleID, new ArrayList<>());
                                        }
                                        bundleMap.get(bundleID).add(pluginID);
                                    }


                                    if (startPlugin(pluginID)) {
                                        returnPluginID = pluginID;
                                    } else {
                                        logger.error("Could not start agentcontroller " + pluginID + " pluginName " + map.get("pluginname") + " no bundle " + map.get("jarfile"));
                                    }

                            } else {
                                logger.error("Could not start bundle Id " + bundleID + " pluginName " + map.get("pluginname") + " no bundle " + map.get("jarfile"));
                                // the configuration made for it goes with it: left behind, the id could never be added again
                                removeConfig(pluginID);
                            }
                        } else {
                            logger.info("Could not create config pluginName " + map.get("pluginname") + " for jar " + map.get("jarfile"));
                        }
                        } finally {
                            pendingOwners.remove(pluginID);
                        }
                        //controllerEngine.getPluginAdmin().startBundle(bundleID);
                        //String pluginID = controllerEngine.getPluginAdmin().addConfig(pluginName,jarFile, map);
                        //controllerEngine.getPluginAdmin().startPlugin(pluginID);
                    } else {
                        logger.error("Can't add " + map.get("pluginname") + " no bundle " + map.get("jarfile"));
                    }
                } else {
                    logger.error("Can't add " + pluginName + " could not find suitable jar for bundle loading!");
                    logger.error(pluginName + " : " + io.cresco.agent.controller.communication.ParamLog.names(incomingMap));
                }

            } catch (Exception ex) {
            logger.error("addPlugin() ", ex);
            }
        } else {
            logger.error("PLUGIN LIMIT REACHED! PLUGIN COUNT= " + pluginCount());
        }
        return returnPluginID;
    }

    public void addDirectConfig(String factoryPid, Dictionary<String, Object> properties) {

        try {
            Configuration configuration = getConfigurationAdmin().createFactoryConfiguration(factoryPid, null);
            configuration.update(properties);

        } catch(Exception ex) {
            logger.error("addDirectConfig() ", ex);
        }

    }

    /** Delete the configuration of {@code pluginId}, if this agent holds one (an add that did not come to a plugin). */
    private void removeConfig(String pluginId) {
        Configuration configuration;
        synchronized (lockConfig) {
            configuration = configMap.remove(pluginId);
        }
        if (configuration != null) {
            try {
                configuration.delete();
            } catch (Exception ex) {
                logger.error("removeConfig() " + pluginId + ": " + ex.getMessage());
            }
        }
    }

    public boolean addConfig(String pluginId, Map<String,Object> map) {
        boolean isAdded = false;
        try {

            // a plugin id is configured once: a second add of an id this agent already holds (a client's retry after
            // its timeout, a pipeline resubmitted, a retry after a failed start) is refused. It used to wait here for
            // the id to become free in a loop with no sleep and no end: one thread at 100% CPU for good, for each.
            // An add that follows a stop of the same id (a restart) waits for that stop, a bounded time.
            java.util.concurrent.CompletableFuture<Void> stop = stopsUnderWay.get(pluginId);
            if (stop != null) {
                try {
                    stop.get(60, java.util.concurrent.TimeUnit.SECONDS);
                } catch (java.util.concurrent.TimeoutException stillStopping) {
                    logger.error("addConfig REFUSED: plugin id " + pluginId + " is still being stopped after 60 s");
                    return false;
                }
            }
            {
                synchronized (lockConfig) {
                    if (configMap.containsKey(pluginId)) {
                        logger.error("addConfig REFUSED: plugin id " + pluginId + " is already configured on this agent"
                                + " (remove it first, or add under another id)");
                    } else {

                        String pid = map.get("pluginname") + ".Plugin";
                        String bsn = (String)map.get("pluginname");
                        String version = (String)map.get("version");

                        String configString = pid + "|" + bsn + "|" + version;


                        //Configuration configuration = getConfigurationAdmin().createFactoryConfiguration(configString, null);

                        Configuration configuration = getConfigurationAdmin().createFactoryConfiguration(map.get("pluginname") + ".Plugin", null);


                        Dictionary properties = new Hashtable();

                        ((Hashtable) properties).putAll(map);
                        properties.put("pluginID", pluginId);
                        //properties.put("service.pid2",configString2);
                        configuration.update(properties);

                        configMap.put(pluginId, configuration);
                        isAdded = true;
                    }
                }



            }


        } catch(Exception ex) {
            logger.error("addConfig() ", ex);
        }

        return isAdded;
    }

    public boolean startPlugin(String pluginID) {
        boolean isStarted = false;

        try {
            ServiceReference<?>[] servRefs = null;
            int count = 0;

            while ((!isStarted) && (count < TRYCOUNT)) {

                String filterString = "(pluginID=" + pluginID + ")";
                Filter filter = context.createFilter(filterString);

                //servRefs = context.getServiceReferences(PluginService.class.getName(), filterString);
                servRefs = context.getServiceReferences(PluginService.class.getName(), filterString);

                //System.out.println("REFS : " + servRefs.length);
                if (servRefs == null || servRefs.length == 0) {

                    //System.out.println("NULL FOUND NOTHING!");
                    logger.debug("No service reference found for pluginID=" + pluginID);

                } else {
                    //System.out.println("Running Service Count: " + servRefs.length);

                    for (ServiceReference sr : servRefs) {

                        boolean assign = sr.isAssignableTo(context.getBundle(), PluginService.class.getName());

                        if(assign) {
                            PluginService ps = (PluginService) context.getService(sr);
                            int statusCode = 7;
                            String statusDesc = "Plugin instance could not be started";
                            try {
                                // the service can be gone between the lookup and here (a stop racing the add): ask again
                                // a few times, never for ever (this loop never re-read it, and never ended)
                                for (int tries = 0; ps == null && tries < TRYCOUNT; tries++) {
                                    logger.warn("startPlugin: the service of plugin " + pluginID + " is not there yet (" + (tries + 1) + "/" + TRYCOUNT + ")");
                                    Thread.sleep(1000);
                                    ps = (PluginService) context.getService(sr);
                                }
                                if (ps == null) throw new IllegalStateException("the service of plugin " + pluginID + " is gone");

                                if(ps.isStarted()) {
                                    statusCode = 10;
                                    statusDesc = "Plugin Active";
                                }
                            } catch(Exception ex) {
                                logger.error("Could not start!", ex);
                            }

                            // no lock: the service lookup and setIsActive are the plugin's code, setStatus writes the DB
                            PluginNode starting = pluginMap.get(pluginID);
                            if (starting != null) {
                                PluginService pluginService = (PluginService) context.getService(sr);
                                starting.setPluginService(pluginService);
                                starting.setStatus(statusCode, statusDesc);
                                pluginService.setIsActive(true);
                            } else {
                                logger.error("NO PLUGIN IN PLUGIN MAP FOR THIS SERVICE : " + pluginID + " thread:" + Thread.currentThread().getName());
                            }

                            isStarted = true;
                        }
                    }
                }
                count++;
                Thread.sleep(100);
            }
            if(servRefs == null) {
                logger.error("startPlugin : COULD NOT START PLUGIN COULD NOT GET SERVICE");
            }
            if(!isStarted) {
                logger.error("startPlugin : Start plugin timeout");
            }
        } catch (Exception ex) {
            logger.error("startPlugin() ", ex);
        }
        return isStarted;
    }

    public boolean pluginExist(String pluginID) {
        boolean pluginExist = false;

        try {

            pluginExist = pluginID != null && pluginMap.containsKey(pluginID);

        } catch(Exception ex) {
            logger.error("pluginExist() ", ex);
        }


        return pluginExist;
    }

    public Map<String,String> getPluginStatus(String pluginID) {
        Map<String,String> statusMap = null;
        try {

            PluginNode pluginNode = pluginID == null ? null : pluginMap.get(pluginID);
            if (pluginNode != null) {
                statusMap = new HashMap<>();
                statusMap.put("status_code", String.valueOf(pluginNode.getStatus_code()));
                statusMap.put("status_desc", pluginNode.getStatus_desc());
                statusMap.put("isactive", String.valueOf(pluginNode.getActive()));
            }

        } catch(Exception ex) {
            logger.error("getPluginStatus() ", ex);
        }
        return statusMap;
    }

    /**
     * Snapshot of in-memory plugin health: pluginID -&gt; status_code (see {@link PluginNode}).
     * Read from the live pluginMap (no lock); feeds the "plugins" HealthCheck via
     * {@code StatusAdapter.fromPluginStatusCode}.
     */
    public Map<String,Integer> getPluginStatusCodes() {
        Map<String,Integer> out = new HashMap<>();
        try {
            for (Map.Entry<String,PluginNode> e : pluginMap.entrySet()) {
                out.put(e.getKey(), e.getValue().getStatus_code());
            }
        } catch (Exception ex) {
            logger.error("getPluginStatusCodes() ", ex);
        }
        return out;
    }

    /**
     * Snapshot of pluginID -&gt; friendly plugin name ({@link PluginNode#getName()}, e.g.
     * {@code io.cresco.wsapi}). Companion to {@link #getPluginStatusCodes()} so the "plugins"
     * HealthCheck can label plugins by name instead of by opaque pluginID.
     */
    public Map<String,String> getPluginNames() {
        Map<String,String> out = new HashMap<>();
        try {
            for (Map.Entry<String,PluginNode> e : pluginMap.entrySet()) {
                out.put(e.getKey(), e.getValue().getName());
            }
        } catch (Exception ex) {
            logger.error("getPluginNames() ", ex);
        }
        return out;
    }

    /**
     * The pNode rows of {@code pluginIds} as a JSON list, each with its configparams redacted: these
     * replies (pluginlist) leave the agent, and the rows hold every plugin's full config (OUT-03).
     */
    static String redactedRows(Iterable<String> pluginIds, java.util.function.Function<String, Map<String,String>> pNodeOf) {
        List<Map<String,String>> configMapList = new ArrayList<>();
        for (String pluginID : pluginIds) {
            Map<String,String> pNode = pNodeOf.apply(pluginID);
            if (pNode != null) {
                configMapList.add(ConfigRedaction.redactNode(pNode));
            }
        }
        return new Gson().toJson(configMapList);
    }

    public String getPluginList() {


        String exportString = null;
        try {
            List<String> pluginIds = new ArrayList<>(pluginMap.keySet());
            exportString = redactedRows(pluginIds, gdb::getPNode);

        } catch(Exception ex) {
            logger.error("PluginExport.pluginExport() Error " + ex.getMessage());
        }

        return exportString;
    }

    public String getPluginExport() {


        String exportString = null;
        try {
            List<String> pluginIds = new ArrayList<>(pluginMap.keySet());
            exportString = redactedRows(pluginIds, gdb::getPNode);

        } catch(Exception ex) {
            logger.error("PluginExport.pluginExport() Error " + ex.getMessage());
        }

        return exportString;
    }

    /*
    public String getPluginExport() {


        String exportString = null;
        try {

            List<Map<String,String>> configMapList = new ArrayList<>();

            synchronized (lockPlugin) {
                Iterator it = pluginMap.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();

                    String pluginID = (String) pair.getKey();


                    PluginNode pluginNode = (PluginNode) pair.getValue();

                    int status_code = pluginNode.getStatus_code();
                    String status_desc = pluginNode.getStatus_desc();
                    //boolean isActive = pluginNode.getActive();

                    Map<String, String> configMap = new HashMap<>();

                    configMap.put("status_code", String.valueOf(status_code));
                    configMap.put("status_desc", status_desc);
                    configMap.put("watchdog_period", String.valueOf(pluginNode.getWatchdogPeriod()));
                    configMap.put("watchdog_ts", String.valueOf(pluginNode.getWatchdogTS()));
                    configMap.put("pluginname", pluginNode.getName());
                    configMap.put("jarfile", pluginNode.getJarPath());
                    configMap.put("version", pluginNode.getVersion());

                    //configMap.put("isactive", String.valueOf(isActive));
                    configMap.put("pluginid", pluginID);
                    configMap.put("configparams", gson.toJson(pluginNode.exportParamMap()));
                    configMapList.add(configMap);
                    //it.remove(); // avoids a ConcurrentModificationException
                }
            }
            exportString = gson.toJson(configMapList);

        } catch(Exception ex) {
            logger.error("PluginExport.pluginExport() Error " + ex.getMessage());
        }

        return exportString;
    }
    */

    public boolean checkService(String className, String componentName) {

        return checkService(className, componentName, 1);

    }

    public boolean checkService(String className, String componentName, int TRYCOUNT) {
        boolean isStarted = false;

        try {
            ServiceReference<?>[] servRefs = null;
            int count = 0;

            while ((!isStarted) && (count < TRYCOUNT)) {

                String filterString = "(component.name=" + componentName + ")";
                Filter filter = context.createFilter(filterString);

                //servRefs = context.getServiceReferences(PluginService.class.getName(), filterString);
                servRefs = context.getServiceReferences(className, filterString);

                //System.out.println("REFS : " + servRefs.length);
                if (servRefs == null || servRefs.length == 0) {
                    //System.out.println("NULL FOUND NOTHING!");

                } else {
                    //System.out.println("Running Service Count: " + servRefs.length);

                    for (ServiceReference sr : servRefs) {

                        boolean assign = servRefs[0].isAssignableTo(context.getBundle(), className);

                        if(assign) {
                            isStarted = true;
                        }
                    }
                }
                count++;
                Thread.sleep(1000);
            }
            if(servRefs == null) {
                logger.error("COULD NOT START PLUGIN COULD NOT GET SERVICE");
            }
        } catch (Exception ex) {
            logger.error("checkService() ", ex);
        }
        return isStarted;
    }

    public Bundle installInternalBundleJars(String bundleName) {

        Bundle installedBundle = null;
        try {
            URL bundleURL = getClass().getClassLoader().getResource(bundleName);
            if(bundleURL != null) {

                String bundlePath = bundleURL.getPath();
                installedBundle = installedHere(context.installBundle(bundlePath,
                        getClass().getClassLoader().getResourceAsStream(bundleName)));

            } else {
                logger.error("installInternalBundleJars() Bundle = null");
            }
        } catch(Exception ex) {
            logger.error("installInternalBundleJars() ", ex);
        }

        if(installedBundle == null) {
            logger.error("controller installInternalBundleJars() Failed to load bundle " + bundleName + " exiting!");
            System.exit(0);
        }

        return installedBundle;
    }

    public void enablePlugin(MsgEvent ce) {

        //todo fix enable
        /*
        String pluginEnable = ce.getParam("action_plugin");

        AgentEngine.pluginMap.get(src_plugin).setStatus_code(10);

        if(ce.getParam("watchdogtimer") == null) {
            ce.setParam("watchdogtimer","5000");
        }

        AgentEngine.pluginMap.get(src_plugin).setWatchDogTimer(Long.parseLong(ce.getParam("watchdogtimer")));
        AgentEngine.pluginMap.get(src_plugin).setWatchDogTS(System.currentTimeMillis());

        logger.debug("Plugin {} status {}",src_plugin, AgentEngine.pluginMap.get(src_plugin).getStatus_code());
        */
    }

    public void disablePlugin(MsgEvent ce) {

        //todo fix disable
        /*
        String src_agent = ce.getParam("src_agent");
        String src_region = ce.getParam("src_region");
        String src_plugin = ce.getParam("src_plugin");
        if(src_agent.equals(AgentEngine.agent) && src_region.equals(AgentEngine.region)) {
            //status = 10, plugin enabled
            AgentEngine.pluginMap.get(src_plugin).setStatus_code(8);
            logger.debug("Plugin {} status {}",src_plugin, AgentEngine.pluginMap.get(src_plugin).getStatus_code());
        } else {
            logger.error("Can't enable plugin: {} for remote host: {} {} on {} {}",src_plugin, src_region, src_agent, AgentEngine.region, AgentEngine.agent);
        }
        */
    }

    public String getPluginJarPath(Map<String,String> hm) {
        String jarFilePath = null;

        try {
            boolean isLocal = false;
            String pluginName = hm.get("pluginname");
            String version = hm.get("version");

            Path repoCacheDir = getRepoCacheDir();
            if (repoCacheDir != null) {

                /*
                pluginMap.put("pluginname",pluginName);
                            pluginMap.put("jarfile",jarFileName);
                            pluginMap.put("md5",pluginMD5);
                            pluginMap.put("version",pluginVersion);

                 */

                List<Map<String, String>> pluginList = plugin.getPluginInventory(repoCacheDir.toFile().getAbsolutePath());
                if (pluginList != null) {
                    for (Map<String, String> params : pluginList) {
                        String pluginNameLocal = params.get("pluginname");
                        String versionLocal = params.get("version");

                        if ((pluginName != null) && (version != null)) {

                            if ((pluginName.equals(pluginNameLocal)) && (version.equals(versionLocal))) {

                            }

                        } else if (pluginName != null) {
                            if (pluginName.equals(pluginNameLocal)) {
                                isLocal = true;
                            }
                        }
                    }

                    if(isLocal) {
                        String tmpFilePath = repoCacheDir.toFile().getAbsolutePath() + System.getProperty("file.separator") + hm.get("jarfile");
                        File checkFile = new File(tmpFilePath);
                        if(checkFile.isFile()) {
                            jarFilePath = tmpFilePath;
                        }
                    }
                }
            }
        } catch(Exception ex) {
            logger.error("getPluginJarPath() ", ex);
        }

        return jarFilePath;
    }


    public Path getRepoCacheDir() {
        Path repoDirPath = null;
        try {
            String repoDirString = null;

            String cresco_data_location = System.getProperty("cresco_data_location");
            if(cresco_data_location != null) {
                Path path = Paths.get(cresco_data_location, "agent-repo-cache");
                repoDirString = plugin.getConfig().getStringParam("repo_cache_dir", path.toAbsolutePath().normalize().toString());
            } else {
                repoDirString =  plugin.getConfig().getStringParam("repo_cache_dir","cresco-data/agent-repo-cache");
            }

            repoDirPath = Paths.get(repoDirString);

            if(!repoDirPath.toFile().isDirectory()) {

                repoDirPath.toFile().mkdir();
            }

        } catch(Exception ex) {
            logger.error("getRepoCacheDir() ", ex);
        }
        return repoDirPath;
    }

    /**
     * An uploaded plugin jar: kept in the repository cache, under its own content hash, only when it verifies
     * ({@link JarIntake}). The caller's {@code md5} is advisory (a mismatch is logged). Returns the cached path, or null.
     */
    public String pluginUpdate(Map<String, String> hm, byte[] jarData) {
        String jarPath = null;
        Path verified = null;
        try {
            verified = jarIntake.verifiedFromBytes(jarData);
            Path path = jarIntake.moveToContentName(verified, getRepoCacheDir());
            verified = null;
            String claimed = hm == null ? null : hm.get("md5");
            if (claimed != null && !claimed.equals(path.getFileName().toString())) {
                logger.warn("pluginUpdate(): the uploader's hash " + claimed + " is not the jar's (" + path.getFileName() + "); the jar's is used");
            }
            jarPath = path.toAbsolutePath().toString();
        } catch (io.cresco.library.security.JarSignatures.Rejected rejected) {
            logger.error("pluginUpdate(): REFUSED an uploaded jar: " + rejected.getMessage());
        } catch (Exception ex) {
            logger.error("pluginUpdate() ", ex);
        } finally {
            if (verified != null) {
                try { Files.deleteIfExists(verified); } catch (Exception ignore) { }
            }
        }
        return jarPath;
    }


    public Path getPlugin(pNode node) {
        //boolean isFound = false;
        Path jarPath = null;
        try {

            logger.debug("Lookup for JAR pluginName: " + node.name);
            String pluginName = node.name;
            String pluginMD5 = node.md5;
            String jarFile = node.jarfile;

            if(node.repoServers.size() > 0) {

                for (Map<String, String> repoMap : node.repoServers) {

                    String region = repoMap.get("region");
                    String agent = repoMap.get("agent");
                    String pluginID = repoMap.get("pluginid");

                    logger.debug("REQUESTING JAR pluginName: " + pluginName + " md5: " + pluginMD5 + " jarfile: " + jarFile + " from region: " + region + " agent: " + agent + " pluginId: " + pluginID);


                    MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, region, agent, pluginID);
                    request.setParam("action", "getjar");
                    request.setParam("action_pluginname", pluginName);
                    request.setParam("action_pluginmd5", pluginMD5);
                    request.setParam("action_jarfile", jarFile);

                    MsgEvent retMsg = plugin.sendRPC(request);

                    if (retMsg != null) {

                        if (retMsg.paramsContains("jardata")) {

                            logger.debug("region: " + region + " agent: " + agent + " pluginId: " + pluginID + " responded with jar data");

                            // a fetched jar enters the cache only when it verifies, and only as the jar asked for
                            Path verified = null;
                            try {
                                verified = jarIntake.verifiedFromBytes(retMsg.getDataParam("jardata"));
                                String hash = io.cresco.library.crypto.CrescoCrypto.contentHashHex(verified);
                                if (pluginMD5 == null || pluginMD5.equals(hash)) {
                                    jarPath = jarIntake.moveToContentName(verified, getRepoCacheDir());
                                    verified = null;
                                    break;
                                } else {
                                    logger.error("Jar " + pluginName + " from " + region + "/" + agent + "/" + pluginID
                                            + " is not the jar asked for (hash " + hash + ", wanted " + pluginMD5 + ")");
                                }
                            } catch (io.cresco.library.security.JarSignatures.Rejected rejected) {
                                logger.error("Jar " + pluginName + " from " + region + "/" + agent + "/" + pluginID + " REFUSED: "
                                        + rejected.getMessage());
                            } finally {
                                if (verified != null) {
                                    try { Files.deleteIfExists(verified); } catch (Exception ignore) { }
                                }
                            }
                        } else {
                            logger.error("region: " + region + " agent: " + agent + " pluginId: " + pluginID + " responded without jar data");
                        }
                    } else {
                        logger.error("REQUESTED JAR pluginName: " + node.name + " from region: " + region + " agent: " + agent + " pluginId: " + pluginID + " NULL response.");

                    }
                }
            } else {
                logger.error("No repo servers found!");
            }

        }
        catch(Exception ex) {
            //System.out.println("getPlugin " + ex.getMessage());
            logger.error("getPlugin() ", ex);
        }
        //return isFound;
        return jarPath;
    }

    public pNode getPnode(Map<String,Object> pluginMap) {
        pNode pNode = null;
        try {

            String requestedName = (String)pluginMap.get("pluginname");
            String requestedVersion = (String)pluginMap.get("version");
            String requestedMD5 = (String)pluginMap.get("md5");

            //make sure cache is populated
            repoCache.cleanUp();

            if((System.currentTimeMillis() - lastRepoUpdate) > 5000) {
                Map<String,List<pNode>> repoSet = gdb.getPluginListRepoSet();
                if(repoSet != null) {
                    repoCache.putAll(gdb.getPluginListRepoSet());
                    lastRepoUpdate = System.currentTimeMillis();
                }
            }

            List<pNode> nodeList = null;
            try {
                nodeList = repoCache.getIfPresent(requestedName);
            } catch (Exception ex) {
                //logger.error("repoCache.getIfPresent(requestedName) requestedName: " + requestedName);
            }

            if(nodeList == null) {
                logger.debug("getPnode() nodeList is NULL");
            } else {
                logger.debug("getPnode() nodeList size : " + nodeList.size());
            }
            if(nodeList != null) {
                for(pNode tmpNode : nodeList) {

                    logger.debug("requestedName: " + requestedName + " requestedVersion: " + requestedVersion + " requestedMD5: " + requestedMD5);
                    logger.debug("foundName: " + tmpNode.name + " foundVersion: " + tmpNode.version + " foundMD5: " + tmpNode.md5);

                    if(tmpNode.isEqual(requestedName,requestedVersion,requestedMD5)) {
                        logger.debug("node: " + gson.toJson(tmpNode));
                        return tmpNode;
                    }
                }

            }

        } catch (Exception ex) {
            logger.error("getPnode()", ex);
        }
        return pNode;
    }

}