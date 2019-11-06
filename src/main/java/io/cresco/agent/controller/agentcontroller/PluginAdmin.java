package io.cresco.agent.controller.agentcontroller;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.agent.AgentState;
import io.cresco.library.app.gEdge;
import io.cresco.library.app.pNode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.*;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import java.io.*;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class PluginAdmin {

    private Gson gson;

    private DBInterfaceImpl gdb;
    private PluginBuilder plugin;

    private int PLUGINLIMIT = 900;
    private int TRYCOUNT = 300;

    private BundleContext context;
    private ConfigurationAdmin confAdmin;
    private Map<String,Configuration> configMap;
    private Map<String,PluginNode> pluginMap;
    private Map<Long,List<String>> bundleMap;
    private Map<String,List<String>> jarRepoSyncMap;

    private String agentEmbeddedJarPath = null;
    private CLogger logger;

    private AtomicBoolean lockConfig = new AtomicBoolean();
    private AtomicBoolean lockPlugin = new AtomicBoolean();
    private AtomicBoolean lockBundle = new AtomicBoolean();
    private AtomicBoolean lockJarRepoSync = new AtomicBoolean();

    private long lastRepoUpdate = 0;

    private Cache<String, List<pNode>> repoCache;

    public int pluginCount() {

        synchronized (lockConfig) {
            return configMap.size();
        }
    }


    public PluginAdmin(PluginBuilder plugin, AgentState agentState, DBInterfaceImpl gdb, BundleContext context) {
        this.plugin = plugin;
        this.gdb = gdb;
        this.gson = new Gson();
        this.configMap = Collections.synchronizedMap(new HashMap<>());
        this.pluginMap = Collections.synchronizedMap(new HashMap<>());
        this.bundleMap = Collections.synchronizedMap(new HashMap<>());
        this.jarRepoSyncMap = Collections.synchronizedMap(new HashMap<>());

        this.context = context;
        logger = plugin.getLogger(PluginAdmin.class.getName(), CLogger.Level.Info);



        repoCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .softValues()
                .maximumSize(1000)
                .expireAfterWrite(5, TimeUnit.SECONDS)
                .build();


        ServiceReference configurationAdminReference = null;

        configurationAdminReference = context.getServiceReference(ConfigurationAdmin.class.getName());

        if (configurationAdminReference != null) {

            boolean assign = configurationAdminReference.isAssignableTo(context.getBundle(), ConfigurationAdmin.class.getName());

            if (assign) {
                confAdmin = (ConfigurationAdmin) context.getService(configurationAdminReference);
            } else {
                logger.error("Could not Assign Configuration Admin!");
            }

        } else {
            logger.error("Admin Does Not Exist!");
        }

    }

    public void setLogLevel(String logId, CLogger.Level level) {

        try {

            Configuration logConfig = confAdmin.getConfiguration("org.ops4j.pax.logging", null);

            Dictionary<String, Object> log4jProps = logConfig.getProperties();
            log4jProps.put("log4j.logger." + logId, level.name().toUpperCase());

            logConfig.updateIfDifferent(log4jProps);

        } catch (Exception ex) {
            logger.error("setLogLevel() " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
        }

    }

    public void setLogLevels(Map<String,String> logMap) {

        try {
            for (Map.Entry<String, String> entry : logMap.entrySet()) {
                String logId = entry.getKey();
                CLogger.Level logLevel = CLogger.Level.valueOf(entry.getValue());
                setLogLevel(logId,logLevel);
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

    }

    public Map<String,String> getLogLevels() {

        Map<String,String> logMap = null;
        try {

            logMap = new HashMap<>();

            Configuration logConfig = confAdmin.getConfiguration("org.ops4j.pax.logging", null);

            Dictionary<String, Object> log4jProps = logConfig.getProperties();

            Enumeration<String> e = log4jProps.keys();
            while(e.hasMoreElements()) {
                String logId = e.nextElement();
                String level = (String)log4jProps.get(logId);

                System.out.println(logId.replace("log4j.logger.","") + ": " + level);
                logMap.put(logId.replace("log4j.logger.",""),level);
            }

        } catch (Exception ex) {
            logger.error("getLogLevels() " + ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);
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
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
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
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return exists;
    }

    public Map<String,Object> jarIsBundle(Map<String,Object> map) {
        Map<String,Object> returnMap = null;
        try {

            Bundle[] bundleList = context.getBundles();

            for(Bundle b : bundleList) {

                String eBundleId = String.valueOf(b.getBundleId());
                String eName = b.getSymbolicName();
                String eVersion = b.getVersion().toString();

                String requestedName = (String) map.get("pluginname");

                if((eName != null) && (eVersion != null)) {

                    if (map.containsKey("version")) {

                        String requestedVersion = (String) map.get("version");
                        String requestedMD5 = (String) map.get("md5");

                        if(requestedMD5 != null) {

                            //String jarLocation = Paths.get(b.getLocation()).toString();
                            String jarLocation = b.getLocation();

                            if(jarLocation.contains("!")) {
                                jarLocation = "jar:" + jarLocation;
                            }

                            String eMD5 = plugin.getMD5(jarLocation);

                            if(eMD5 != null) {

                                if ((eName.equals(requestedName) && (eVersion.equals(requestedVersion)) )) {

                                    returnMap = new HashMap<>();
                                    returnMap.putAll(map);
                                    returnMap.put("jarstatus", "bundle");
                                    returnMap.put("bundle_id", eBundleId);
                                }

                            }

                        } else {

                            if ((eName.equals(requestedName) && (eVersion.equals(requestedVersion)))) {

                                returnMap = new HashMap<>();
                                returnMap.putAll(map);
                                returnMap.put("jarstatus", "bundle");
                                returnMap.put("bundle_id", eBundleId);
                            }
                        }
                    } else {
                        if (eName.equals(requestedName)) {

                            returnMap = new HashMap<>();
                            returnMap.putAll(map);
                            returnMap.put("version", eVersion);
                            returnMap.put("jarstatus", "bundle");
                            returnMap.put("bundle_id", eBundleId);
                        }
                    }

                }

            }


        } catch (Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error("jarIsBundle() " + ex.getMessage());
            logger.error("jarIsBundle() " + errors);

        }
        return returnMap;
    }


    public Map<String,Object> jarIsEmbedded(Map<String,Object> map) {
        Map<String,Object> returnMap = null;
        try {

            String requestedJarPath = (String) map.get("jarfile");

            if(requestedJarPath != null) {

                if (agentEmbeddedJarPath == null) {

                    Bundle[] bundleList = context.getBundles();

                    for (Bundle b : bundleList) {

                        //String eBundleId = String.valueOf(b.getBundleId());
                        String eName = b.getSymbolicName();
                        if (eName != null) {
                            if (eName.equals("io.cresco.controller")) {
                                String jarLocation = b.getLocation();
                                if (jarLocation.contains("!")) {
                                    String[] locationSplit = jarLocation.split("!");
                                    if (locationSplit.length > 0) {
                                        agentEmbeddedJarPath = locationSplit[0];
                                        //logger.error("agent location: " + locationSplit[0]);
                                    }
                                }
                            }
                        }
                    }
                }

                if (agentEmbeddedJarPath != null) {
                    URL url = getClass().getClassLoader().getResource(requestedJarPath);

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
                            in.close();

                            String requestedName = (String) map.get("pluginname");
                            if (map.containsKey("version")) {
                                String requestedVersion = (String) map.get("version");
                                String requestedMD5 = (String) map.get("md5");

                                if ((eName.equals(requestedName) && (eVersion.equals(requestedVersion)) && (eMD5.equals(requestedMD5)))) {
                                    returnMap = new HashMap<>();
                                    returnMap.putAll(map);
                                    returnMap.put("jarstatus", "embedded");
                                }
                            } else {
                                if (eName.equals(requestedName)) {
                                    returnMap = new HashMap<>();
                                    returnMap.putAll(map);
                                    returnMap.put("version", eVersion);
                                    returnMap.put("md5", eMD5);
                                    returnMap.put("jarstatus", "embedded");
                                }
                            }
                        }catch (Exception ex) {
                            //gobble exception
                        }
                    }
                }
            }

        } catch (Exception ex) {
            logger.error("jarIsEmbedded-Agent()");
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
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
                                returnMap = new HashMap<>();
                                returnMap.putAll(map);
                                returnMap.put("jarstatus", "absolutepath");
                            }
                        } else {
                            if (aName.equals(requestedName)) {
                                returnMap = new HashMap<>();
                                returnMap.putAll(map);
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
            logger.error("jarIsAbsolutePath()");
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
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
                        String lName = params.get("pluginname");
                        String lVersion = params.get("version");
                        String lMD5 = params.get("md5");

                        String requestedName = (String) map.get("pluginname");
                        if(map.containsKey("version")) {
                            String requestedVersion = (String) map.get("version");
                            String requestedMD5 = (String) map.get("md5");

                            if((lName.equals(requestedName) && (lVersion.equals(requestedVersion)) && (lMD5.equals(requestedMD5)))) {
                                returnMap = new HashMap<>();
                                returnMap.putAll(map);
                                returnMap.put("jarfile",params.get("jarfile"));
                                returnMap.put("jarstatus","localcache");
                            }
                        } else {

                            if(lName.equals(requestedName)) {
                                returnMap = new HashMap<>();
                                returnMap.putAll(map);
                                returnMap.put("jarfile",params.get("jarfile"));
                                returnMap.put("version",lVersion);
                                returnMap.put("md5",lMD5);
                                returnMap.put("jarstatus","localcache");
                            }
                        }


                    }
                }
            }
        } catch (Exception ex) {
            logger.error("getJarFromLocalCache()");
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }

        return returnMap;
    }

    public Map<String,Object> getJarFromRepo(Map<String,Object> map)  {
        Map<String,Object> returnMap = null;
        try {

            pNode node = getPnode(map);
            if(node != null) {
                Path jarPath = getPlugin(node);
                if(jarPath != null) {
                    if (jarPath.toFile().isFile()) {
                        returnMap = getJarFromLocalCache(map);
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
            logger.error("getJarFromRepo()");
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }

        return returnMap;
    }

    public Map<String,Object> validatePluginMap(Map<String,Object> map) {
        Map<String,Object> validatedMap = null;
        try {

            String requestedName = (String) map.get("pluginname");
            String requestedMD5 = (String) map.get("md5");
            if (requestedMD5 == null) {
                requestedMD5 = "null";
            }

            boolean repoSyncActive = true;

            //check if download is in progress

            while (repoSyncActive) {

                synchronized (lockJarRepoSync) {
                    if (jarRepoSyncMap.containsKey(requestedName)) {
                        if (jarRepoSyncMap.get(requestedName).contains(requestedMD5)) {
                            repoSyncActive = true;


                        } else {
                            jarRepoSyncMap.get(requestedName).add(requestedMD5);
                            repoSyncActive = false;
                            logger.debug("SET LOCK ON EXISTING PLUGIN NAME: " + requestedName + " MD5: " + requestedMD5);
                        }

                    } else {
                        jarRepoSyncMap.put(requestedName, new ArrayList<>());
                        jarRepoSyncMap.get(requestedName).add(requestedMD5);
                        repoSyncActive = false;
                        logger.debug("SET LOCK ON NEW PLUGIN NAME: " + requestedName + " MD5: " + requestedMD5);
                    }
                }

                if(repoSyncActive) {
                    logger.info("Waiting on repoSync to complete for pluginName: " + requestedName + " MD5: " + requestedMD5);
                    Thread.sleep(1000);
                }
            }

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

            if(validatedMap != null) {
                return validatedMap;
            } else {
                validatedMap = getJarFromRepo(map);
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
            logger.error("validatePluginMap()");
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
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
                    }


                    break;
                case "absolutepath":

                    bundle = context.getBundle((String) map.get("jarfile"));

                    if (bundle == null) {
                        bundle = context.installBundle("file:" + map.get("jarfile"));
                    }

                    break;

                case "localcache":

                    Path jarPath = Paths.get(getRepoCacheDir().toString(),(String)map.get("jarfile"));
                    if(jarPath.toFile().isFile()) {
                        bundle = context.getBundle(jarPath.toFile().getAbsolutePath());

                        if (bundle == null) {
                            bundle = context.installBundle("file:" + jarPath.toFile().getAbsolutePath());
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
            logger.error("addBundle()");
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return bundleID;
    }


    /*
    public long addBundle(Map<String,Object> pluginMap) {
        long bundleID = -1;
        try {


            boolean jarIsLocal = pluginIsLocal(pluginMap);
            String fileLocation = null;

            if(!jarIsLocal) {
                //try to download node
                //pNode node = gson.fromJson(ce.getCompressedParam("pnode"), pNode.class);
                //jarIsLocal = controllerEngine.getPluginAdmin().getPlugin(node);
                //logger.error("!!! Implement plugin fetch from repo");
            }

            if(jarIsLocal) {

                //replace remote jarfilename with local
                fileLocation = getCachedJarPath(pluginMap);

            } else {
                fileLocation = (String) pluginMap.get("jarfile");
            }

            if(fileLocation != null) {
                Bundle bundle = null;

                //absolute file path was given
                Path checkFile = Paths.get(fileLocation);

                if (checkFile.toFile().isFile()) {

                    bundle = context.getBundle(fileLocation);

                    if (bundle == null) {
                        bundle = context.installBundle("file:" + fileLocation);
                    }

                }
                //check local repo
                else {
                    URL bundleURL = getClass().getClassLoader().getResource(fileLocation);
                    if (bundleURL != null) {

                        String bundlePath = bundleURL.getPath();
                        InputStream bundleStream = getClass().getClassLoader().getResourceAsStream(fileLocation);
                        bundle = context.installBundle(bundlePath, bundleStream);
                    }
                }
                if (bundle != null) {
                    bundleID = bundle.getBundleId();
                }
            }


        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return bundleID;
    }
    */

    String mostTargeted(String key, String pid, Bundle bundle) throws Exception {

        String bsn = bundle.getSymbolicName();
        Version version = bundle.getVersion();
        String location = bundle.getLocation();
        String f = String.format("(|(%1$s=%2$s)(%1$s=%2$s|%3$s)" +
                        "(%1$s=%2$s|%3$s|%4$s)(%1$s=%2$s|%3$s|%4$s|%5$s))",
                key, pid, bsn, version, location );

        System.out.println("Config: " + f);

        Configuration[] configurations = confAdmin.listConfigurations(f);
        if (configurations == null)
            return null;

        String largest = null;
        for (Configuration c : configurations) {
            String s = (String) c.getProperties().get(key);
            if ((largest == null) || (largest.length() < s.length()))
                largest = s;
        }
        return largest;
    }



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
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return  isStarted;
    }

    public boolean stopBundle(long bundleID) {
        boolean isStopped = false;
        try {
            context.getBundle(bundleID).stop();
            isStopped = true;
        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return  isStopped;
    }

    public boolean removeBundle(long bundleID) {
        boolean isRemoved = false;
        try {
            context.getBundle(bundleID).uninstall();
            isRemoved = true;
        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return  isRemoved;
    }

    public void stopAllPlugins() {

        Set<String> keys = null;
        synchronized (lockConfig) {
            keys = pluginMap.keySet();
        }

        for(String pid : keys) {
            stopPlugin(pid);
        }

    }

    public boolean stopPlugin(String pluginId) {
        boolean isStopped = false;
        try {

            logger.debug("stopPlugin: " + pluginId);

            String jarFilePath = null;
            String pid = null;
            boolean isPluginStopped = false;
            long bundleID = -1;

            synchronized (lockPlugin) {
                if (pluginMap.containsKey(pluginId)) {
                    jarFilePath = pluginMap.get(pluginId).getJarPath();
                    isPluginStopped = pluginMap.get(pluginId).getPluginService().isStopped();
                    bundleID = pluginMap.get(pluginId).getBundleID();
                } else {
                    logger.error("stopPlugin() no key found for pluginid: " + pluginId);
                    //not found return true
                    return true;
                }
            }

            logger.debug("stopPlugin jarfilePath: " + jarFilePath);
            logger.debug("stopPlugin ispluginstipped: " + isPluginStopped);
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



                if ((jarFilePath != null) && (pid != null)) {

                    Configuration pluginConfig = confAdmin.getConfiguration(pid);
                    if(pluginConfig != null) {

                        pluginConfig.delete();

                        synchronized (lockPlugin) {
                            pluginMap.remove(pluginId);
                        }
                        synchronized (lockConfig) {
                            configMap.remove(pluginId);
                        }
                        //remove from database
                        gdb.removeNode(plugin.getRegion(), plugin.getAgent(), pluginId);

                        isStopped = true;
                    }
                }
            } else {
                logger.error("stopPlugin() could not stop plugin!");
            }

        } catch(Exception ex) {

            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error("stopPlugin() " + errors.toString());
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

                    Configuration pluginConfig = confAdmin.getConfiguration(pid);
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

    public void msgIn(MsgEvent msg) {

        String pluginID = msg.getDstPlugin();
        synchronized (lockPlugin) {
            if (pluginMap.containsKey(pluginID)) {
                if(pluginMap.get(pluginID).getActive()) {
                    pluginMap.get(pluginID).getPluginService().inMsg(msg);
                }
            }
        }

    }

    public String addPlugin(Map<String,Object> map) {
        return addPlugin(map, null);
    }

    public String addPlugin(Map<String,Object> map, String edges) {

        String returnPluginID = null;
        if(pluginCount() < PLUGINLIMIT) {
            try {

                String pluginID = (String)map.get("inode_id");

                if(pluginID == null) {
                    pluginID = "plugin-" + UUID.randomUUID().toString();
                }

                String pluginName = (String)map.get("pluginname");

                map = validatePluginMap(map);

                if(map != null) {

                    long bundleID = addBundle(map);
                    if (bundleID != -1) {

                        if (edges != null) {
                            map.put("edges", edges);
                        }

                        //String pluginID = addConfig(pluginName, map);
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
                                logger.error("Remove configuration! --  bundle Id " + bundleID + " pluginName " + map.get("pluginName") + " no bundle " + map.get("jarFile"));

                            }
                        } else {
                            System.out.println("Could not create config pluginName " + map.get("pluginname") + " for jar " + map.get("jarfile"));
                        }
                        //controllerEngine.getPluginAdmin().startBundle(bundleID);
                        //String pluginID = controllerEngine.getPluginAdmin().addConfig(pluginName,jarFile, map);
                        //controllerEngine.getPluginAdmin().startPlugin(pluginID);
                    } else {
                        logger.error("Can't add " + map.get("pluginname") + " no bundle " + map.get("jarfile"));
                    }
                } else {
                    logger.error("Can't add " + pluginName + " could not find suitable jar for bundle loading!");
                }



            } catch (Exception ex) {
               StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
            }
        }
        return returnPluginID;
    }

    public void addDirectConfig(String factoryPid, Dictionary<String, Object> properties) {

        try {
            Configuration configuration = confAdmin.createFactoryConfiguration(factoryPid, null);
            configuration.update(properties);

        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }

    }


    public boolean addConfig(String pluginId, Map<String,Object> map) {
        boolean isAdded = false;
        try {

            boolean isEmpty = false;

            while (!isEmpty) {

                synchronized (lockConfig) {
                    if (!configMap.containsKey(pluginId)) {

                        String pid = map.get("pluginname") + ".Plugin";
                        String bsn = (String)map.get("pluginname");
                        String version = (String)map.get("version");

                        String configString = pid + "|" + bsn + "|" + version;


                        //Configuration configuration = confAdmin.createFactoryConfiguration(configString, null);

                        Configuration configuration = confAdmin.createFactoryConfiguration(map.get("pluginname") + ".Plugin", null);


                        Dictionary properties = new Hashtable();

                        ((Hashtable) properties).putAll(map);
                        properties.put("pluginID", pluginId);
                        //properties.put("service.pid2",configString2);
                        configuration.update(properties);

                        configMap.put(pluginId, configuration);
                        isEmpty = true;
                        isAdded = true;
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

                    System.out.println("NULL FOUND NOTHING!");

                } else {
                    //System.out.println("Running Service Count: " + servRefs.length);

                    for (ServiceReference sr : servRefs) {

                        boolean assign = sr.isAssignableTo(context.getBundle(), PluginService.class.getName());

                        if(assign) {
                            PluginService ps = (PluginService) context.getService(sr);
                            int statusCode = 7;
                            String statusDesc = "Plugin instance could not be started";
                            try {
                                while(ps == null) {
                                    logger.error("PLUGIN SERVICE FOR PLUGIN " + pluginID + " is Null");
                                    Thread.sleep(1000);
                                }

                                if(ps.isStarted()) {
                                    statusCode = 10;
                                    statusDesc = "Plugin Active";
                                }
                            } catch(Exception ex) {
                                logger.error("Could not start!");
                               StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
                            }

                            synchronized (lockPlugin) {
                                if (pluginMap.containsKey(pluginID)) {
                                    pluginMap.get(pluginID).setPluginService((PluginService) context.getService(sr));
                                    pluginMap.get(pluginID).setStatus(statusCode, statusDesc);
                                } else {
                                    logger.error("NO PLUGIN IN PLUGIN MAP FOR THIS SERVICE : " + pluginID + " elements " + pluginMap.hashCode() + " thread:" + Thread.currentThread().getName());
                                }
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
        } catch (Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return isStarted;
    }

    public boolean pluginExist(String pluginID) {
        boolean pluginExist = false;

        try {

            synchronized (lockPlugin) {
                if(pluginMap.containsKey(pluginID)) {
                    pluginExist = true;
                }
            }

        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }


        return pluginExist;
    }

    public Map<String,String> getPluginStatus(String pluginID) {
        Map<String,String> statusMap = null;
        try {

            synchronized (lockPlugin) {
                if(pluginMap.containsKey(pluginID)) {
                    statusMap = new HashMap<>();
                    PluginNode pluginNode = pluginMap.get(pluginID);
                    int status_code = pluginNode.getStatus_code();
                    String status_desc = pluginNode.getStatus_desc();
                    boolean isActive = pluginNode.getActive();

                    statusMap.put("status_code", String.valueOf(status_code));
                    statusMap.put("status_desc", status_desc);
                    statusMap.put("isactive", String.valueOf(isActive));
                }
            }

        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return statusMap;
    }

    public String getPluginExport() {


        String exportString = null;
        try {

            List<Map<String,String>> configMapList = new ArrayList<>();

            synchronized (lockPlugin) {
                Iterator it = pluginMap.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();

                    String pluginID = (String) pair.getKey();

                    configMapList.add(gdb.getPNode(pluginID));
                    //it.remove(); // avoids a ConcurrentModificationException
                }
            }
            exportString = gson.toJson(configMapList);

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
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return isStarted;
    }

    public Bundle installInternalBundleJars(String bundleName) {

        Bundle installedBundle = null;
        try {
            URL bundleURL = getClass().getClassLoader().getResource(bundleName);
            if(bundleURL != null) {

                String bundlePath = bundleURL.getPath();
                installedBundle = context.installBundle(bundlePath,
                        getClass().getClassLoader().getResourceAsStream(bundleName));

            } else {
                logger.error("installInternalBundleJars() Bundle = null");
            }
        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
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
                        String tmpFilePath = repoCacheDir.toFile().getAbsolutePath() + "/" + hm.get("jarfile");
                        File checkFile = new File(tmpFilePath);
                        if(checkFile.isFile()) {
                            jarFilePath = tmpFilePath;
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

        return jarFilePath;
    }


    public Path getRepoCacheDir() {
        Path repoDirPath = null;
        try {

            String repoDirString =  plugin.getConfig().getStringParam("repo_cache_dir","cresco-data/agent-repo-cache");

            repoDirPath = Paths.get(repoDirString);

            if(!repoDirPath.toFile().isDirectory()) {

                repoDirPath.toFile().mkdir();
            }

        } catch(Exception ex) {
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return repoDirPath;
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

                            Path path = Paths.get(getRepoCacheDir().toFile().getAbsolutePath() + "/" + jarFile);

                            Files.write(path, retMsg.getDataParam("jardata"));

                            if (path.toFile().isFile()) {
                                String md5 = plugin.getMD5(path.toFile().getAbsolutePath());
                                if (pluginMD5.equals(md5)) {
                                    jarPath = path;
                                } else {
                                    logger.error("Jar " + pluginName + " failed MD5 Check");
                                }
                            } else {
                                if(jarPath != null) {
                                    logger.error("Jar " + pluginName + " Path: " + jarPath.toFile().getAbsolutePath() + " is not a file!");
                                } else {
                                    logger.error("Jar " + pluginName + " Path = NULL");
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
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
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
                repoCache.putAll(gdb.getPluginListRepoSet());
                lastRepoUpdate = System.currentTimeMillis();
            }

            List<pNode> nodeList = repoCache.getIfPresent(requestedName);
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
            logger.error("getPnode()");
           StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            logger.error(sStackTrace);;
        }
        return pNode;
    }

}