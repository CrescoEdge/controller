package io.cresco.agent.controller.agentcontroller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.agent.db.DBInterface;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.agent.AgentState;
import io.cresco.library.app.gEdge;
import io.cresco.library.app.pNode;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class PluginAdmin {

    private Gson gson;

    private DBInterfaceImpl gdb;
    private PluginBuilder plugin;

    private int PLUGINLIMIT = 900;
    private int TRYCOUNT = 30;

    private BundleContext context;
    private ConfigurationAdmin confAdmin;
    private Map<String,Configuration> configMap;
    private Map<String,PluginNode> pluginMap;
    private Map<Long,List<String>> bundleMap;


    private CLogger logger;

    private AtomicBoolean lockConfig = new AtomicBoolean();
    private AtomicBoolean lockPlugin = new AtomicBoolean();
    private AtomicBoolean lockBundle = new AtomicBoolean();


    private AgentState agentState;

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

        this.context = context;
        this.agentState = agentState;
        logger = plugin.getLogger(PluginAdmin.class.getName(), CLogger.Level.Info);


        ServiceReference configurationAdminReference = null;

            configurationAdminReference = context.getServiceReference(ConfigurationAdmin.class.getName());

            if (configurationAdminReference != null) {

                boolean assign = configurationAdminReference.isAssignableTo(context.getBundle(), ConfigurationAdmin.class.getName());

                if (assign) {
                    confAdmin = (ConfigurationAdmin) context.getService(configurationAdminReference);
                } else {
                    System.out.println("Could not Assign Configuration Admin!");
                }

            } else {
                System.out.println("Admin Does Not Exist!");
            }

    }

    public void setLogLevel(String logId, CLogger.Level level) {

        try {

            logId = logId.toLowerCase();
            /*
            if (level != CLogger.Level.Info) {
                System.out.println("LOG ID: " + logId + " LEVEL:" + level.name());
            }
            */

                Configuration logConfig = confAdmin.getConfiguration("org.ops4j.pax.logging", null);

                Dictionary<String, Object> log4jProps = logConfig.getProperties();
                log4jProps.put("log4j.logger." + logId, level.name().toUpperCase());

                logConfig.updateIfDifferent(log4jProps);


        } catch (Exception ex) {
            ex.printStackTrace();
        }

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
            ex.printStackTrace();
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
            ex.printStackTrace();
        }
        return exists;
    }



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
            ex.printStackTrace();
        }
        return bundleID;
    }

    public boolean startBundle(long bundleID) {
        boolean isStarted = false;
        try {
            context.getBundle(bundleID).start();
            isStarted = true;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  isStarted;
    }

    public boolean stopBundle(long bundleID) {
        boolean isStopped = false;
        try {
            context.getBundle(bundleID).stop();
            isStopped = true;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  isStopped;
    }

    public boolean removeBundle(long bundleID) {
        boolean isRemoved = false;
        try {
            context.getBundle(bundleID).uninstall();
            isRemoved = true;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  isRemoved;
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
            ex.printStackTrace();
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
        return addPlugin(null, map, null);
    }

    public String addPlugin(String pluginId, Map<String,Object> map) {
        return addPlugin(pluginId, map, null);
    }

    public String addPlugin(Map<String,Object> map, String edges) {
        return addPlugin(null, map, edges);
    }

    public String addPlugin(String pluginID, Map<String,Object> map, String edges) {

        String returnPluginID = null;
        if(pluginCount() < PLUGINLIMIT) {
            try {

                if(pluginID == null) {
                    pluginID = "plugin-" + UUID.randomUUID().toString();
                }

                long bundleID = addBundle(map);
                if (bundleID != -1) {

                    if(edges != null) {
                        map.put("edges",edges);
                    }

                    //String pluginID = addConfig(pluginName, map);
                    addConfig(pluginID, map);

                    if (startBundle(bundleID)) {
                        if (pluginID != null) {

                            PluginNode pluginNode = null;
                            if(edges != null) {
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
                                if(!bundleMap.containsKey(bundleID)) {
                                    bundleMap.put(bundleID,new ArrayList<>());
                                }
                                bundleMap.get(bundleID).add(pluginID);
                            }


                            if (startPlugin(pluginID)) {
                                returnPluginID = pluginID;
                            } else {
                                System.out.println("Could not start agentcontroller " + pluginID + " pluginName " + map.get("pluginname") + " no bundle " + map.get("jarfile"));
                            }

                        } else {
                            System.out.println("Could not create config for " + " pluginName " + map.get("pluginname") + " no bundle " + map.get("jarfile"));
                        }
                    } else {
                        System.out.println("Could not start bundle Id " + bundleID + " pluginName " + map.get("pluginname") + " no bundle " + map.get("jarfile"));
                        System.out.println("Remove configuration! --  bundle Id " + bundleID + " pluginName " + map.get("pluginName") + " no bundle " + map.get("jarFile"));

                    }
                    //controllerEngine.getPluginAdmin().startBundle(bundleID);
                    //String pluginID = controllerEngine.getPluginAdmin().addConfig(pluginName,jarFile, map);
                    //controllerEngine.getPluginAdmin().startPlugin(pluginID);
                } else {
                    logger.error("Can't add " + map.get("pluginname") + " no bundle " + map.get("jarfile"));
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return returnPluginID;
    }

    public void addDirectConfig(String factoryPid, Dictionary<String, Object> properties) {

        try {
            Configuration configuration = confAdmin.createFactoryConfiguration(factoryPid, null);
            configuration.update(properties);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public boolean addConfig(String pluginId, Map<String,Object> map) {
        boolean isAdded = false;
        try {

                boolean isEmpty = false;

                while (!isEmpty) {

                    synchronized (lockConfig) {
                        if (!configMap.containsKey(pluginId)) {
                            Configuration configuration = confAdmin.createFactoryConfiguration((String)map.get("pluginname") + ".Plugin", null);

                            Dictionary properties = new Hashtable();

                            ((Hashtable) properties).putAll(map);

                            properties.put("pluginID", pluginId);
                            configuration.update(properties);

                            configMap.put(pluginId, configuration);
                            isEmpty = true;
                        }
                    }

                }


        } catch(Exception ex) {
            ex.printStackTrace();
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

                } else {
                    //System.out.println("Running Service Count: " + servRefs.length);

                    for (ServiceReference sr : servRefs) {

                        boolean assign = servRefs[0].isAssignableTo(context.getBundle(), PluginService.class.getName());

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
                                System.out.println("Could not start!");
                                ex.printStackTrace();
                            }

                            synchronized (lockPlugin) {
                                if (pluginMap.containsKey(pluginID)) {
                                    pluginMap.get(pluginID).setPluginService((PluginService) context.getService(sr));
                                    pluginMap.get(pluginID).setStatus(statusCode, statusDesc);
                                } else {
                                    System.out.println("NO PLUGIN IN PLUGIN MAP FOR THIS SERVICE : " + pluginID + " elements " + pluginMap.hashCode() + " thread:" + Thread.currentThread().getName());
                                }
                            }

                            isStarted = true;
                        }
                    }
                }
                count++;
                Thread.sleep(1000);
            }
            if(servRefs == null) {
                logger.error("startPlugin : COULD NOT START PLUGIN COULD NOT GET SERVICE");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
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
            ex.printStackTrace();
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
            ex.printStackTrace();
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
                    PluginNode pluginNode = (PluginNode) pair.getValue();

                    int status_code = pluginNode.getStatus_code();
                    String status_desc = pluginNode.getStatus_desc();
                    //boolean isActive = pluginNode.getActive();

                    Map<String, String> configMap = new HashMap<>();


                    configMap.put("status_code", String.valueOf(status_code));
                    configMap.put("status_desc", status_desc);
                    configMap.put("watchdogtimer", String.valueOf(pluginNode.getWatchdogTimer()));
                    //configMap.put("isactive", String.valueOf(isActive));
                    configMap.put("pluginid", pluginID);
                    configMap.put("configparams", gson.toJson(pluginNode.exportParamMap()));
                    configMapList.add(configMap);
                    //it.remove(); // avoids a ConcurrentModificationException
                }
            }
            exportString = gson.toJson(configMapList);

        } catch(Exception ex) {
            System.out.println("PluginExport.pluginExport() Error " + ex.getMessage());
        }

        return exportString;
    }

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
                System.out.println("COULD NOT START PLUGIN COULD NOT GET SERVICE");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
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
                System.out.println("installInternalBundleJars() Bundle = null");
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        if(installedBundle == null) {
            System.out.println("controller installInternalBundleJars() Failed to load bundle " + bundleName + " exiting!");
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

                        } else {
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
            ex.printStackTrace();
        }

        return jarFilePath;
    }

    public boolean pluginIsLocal(Map<String,Object> hm) throws IOException {

        boolean isLocal = false;

        String jarFilePath = null;

        String pluginName = (String)hm.get("pluginname");
        String version = (String)hm.get("version");

        Path repoCacheDir = getRepoCacheDir();
        if (repoCacheDir != null) {

            List<Map<String, String>> pluginList = plugin.getPluginInventory(repoCacheDir.toFile().getAbsolutePath());
            if (pluginList != null) {
                for (Map<String, String> params : pluginList) {
                    String pluginNameLocal = params.get("pluginname");
                    String versionLocal = params.get("version");

                    if ((pluginName != null) && (version != null)) {

                        if ((pluginName.equals(pluginNameLocal)) && (version.equals(versionLocal))) {
                            isLocal = true;
                        }

                    } else {
                        if (pluginName.equals(pluginNameLocal)) {
                            //isLocal = true;
                            logger.error("Plugin Version Mismatch");
                        }
                    }
                }
            }
        }

        return isLocal;
    }

    public String getCachedJarPath(Map<String,Object> hm) throws IOException {

        String jarFilePath = null;

        String pluginName = (String)hm.get("pluginname");
        String version = (String)hm.get("version");

        Path repoCacheDir = getRepoCacheDir();
        if (repoCacheDir != null) {

            List<Map<String, String>> pluginList = plugin.getPluginInventory(repoCacheDir.toFile().getAbsolutePath());
            if (pluginList != null) {
                for (Map<String, String> params : pluginList) {
                    String pluginNameLocal = params.get("pluginname");
                    String versionLocal = params.get("version");

                    if ((pluginName != null) && (version != null)) {

                        if ((pluginName.equals(pluginNameLocal)) && (version.equals(versionLocal))) {
                            //pluginMap.put("jarfile", jarFileName);
                            //pluginMap.put("md5", pluginMD5);
                            jarFilePath = params.get("jarfile");
                        }

                    } else {
                        if (pluginName.equals(pluginNameLocal)) {
                            //jarFilePath = params.get("jarfile");
                            logger.error("Plugin Version Mismatch");
                        }
                    }
                }
            }
        }

        return jarFilePath;
    }

    public Path getRepoCacheDir() {
        Path repoDirPath = null;
        try {

            String repoDirString =  plugin.getConfig().getStringParam("repo_cache_dir","repo-cache");

            repoDirPath = Paths.get(repoDirString);

            if(!repoDirPath.toFile().isDirectory()) {

                repoDirPath.toFile().mkdir();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return repoDirPath;
    }

    public boolean getPlugin(pNode node) {
        boolean isFound = false;
        try {

            logger.debug("REQUESTING JAR : " + node.name);
            String pluginName = node.name;
            String pluginMD5 = node.md5;
            String jarFile = node.jarfile;

            for(Map<String,String> repoMap : node.repoServers) {

                String region = repoMap.get("region");
                String agent = repoMap.get("agent");
                String pluginID = repoMap.get("pluginid");

                MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,region,agent,pluginID);
                request.setParam("action","getjar");
                request.setParam("action_pluginname",pluginName);
                request.setParam("action_pluginmd5",pluginMD5);
                request.setParam("action_jarfile",jarFile);

                MsgEvent retMsg = plugin.sendRPC(request);

                String jarFileSavePath = getRepoCacheDir().toFile().getAbsolutePath() + "/" + jarFile;

                logger.debug("SAVE FILE : " + jarFileSavePath);

                Path path = Paths.get(jarFileSavePath);
                Files.write(path, retMsg.getDataParam("jardata"));
                File jarFileSaved = new File(jarFileSavePath);
                if(jarFileSaved.isFile()) {
                    String md5 = plugin.getMD5(jarFileSavePath);
                    if(pluginMD5.equals(md5)) {
                        isFound = true;
                        logger.debug("SAVE FILE : " + jarFileSavePath + " isFound" + isFound);
                    }
                }
            }

        }
        catch(Exception ex) {
            //System.out.println("getPlugin " + ex.getMessage());
            ex.printStackTrace();
        }
        return isFound;
    }




}
