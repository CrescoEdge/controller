package io.cresco.agent.controller.db.testhelpers;

import com.google.gson.Gson;
//import io.cresco.agent.controller.agentcontroller.PluginNode;


import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class PluginAdmin {

    private Gson gson;

    //private int PLUGINLIMIT = 900;
    //private int TRYCOUNT = 30;


    private Map<String,Object> configMap;
    private Map<String,PluginNode> pluginMap;

    private AtomicBoolean lockConfig = new AtomicBoolean();
    private AtomicBoolean lockPlugin = new AtomicBoolean();


    //private AgentState agentState;

    /*public int pluginCount() {

        synchronized (lockConfig) {
            return configMap.size();
        }
    }*/

    /*
    public void getRepo() {


        Repository repo = null;
        ServiceReference repoReference = null;

        repoReference = context.getServiceReference(Repository.class.getName());
        if (repoReference != null) {

            boolean assign = repoReference.isAssignableTo(context.getBundle(), Repository.class.getName());

            if (assign) {
                repo = (Repository) context.getService(repoReference);
            } else {
                System.out.println("Could not Assign Configuration Admin!");
            }

        } else {
            System.out.println("Admin Does Not Exist!");
        }


    }
    */

    public PluginAdmin() {

        this.gson = new Gson();
        this.configMap = Collections.synchronizedMap(new HashMap<>());
        this.pluginMap = Collections.synchronizedMap(new HashMap<>());

    }

   /* public long addBundle(String fileLocation) {
        long bundleID = -1;
        try {

            Bundle bundle = null;

            File checkFile = new File(fileLocation);
            if(checkFile.isFile()) {

                bundle = context.getBundle(fileLocation);

                if(bundle == null) {
                    bundle = context.installBundle("file:" + fileLocation);
                }

            }
            //check local repo
            else {
                URL bundleURL = getClass().getClassLoader().getResource(fileLocation);
                if(bundleURL != null) {

                    String bundlePath = bundleURL.getPath();
                    InputStream bundleStream = getClass().getClassLoader().getResourceAsStream(fileLocation);
                    bundle = context.installBundle(bundlePath,bundleStream);
                }
            }
            if(bundle != null) {
                bundleID = bundle.getBundleId();
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
*/


    public String addPlugin(String pluginName, String jarFile, Map<String,Object> map) {
        String returnPluginID = null;
            try {
                    String pluginID = addConfig(pluginName, map);
                        if (pluginID != null) {
                            PluginNode pluginNode = new PluginNode(pluginID, pluginName, jarFile, map);
                            synchronized (lockPlugin) {
                                pluginMap.put(pluginID, pluginNode);
                            }
                            returnPluginID = pluginID;
                        } else {
                            System.out.println("Could not create config for " + " pluginName " + pluginName + " no bundle " + jarFile);
                        }
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        return returnPluginID;
    }

    public String addConfig(String pluginName, Map<String,Object> map) {

        String pluginID = null;
        try {


            boolean isEmpty = false;
            int id = 0;
            while (!isEmpty) {

                synchronized (lockConfig) {
                    if (!configMap.containsKey("plugin/" + id)) {
                        pluginID = "plugin/" + id;
                        //Configuration configuration = confAdmin.createFactoryConfiguration(pluginName + ".Plugin", null);
                        //Dictionary properties = new Hashtable();

                        //((Hashtable) properties).putAll(map);

                        //properties.put("pluginID", pluginID);
                        //configuration.update(properties);
                        map.put("pluginID",pluginID);
                        configMap.put(pluginID, map);
                        isEmpty = true;
                    }
                }
                id++;
            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return pluginID;
    }

   /* public boolean startPlugin(String pluginID) {
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
                            try {
                                ps.isStarted();
                            } catch(Exception ex) {
                                System.out.println("Could not start!");
                                ex.printStackTrace();
                            }

                            synchronized (lockPlugin) {
                                if (pluginMap.containsKey(pluginID)) {
                                    pluginMap.get(pluginID).setPluginService((PluginService) context.getService(sr));
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
                System.out.println("COULD NOT START PLUGIN COULD NOT GET SERVICE");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return isStarted;
    }*/

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

                    int status = pluginNode.getStatus_code();
                    boolean isActive = pluginNode.getActive();

                    Map<String, String> configMap = new HashMap<>();

                    configMap.put("status", String.valueOf(status));
                    configMap.put("isactive", String.valueOf(isActive));
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

    //Not normally available, added for testbed
    public Map<String,PluginNode> getPluginMap(){
        return pluginMap;
    }

}
