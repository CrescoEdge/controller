package io.cresco.agent.core;


import io.cresco.library.agent.AgentService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import java.io.File;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;

public class StaticAgentLoader implements Runnable  {

    private ConfigurationAdmin confAdmin;
    private BundleContext context;
    private boolean isAgentActive = false;


    public StaticAgentLoader(BundleContext context) {
        this.context = context;
    }


    public void run() {

            try {

                String agentID = agentInit();


                if(agentID != null) {

                    if(startAgent(context, agentID)) {
                        System.out.println("Started agent " + agentID);
                    } else {
                        System.out.println("Unable to start agent " + agentID);
                    }

                } else {
                    System.out.println("Could not init agent!");
                }


                while(isAgentActive) {
                    Thread.sleep(5000);
                }


            } catch(Exception ex) {
                ex.printStackTrace();
            }

    }

    private String agentInit() {
        String returnAgentID = null;
        try {

            String agentConfig = System.getProperty("agentConfig");

            if (agentConfig == null) {
                agentConfig = "conf/agent.ini";
            }

            File configFile = new File(agentConfig);
            if (configFile.isFile()) {

                //Agent Config
                Config config = config = new Config(configFile.getAbsolutePath());
                Map<String, Object> map = config.getConfigMap();

                setConfAdmin(context);

                if (confAdmin != null) {
                    //Configuration configuration = confAdmin.createFactoryConfiguration("io.cresco.library.agent.AgentService", null);
                    Configuration configuration = confAdmin.createFactoryConfiguration("io.cresco.AgentServiceImpl", null);

                    Dictionary properties = new Hashtable();

                    ((Hashtable) properties).putAll(map);
                    String agentID = UUID.randomUUID().toString();
                    properties.put("agentID", agentID);
                    configuration.update(properties);

                    returnAgentID = agentID;

                }


            } else {
                System.out.println("NO CONFIG FILE " + agentConfig + " FOUND! ");
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return returnAgentID;
    }

    private void setConfAdmin(BundleContext context) {
        try {
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
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public boolean startAgent(BundleContext context, String agentID) {
        boolean isStarted = false;
        try {
            ServiceReference<?>[] servRefs = null;

            while (servRefs == null) {

                String filterString = "(agentID=" + agentID + ")";
                Filter filter = context.createFilter(filterString);

                servRefs = context.getServiceReferences(AgentService.class.getName(), filterString);

                if (servRefs == null || servRefs.length == 0) {
                    System.out.println("NULL FOUND NOTHING!");
                } else {
                    System.out.println("Running Service Count: " + servRefs.length);

                    for (ServiceReference sr : servRefs) {
                        boolean assign = servRefs[0].isAssignableTo(context.getBundle(), AgentService.class.getName());
                        if(assign) {
                            System.out.println("Can Assign Service : " + assign);
                            AgentService as = (AgentService)context.getService(sr);
                            //LoaderService ls = (LoaderService) context.getService(sr);
                        } else {
                            System.out.println("Can't Assign Service : " + assign);
                        }
                        //Check agent here

                        isStarted = true;

                    }
                }
                Thread.sleep(1000);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return isStarted;
    }



}
