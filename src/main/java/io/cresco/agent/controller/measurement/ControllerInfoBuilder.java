package io.cresco.agent.controller.measurement;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;

class ControllerInfoBuilder {
    private Gson gson;

    private MBeanServer server;
    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;

    public ControllerInfoBuilder(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(ControllerInfoBuilder.class.getName(),CLogger.Level.Info);
        gson = new Gson();
        server = ManagementFactory.getPlatformMBeanServer();
    }

    public String getControllerInfoMap() {

        String returnStr = null;
        try {

            Map<String,List<Map<String,String>>> info = new HashMap<>();
            info.put("controller", controllerEngine.getMeasurementEngine().getMetricGroupList("controller"));

            Map<String,String> metricsMap = new HashMap<>();
            metricsMap.put("name","controller_group");
            metricsMap.put("metrics",gson.toJson(info));

            List<Map<String,String>> metricsList = new ArrayList<>();
            metricsList.add(metricsMap);

            returnStr = gson.toJson(metricsList);

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return returnStr;
    }


    public String getControllerInfoMapOld() {

        String returnStr = null;
        try {
            StringBuilder sb = new StringBuilder();

            Set<ObjectInstance> instances = server.queryMBeans(null, null);

            Iterator<ObjectInstance> iterator = instances.iterator();
            while (iterator.hasNext()) {
                ObjectInstance instance = iterator.next();
                sb.append(instance.getObjectName() + "\n");
                if(instance.getObjectName().toString().contains("agent-")) {
                    try {

                        //MBeanInfo info = server.getMBeanInfo(instance.getObjectName());
                        //MBeanAttributeInfo[] attrInfo = info.getAttributes();
                        //(ObjectName[])server.getAttribute(http, attr.getName()))
                        //logger.info(server.getAttribute(instance.getObjectName(), "Subscriptions"));
                        for(ObjectName on : (ObjectName[])server.getAttribute(instance.getObjectName(), "Subscriptions")) {
                            logger.info(on.getCanonicalName());
                        }
                        //WriteAttributes(server, instance.getObjectName(), false);

                    } catch(Exception ex) {
                        logger.error("get error " + ex.getMessage());
                        ex.printStackTrace();
                    }
                }
                //System.out.println("MBean Found:");
                //System.out.println("Class Name:t" + instance.getClassName());
                //System.out.println("Object Name:t" + instance.getObjectName());
                //System.out.println("****************************************");
                //if(instance.getClassName().equals("com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManagerMBean")) {

            /*
            try {
                WriteAttributes(server, instance.getObjectName());
            } catch(Exception ex) {

            }
            */

                //}
            }
            returnStr = sb.toString();

        } catch(Exception ex) {

        }
        return returnStr;
    }

    private void WriteAttributes(final MBeanServer mBeanServer, final ObjectName http, Boolean indent)
            throws InstanceNotFoundException, IntrospectionException, ReflectionException
    {
        MBeanInfo info = mBeanServer.getMBeanInfo(http);
        MBeanAttributeInfo[] attrInfo = info.getAttributes();

        logger.error("\n --Attributes for object: " + http);
        for (MBeanAttributeInfo attr : attrInfo)
        {
            try {
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                //System.out.println(attr.getName() + "=" + server.getAttribute(http, attr.getName()) + " " + indent);
                if(attr.getName().equals("Subscriptions")){

                    for(ObjectName on : (ObjectName[])server.getAttribute(http, attr.getName())) {

                        //System.out.println("--- " + on.toString());
                        try {
                            WriteAttributes(mBeanServer, on, true);
                        } catch(Exception ex) {
                            logger.error("Error Write Attb 2: " + ex.toString());

                            ex.printStackTrace();
                        }
                    }
                    /*
                    System.out.println("--- " + server.getAttribute(http, attr.getName()).getClass().getCanonicalName());
                    System.out.println("--- " + server.getAttribute(http, attr.getName()).getClass().getName());
                    System.out.println("--- " + server.getAttribute(http, attr.getName()).getClass().getTypeName());
                    System.out.println("--- " + server.getAttribute(http, attr.getName()).getClass().getSimpleName());
                    */

                }
            } catch(Exception ex) {
                logger.error("Error Write Attb : " + ex.toString());
                ex.printStackTrace();
            }
            //System.out.println("  " + attr.getName() + "\n");
        }
    }


}
