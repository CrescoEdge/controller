package io.cresco.agent.data;

import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.output.sink.InMemorySink;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CEPEngine {

    private PluginBuilder plugin;
    private CLogger logger;

    private SiddhiManager siddhiManager;
    private Map<String,CEPInstance> cepMap;
    private AtomicBoolean lockCEP = new AtomicBoolean();


    public CEPEngine(PluginBuilder pluginBuilder) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(CEPEngine.class.getName(),CLogger.Level.Info);

        cepMap = Collections.synchronizedMap(new HashMap<>());

        // Creating Siddhi Manager
        siddhiManager = new SiddhiManager();

        try {
            InMemorySink sink = new InMemorySink();
            sink.connect();
        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }

    public CEPInstance getCEPInstance(String cepId) {
        CEPInstance cepInstance = null;
        try {
            synchronized (lockCEP) {
                if(cepMap.containsKey(cepId)) {
                    cepInstance = cepMap.get(cepId);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return cepInstance;
    }

    public boolean removeCEP(String cepId) {
        boolean isRemoved = false;
        synchronized (lockCEP) {
            if(cepMap.containsKey(cepId)) {
                cepMap.get(cepId).shutdown();
            }
            cepMap.remove(cepId);
            isRemoved = true;
        }
        return isRemoved;
    }

    public void shutdown() {
        try {

            synchronized (lockCEP) {
                for (Map.Entry<String, CEPInstance> entry : cepMap.entrySet()) {
                    //String key = entry.getKey();
                    CEPInstance value = entry.getValue();
                    value.shutdown();
                }
            }

            if(siddhiManager != null) {
                siddhiManager.shutdown();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void clear() {
        try {
            synchronized (lockCEP) {
                for (Map.Entry<String, CEPInstance> entry : cepMap.entrySet()) {
                    //String key = entry.getKey();
                    CEPInstance value = entry.getValue();
                    value.clear();
                }
            }

            if(siddhiManager != null) {
                siddhiManager.shutdown();
                siddhiManager = new SiddhiManager();
            }


        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

        public boolean createCEP(String cepId, String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {

        boolean isCreated = false;
        try {

            CEPInstance cepInstance = new CEPInstance(plugin,siddhiManager,cepId, inputStreamName, inputStreamDefinition, outputStreamName, outputStreamDefinition, queryString);

            synchronized (lockCEP) {
                cepMap.put(cepId,cepInstance);
            }
            isCreated = true;

            } catch (Exception ex) {
            ex.printStackTrace();
        }
        return isCreated;
    }

    public void input(String cepId, String streamName, String jsonPayload) {
        try {

            synchronized (lockCEP) {
                if(cepMap.containsKey(cepId)) {
                    cepMap.get(cepId).input(streamName, jsonPayload);
                } else {
                    logger.error("cepId: " + cepId + " does not exist!");
                }
            }


        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

}
