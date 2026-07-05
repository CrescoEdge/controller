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

    private volatile SiddhiManager siddhiManager;
    private Map<String,CEPInstance> cepMap;
    private AtomicBoolean lockCEP = new AtomicBoolean();
    private InMemorySink sink;
    // SiddhiManager init (loading the whole extension registry) takes several seconds; do it in the
    // background so the plugin activates immediately, and gate CEP creation on it being ready.
    private final java.util.concurrent.CountDownLatch siddhiLatch = new java.util.concurrent.CountDownLatch(1);

    public CEPEngine(PluginBuilder pluginBuilder) {

        this.plugin = pluginBuilder;
        logger = plugin.getLogger(CEPEngine.class.getName(),CLogger.Level.Info);

        cepMap = Collections.synchronizedMap(new HashMap<>());

        // Build the SiddhiManager (which loads the whole extension registry via a classindex scan
        // over the bundled extensions) on a background thread so plugin activation is not blocked by
        // the multi-second init. Siddhi resolves extensions via the *thread context* classloader, so
        // pin the TCCL to Siddhi's own classloader (this bundle) or the scan misses its extensions
        // and fails with ExtensionNotFoundException. createCEP() awaits this init.
        Thread init = new Thread(() -> {
            ClassLoader prevTccl = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(SiddhiManager.class.getClassLoader());
                siddhiManager = new SiddhiManager();
                try {
                    sink = new InMemorySink();
                    sink.connect();
                } catch (Exception ex) {
                    logger.error("CEPEngine sink init error", ex);
                }
                logger.info("CEP SiddhiManager ready");
            } catch (Throwable t) {
                logger.error("CEPEngine SiddhiManager init error", t);
            } finally {
                Thread.currentThread().setContextClassLoader(prevTccl);
                siddhiLatch.countDown();
            }
        }, "cep-siddhi-init");
        init.setDaemon(true);
        init.start();
    }

    // Block until the background SiddhiManager init completes (or times out).
    private boolean awaitSiddhi() {
        try {
            return siddhiLatch.await(120, java.util.concurrent.TimeUnit.SECONDS) && siddhiManager != null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
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
                sink.shutdown();
            }

            if(siddhiManager != null) {
                siddhiManager.shutdown();
            }

        } catch (Exception ex) {
            logger.error("CEPEngine.shutdown error", ex);
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
            logger.error("CEPEngine.clear error", ex);
        }
    }

        public boolean createCEP(String cepId, String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {

        boolean isCreated = false;
        if(!awaitSiddhi()) {
            logger.error("CEPEngine.createCEP: SiddhiManager not ready");
            return false;
        }
        try {

            CEPInstance cepInstance = new CEPInstance(plugin,siddhiManager,cepId, inputStreamName, inputStreamDefinition, outputStreamName, outputStreamDefinition, queryString);

            synchronized (lockCEP) {
                cepMap.put(cepId,cepInstance);
            }
            isCreated = true;

            } catch (Exception ex) {
            logger.error("CEPEngine.createCEP error", ex);
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
            logger.error("CEPEngine.input error", ex);
        }
    }

}
