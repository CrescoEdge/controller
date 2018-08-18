package io.cresco.agent.controller.measurement;

import com.google.gson.Gson;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.metrics.CrescoMeterRegistry;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MeasurementEngine {


    private ControllerEngine controllerEngine;
    private PluginBuilder plugin;
    private CLogger logger;
    private CrescoMeterRegistry crescoMeterRegistry;
    //private SimpleMeterRegistry crescoMeterRegistry;
    //private CompositeMeterRegistry crescoMeterRegistry;
    //private MeterRegistry crescoMeterRegistry;
    //private Timer msgRouteTimer;

    private Gson gson;
    private Map<String,CMetric> metricMap;

    public MeasurementEngine(ControllerEngine controllerEngine) {
        this.controllerEngine = controllerEngine;
        this.plugin = controllerEngine.getPluginBuilder();
        this.logger = plugin.getLogger(MeasurementEngine.class.getName(), CLogger.Level.Info);

        this.metricMap = new ConcurrentHashMap<>();

        gson = new Gson();

        //crescoMeterRegistry = new SimpleMeterRegistry();
        //crescoMeterRegistry = new CompositeMeterRegistry();
        crescoMeterRegistry = plugin.getCrescoMeterRegistry();


        metricInit();

        //logger.error("STARTED M ENGINE");
       // MetricRegistry metricRegistry = new MetricRegistry();
        /*
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .inDomain("cresco")
                .build();
        */
        //jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM, HierarchicalNameMapper.DEFAULT, metricRegistry, jmxReporter);

        //crescoMeterRegistry = new CrescoMeterRegistry(agentcontroller,CrescoConfig.DEFAULT, Clock.SYSTEM);

        //JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

        /*
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        new ClassLoaderMetrics().bindTo(jmxMeterRegistry);
        new JvmMemoryMetrics().bindTo(jmxMeterRegistry);
        new JvmGcMetrics().bindTo(jmxMeterRegistry);
        new ProcessorMetrics().bindTo(jmxMeterRegistry);
        new JvmThreadMetrics().bindTo(jmxMeterRegistry);
        */
        //this.msgRouteTimer = this.jmxMeterRegistry.timer("cresco_message.transaction.time");
        //this.msgRouteTimer = this.crescoMeterRegistry.timer("cresco_message.transaction.time");

        //JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);


        new ClassLoaderMetrics().bindTo(crescoMeterRegistry);
        new JvmMemoryMetrics().bindTo(crescoMeterRegistry);
        //new JvmGcMetrics().bindTo(crescoMeterRegistry);
        new ProcessorMetrics().bindTo(crescoMeterRegistry);
        new JvmThreadMetrics().bindTo(crescoMeterRegistry);

        //todo enable metrics
        //initInternal();


        //Queue<String> receivedMessages = crescoMeterRegistry.gauge("unprocessed.messages", new ConcurrentLinkedQueue<>(), ConcurrentLinkedQueue::size);

        /*
        for (Map.Entry<String, CMetric> entry : metricMap.entrySet()) {
            //String name = entry.getKey();
            CMetric metric = entry.getValue();
            //logger.error(metric.name + " " + metric.group + " " + metric.className + " " + metric.type.name());
            logger.info(writeMetricString(metric));

        }
        */

        /*
        for(Meter m : crescoMeterRegistry.getMeters()) {
            logger.error(m.getId().getName() + " " + m.getId().getDescription() + " " + m.getId().getType().name());

        }
        */

    }

    public List<Map<String,String>> getMetricGroupList(String group) {
        List<Map<String,String>> returnList = null;
        try {
            returnList = new ArrayList<>();

            for (Map.Entry<String, CMetric> entry : metricMap.entrySet()) {
                CMetric metric = entry.getValue();
                if(metric.group.equals(group)) {
                    returnList.add(writeMetricMap(metric));
                }
            }
        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return returnList;
    }

    public Map<String,String> writeMetricMap(CMetric metric) {

        Map<String,String> metricValueMap = null;

        try {
            metricValueMap = new HashMap<>();

            if (Meter.Type.valueOf(metric.className) == Meter.Type.GAUGE) {

                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                metricValueMap.put("value",String.valueOf(crescoMeterRegistry.get(metric.name).gauge().value()));



            } else if (Meter.Type.valueOf(metric.className) == Meter.Type.TIMER) {
                TimeUnit timeUnit = crescoMeterRegistry.get(metric.name).timer().baseTimeUnit();
                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                metricValueMap.put("mean",String.valueOf(crescoMeterRegistry.get(metric.name).timer().mean(timeUnit)));
                metricValueMap.put("max",String.valueOf(crescoMeterRegistry.get(metric.name).timer().max(timeUnit)));
                metricValueMap.put("totaltime",String.valueOf(crescoMeterRegistry.get(metric.name).timer().totalTime(timeUnit)));
                metricValueMap.put("count",String.valueOf(crescoMeterRegistry.get(metric.name).timer().count()));
                }

            else if (Meter.Type.valueOf(metric.className) == Meter.Type.DISTRIBUTION_SUMMARY) {
                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                metricValueMap.put("mean",String.valueOf(crescoMeterRegistry.get(metric.name).summary().mean()));
                metricValueMap.put("max",String.valueOf(crescoMeterRegistry.get(metric.name).summary().max()));
                metricValueMap.put("totaltime",String.valueOf(crescoMeterRegistry.get(metric.name).summary().totalAmount()));
                metricValueMap.put("count",String.valueOf(crescoMeterRegistry.get(metric.name).summary().count()));
            }

            else  if (Meter.Type.valueOf(metric.className) == Meter.Type.COUNTER) {
                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                try {
                    metricValueMap.put("count", String.valueOf(crescoMeterRegistry.get(metric.name).functionCounter().count()));
                } catch (Exception ex) {
                    metricValueMap.put("count", String.valueOf(crescoMeterRegistry.get(metric.name).counter().count()));
                }

            } else {
                logger.error("NO WRITER FOUND " + metric.className);
            }

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return metricValueMap;
    }

    private void initInternal() {

        Map<String,String> internalMap = new HashMap<>();

        internalMap.put("jvm.memory.max", "controller");
        internalMap.put("jvm.memory.used", "controller");
        internalMap.put("jvm.memory.committed", "controller");
        internalMap.put("jvm.buffer.memory.used", "controller");
        internalMap.put("jvm.threads.daemon", "controller");
        internalMap.put("jvm.threads.live", "controller");
        internalMap.put("jvm.threads.peak", "controller");
        internalMap.put("jvm.classes.loaded", "controller");
        internalMap.put("jvm.classes.unloaded", "controller");
        internalMap.put("jvm.buffer.total.capacity", "controller");
        internalMap.put("jvm.buffer.count", "controller");
        internalMap.put("system.load.average.1m", "controller");
        internalMap.put("system.cpu.count", "controller");
        internalMap.put("system.cpu.usage", "controller");
        internalMap.put("process.cpu.usage", "controller");

        for (Map.Entry<String, String> entry : internalMap.entrySet()) {
            String name = entry.getKey();
            String group = entry.getValue();
            setExisting(name,group);
        }



    }

    public void setExisting(String name, String group) {

        Meter m = crescoMeterRegistry.get(name).meter();

        if(m != null) {

            metricMap.put(name,new CMetric(name,m.getId().getDescription(),group,m.getId().getType().name()));
            //metricMap.put(name,new CMetric(name,m.getId().getDescription(),group,m.getClass().getSimpleName()));

            /*
            if (m instanceof TimeGauge) {
                //return writeGauge((TimeGauge) m, wallTime);
            } else if (m instanceof Gauge) {
                //return writeGauge((Gauge) m, wallTime);
            } else if (m instanceof Counter) {
                //return writeCounter((Counter) m, wallTime);
            } else if (m instanceof FunctionCounter) {
                //return writeCounter((FunctionCounter) m, wallTime);
            } else if (m instanceof Timer) {
                //return writeTimer((Timer) m, wallTime);
            } else if (m instanceof FunctionTimer) {
                //return writeTimer((FunctionTimer) m, wallTime);
            } else if (m instanceof DistributionSummary) {
                //return writeSummary((DistributionSummary) m, wallTime);
            } else if (m instanceof LongTaskTimer) {
                //return writeLongTaskTimer((LongTaskTimer) m, wallTime);
            }
            */


        }

    }

    public Boolean setTimer(String name, String description, String group) {

        if(metricMap.containsKey(name)) {
            return false;
        } else {
            //this.crescoMeterRegistry.timer()

            Timer timer = Timer.builder(name).description(description).register(crescoMeterRegistry);
            //this.crescoMeterRegistry.
            //Timer timer = this.crescoMeterRegistry.timer(agentcontroller.getPluginID() + "_" + name);
            //timer.takeSnapshot().
            metricMap.put(name,new CMetric(name,description,group,"TIMER"));
            return true;
        }
    }

    public Timer getTimer(String name) {
        if(metricMap.containsKey(name)) {
            return this.crescoMeterRegistry.timer(name);
        } else {
            return null;
        }
    }

    public void updateTimer(String name, long timeStamp) {
        getTimer(name).record(System.nanoTime() - timeStamp, TimeUnit.NANOSECONDS);
    }

    public Gauge getGauge(String name) {
        if(metricMap.containsKey(name)) {
            return this.crescoMeterRegistry.get(name).gauge();
        } else {
            return null;
        }
    }

    public Gauge getGaugeRaw(String name) {
            return this.crescoMeterRegistry.get(name).gauge();
    }

    private void metricInit() {

        setTimer("message.transaction.time", "The timer for messages", "controller");


    }

    public void initRegionalMetrics() {

        if(controllerEngine.getBrokeredAgents() != null) {

            Gauge.builder("brokered.agent.count", controllerEngine.getBrokeredAgents(), ConcurrentHashMap::size)
                    .description("The number of currently brokered agents.")
                    .register(crescoMeterRegistry);
        }
    }

    public void initGlobalMetrics() {
        if(controllerEngine.getResourceScheduleQueue() != null) {
            Gauge.builder("incoming.resource.queue", controllerEngine.getResourceScheduleQueue(), BlockingQueue::size)
                    .description("The number of queued incoming resources to be scheduled.")
                    .register(crescoMeterRegistry);
        }

        if(controllerEngine.getAppScheduleQueue() != null) {
            Gauge.builder("incoming.application.queue", controllerEngine.getAppScheduleQueue(), BlockingQueue::size)
                    .description("The number of queued incoming applications to be scheduled.")
                    .register(crescoMeterRegistry);
        }
    }


}
