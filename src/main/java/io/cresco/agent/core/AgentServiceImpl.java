package io.cresco.agent.core;

import io.cresco.agent.controller.agentcontroller.PluginAdmin;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.agent.data.DataPlaneLogger;
import io.cresco.agent.db.DBEngine;
import io.cresco.agent.db.DBInterfaceImpl;
import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.*;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;


/*
 * Not published by DS (service = {}): activate() registers AgentService itself, through a service factory, so every bundle
 * gets its own instance (BundleAgentService) and the controller knows which bundle is calling. The controller bundle gets
 * this object.
 */
@Component(
        service = {} ,
        immediate = true,
        reference=@Reference(name="ConfigurationAdmin", service=ConfigurationAdmin.class)
)

public class AgentServiceImpl implements AgentService {

    private volatile org.osgi.framework.ServiceRegistration<?> agentServiceRegistration;
    private volatile long controllerBundleId = -1;

    private ControllerEngine controllerEngine;
    private ControllerStateImp cstate;
    private AgentState agentState;
    private PluginBuilder plugin;
    private PluginAdmin pluginAdmin;
    private DBEngine dbe;
    private DBInterfaceImpl gdb;
    private CLogger logger;
    private DataPlaneLogger dataPlaneLogger;

    //this needs to be pulled from Config
    private String ENV_PREFIX = "CRESCO_";


    public AgentServiceImpl() {


    }


    public CLogger getCLogger(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName, CLogger.Level level) {
        return new CLoggerImpl(pluginBuilder,baseClassName,issuingClassName, dataPlaneLogger);


    }

    public CLogger getCLogger(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName) {
        return new CLoggerImpl(pluginBuilder,baseClassName,issuingClassName, dataPlaneLogger);

    }

    private java.util.List<String> secretSourceNotes = java.util.Collections.emptyList();

    private Map<String,Object> initAgentConfigMap() {
        Map<String, Object> configParams = null;
        try {

            configParams = new HashMap<>();

            String agentConfig = System.getProperty("agentConfig");

            if (agentConfig == null) {
                agentConfig = "conf/agent.ini";
            }

            File configFile = new File(agentConfig);
            Config config = null;
            if (configFile.isFile()) {

                //Agent Config
                config = new Config(configFile.getAbsolutePath());
                configParams = config.getConfigMap();

            }

            /*
            String configMsg = "Property > Env";

            if (config == null) {
                configParams = new HashMap<>();
            } else {
                configMsg = "Property > Env > " + configFile;
            }
            */


            // OUT-03b: secret params from <param>_file (owner-only file) or CRESCO_<PARAM>, ahead of -D
            secretSourceNotes = SecretSources.resolve(configParams, System.getenv(), System.getProperties());

            String platform = System.getenv("CRESCO_PLATFORM");
            if (platform == null) {

                if(config != null) {
                    platform = config.getStringParams("general", "platform");
                }

                if (platform == null) {
                    platform = "unknown";
                }
            }

            configParams.put("platform", platform);
            //enableMsg.setParam("platform", platform);

            String environment = System.getenv("CRESCO_ENVIRONMENT");
            if (environment == null) {

                if(config != null) {
                    environment = config.getStringParams("general", "environment");
                }

                if (environment == null) {
                    try {
                        environment = System.getProperty("os.name");
                    } catch (Exception ex) {
                        environment = "unknown";
                    }
                }
            }
            //enableMsg.setParam("environment", environment);
            configParams.put("environment", environment);

            String location = System.getenv("CRESCO_LOCATION");
            if(location == null) {

                if(config != null) {
                    location = config.getStringParams("general", "location");
                }
            }
            if (location == null) {

                try {
                    location = InetAddress.getLocalHost().getHostName();
                    if (location != null) {
                        //logger.info("Location set: " + location);
                    }
                } catch (Exception ex) {
                    //logger.error("getLocalHost() Failed : " + ex.getMessage());
                }

                if (location == null) {
                    try {

                        String osType = System.getProperty("os.name").toLowerCase();
                        if (osType.equals("windows")) {
                            location = System.getenv("COMPUTERNAME");
                        } else if (osType.equals("linux")) {
                            location = System.getenv("HOSTNAME");
                        }

                        if (location != null) {
                            //logger.info("Location set env: " + location);
                        }

                    } catch (Exception exx) {
                        //do nothing
                        //logger.error("Get System Env Failed : " + exx.getMessage());
                    }
                }
            }
            if (location == null) {
                location = "unknown";
            }
            //enableMsg.setParam("location", location);
            configParams.put("location", location);
        } catch (SecretSources.SecretSourceException sse) {
            // an unusable secret file: refuse to start (the logger does not exist yet)
            System.err.println("Cresco agent: " + sse.getMessage());
            throw sse;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }
        return configParams;
    }

    @Override
    public String getAgentDataDirectory() {
        String agentDataDirectory = null;
        try {

            String cresco_data_location = System.getProperty("cresco_data_location");
            if(cresco_data_location != null) {

                if(plugin != null) {
                    agentDataDirectory = plugin.getConfig().getStringParam("agent_data_directory", cresco_data_location);
                } else {
                    agentDataDirectory = cresco_data_location;
                }

            } else {

                if(plugin != null) {
                    agentDataDirectory = plugin.getConfig().getStringParam("agent_data_directory", FileSystems.getDefault().getPath("cresco-data").toAbsolutePath().toString());
                } else {
                    agentDataDirectory = FileSystems.getDefault().getPath("cresco-data").toAbsolutePath().toString();
                }

            }




        } catch (Exception ex) {
            if(logger != null) {
                logger.error("getAgentDataDirectory()", ex);
            } else {
                ex.printStackTrace();
            }
        }
        return agentDataDirectory;
    }

    @Activate
    void activate(BundleContext context) {

    //AgentServiceImpl agentService = this;

        Map<String,Object> configParams = initAgentConfigMap();

        //create plugin
        plugin = new PluginBuilder(this, this.getClass().getName(), context, configParams);

        // controller#23: a thread killed by an OutOfMemoryError (or any VirtualMachineError) halts the JVM, so the
        // agent is restarted instead of living on without its broker threads
        FatalErrorGuard.install(plugin.getLogger(FatalErrorGuard.class.getName(), CLogger.Level.Info));

        //create dataplane logger
        dataPlaneLogger = new DataPlaneLogger(plugin);

        //starting database
        dbe = new DBEngine(plugin);

        //create controller database implementation
        gdb = new DBInterfaceImpl(plugin, dbe);

        //control state
        cstate = new ControllerStateImp(plugin, dbe);

        //agent state
        agentState = new AgentState(cstate);

        //create admin
        pluginAdmin = new PluginAdmin(this, plugin, agentState, gdb, context, dataPlaneLogger);

        //create logger
        logger = plugin.getLogger("agent:io.cresco.agent.core.agentservice", CLogger.Level.Info);
        //setLogLevel("agent:io.cresco.agent.core.agentservice", CLogger.Level.Info);
        //pluginAdmin.setLogLevel("agent:io.cresco.agent.core.agentservice", CLogger.Level.Info);

        logger.info("");
        logger.info("       ________   _______      ________   ________   ________   ________");
        logger.info("      /  _____/  /  ___  |    /  _____/  /  _____/  /  _____/  /  ___   /");
        logger.info("     /  /       /  /__/  /   /  /__     /  /___    /  /       /  /  /  /");
        logger.info("    /  /       /  __   /    /  ___/    /____   /  /  /       /  /  /  /");
        logger.info("   /  /____   /  /  |  |   /  /____   _____/  /  /  /____   /  /__/  /");
        logger.info("  /_______/  /__/   |__|  /_______/  /_______/  /_______/  /________/");
        logger.info("");
        logger.info("");

        for (String note : secretSourceNotes) {
            if (note.startsWith("WARN: ")) logger.warn(note.substring(6)); else logger.info(note);
        }

        logger.info("Controller Version: " + getControllerVersion());
        if(plugin.getConfig().getStringParam("cresco_data_location") != null) {
            logger.info("Storage Path: " + plugin.getConfig().getStringParam("cresco_data_location"));
        }
        controllerEngine = new ControllerEngine(cstate, plugin, pluginAdmin, gdb);

        logSecurityModeWarnings();
        registerAgentServiceFactory(context);



        (new Thread() {
        public void run() {
            try {

                //core init needs to go here
                if(controllerEngine.start()) {
                    logger.info("Controller Completed Startup");
                } else if (controllerEngine.isRefusedByConfiguration()) {
                    // refuse to start: exit non-zero (78, EX_CONFIG) so a supervisor sees a configuration error, not a hang
                    logger.error("Controller REFUSED to start: its security configuration is inconsistent (see above). Exiting.");
                    System.exit(78);
                    return;
                } else {
                    logger.error("Controlled Failed Startup : Exiting");
                }

                while(!controllerEngine.cstate.isActive()) {
                    logger.info("Waiting for controller to become active...");
                    logger.info("Controller State = " + controllerEngine.cstate.getControllerState().toString());

                    Thread.sleep(1000);
                }

                plugin.setIsActive(true);


            } catch(Exception ex) {
                logger.error("activate() controller startup thread", ex);
            }
        }
    }).start();

    }

    @Deactivate
    void deactivate(BundleContext context) {

        if(logger != null) {
            logger.info("Starting Controller Shutdown");
        }
        org.osgi.framework.ServiceRegistration<?> reg = agentServiceRegistration;
        agentServiceRegistration = null;
        if (reg != null) {
            try { reg.unregister(); } catch (Exception ignore) { }
        }

        if(controllerEngine != null) {

            if(!controllerEngine.stop()) {
                if(logger != null) {
                    logger.error("deactivate() ControllerEngine stop() was dirty!");
                }
            }

        }

        if(logger != null) {
            logger.info("Controller Shutdown Completed");
        }

    }

    @Modified
    void modified(BundleContext context, Map<String,Object> map) {
        logger.info("Modified Config Map PluginID:" + map.get("pluginID"));
    }

    @Override
    public void setLogLevel(String logId, CLogger.Level level) {

        //set log level for files
        if(pluginAdmin != null) {
            pluginAdmin.setLogLevel(logId, level);
        }

    }

    public String getControllerVersion() {
        String version = null;
        try{

            String jarURLString = new File(AgentServiceImpl.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            if(jarURLString.contains("!/")) {
                jarURLString = "jar:file://" + jarURLString;
                URL inputURL = new URL(jarURLString);
                JarInputStream jarStream = null;

                try {

                    if (inputURL != null) {
                        JarURLConnection conn = (JarURLConnection) inputURL.openConnection();
                        InputStream in = conn.getInputStream();

                        jarStream = new JarInputStream(in);
                        Manifest mf = jarStream.getManifest();
                        //Manifest mf = conn.getManifest();
                        if (mf != null) {
                            Attributes mainAttribs = mf.getMainAttributes();
                            if (mainAttribs != null) {
                                version = mainAttribs.getValue("Bundle-Version");
                            }
                        }
                        if (in != null) {
                            in.close();
                        }
                    
                    }
                } catch(Exception ex) {
                    logger.error("getControllerVersion() manifest read", ex);
                } finally {

                    if(jarStream != null) {
                        jarStream.close();
                    }
                }


            } else {
                version = plugin.getPluginVersion(jarURLString);
            }

        }
        catch(Exception ex)
        {
            logger.error("getControllerVersion()", ex);

        }
        return version;
    }

    @Override
    public AgentState getAgentState() {
        return agentState;
    }


    /**
     * The controller's own send (its PluginBuilder holds this object): whatever the message says about its caller is
     * replaced by this controller.
     */
    @Override
    public void msgOut(String id, MsgEvent msg) {
        try {
            controllerEngine.stampSelf(msg);
            controllerEngine.msgIn(msg);
        } catch(Exception ex) {
            logger.error(msg.printHeader());
            logger.error(io.cresco.agent.controller.communication.ParamLog.describe(msg));

            logger.error("msgOut()", ex);
        }
    }

    // ------------------------------------------------------------------ per-bundle service (verified callers)

    private void registerAgentServiceFactory(BundleContext context) {
        if (context == null || context.getBundle() == null) {
            logger.error("no bundle context: the AgentService is not registered (only a test runs the controller this way)");
            return;
        }
        controllerBundleId = context.getBundle().getBundleId();
        final AgentServiceImpl core = this;
        agentServiceRegistration = context.registerService(AgentService.class.getName(),
                new org.osgi.framework.ServiceFactory<AgentService>() {
                    @Override
                    public AgentService getService(org.osgi.framework.Bundle bundle, org.osgi.framework.ServiceRegistration<AgentService> r) {
                        // review F5: never the controller's own instance, not even for the controller bundle (any bundle can
                        // borrow its BundleContext): the controller uses its own reference, never the registry. The
                        // controller bundle runs no plugin, so whatever is sent through this instance is dropped.
                        return new BundleAgentService(core, bundle);
                    }

                    @Override
                    public void ungetService(org.osgi.framework.Bundle bundle, org.osgi.framework.ServiceRegistration<AgentService> r,
                                             AgentService service) { }
                }, null);
    }

    private final java.util.concurrent.ConcurrentHashMap<String, Long> refusalLog = new java.util.concurrent.ConcurrentHashMap<>();

    /** One WARN line per kind of refusal per minute. */
    private void refused(String kind, String detail) {
        long now = System.currentTimeMillis();
        Long last = refusalLog.get(kind);
        if (last == null || now - last > 60_000L) {
            refusalLog.put(kind, now);
            if (logger != null) logger.warn("REFUSED " + kind + ": " + detail);
        }
    }

    /**
     * A plugin's send: it goes out as {@code plugin:<tenant>/<region>/<agent>/<id>}, and only when {@code bundle} runs the
     * plugin {@code id}; otherwise it is dropped (logged).
     */
    void msgOutFromBundle(org.osgi.framework.Bundle bundle, String id, MsgEvent msg) {
        if (msg == null) return;
        if (pluginAdmin == null || !pluginAdmin.bundleRunsPlugin(bundle.getBundleId(), id)) {
            refused("send as a plugin the bundle does not run", bundle.getSymbolicName() + " sent as plugin " + id
                    + " (it runs " + (pluginAdmin == null ? "nothing yet" : pluginAdmin.pluginIdsOf(bundle.getBundleId())) + "); dropped "
                    + msg.printHeader());
            return;
        }
        try {
            io.cresco.agent.controller.communication.CallerStamps.stamp(msg, controllerEngine.pluginCaller(id), controllerEngine.regionAlias());
            controllerEngine.markForSigning(msg);
            controllerEngine.msgIn(msg);
        } catch (Exception ex) {
            logger.error("msgOut() from " + bundle.getSymbolicName() + ": " + ex.getMessage(), ex);
        }
    }

    /**
     * A send on behalf of an API user {@code bundle} authenticated: only from a bundle in {@code on_behalf_bundles}, for
     * one of its plugins, for an API caller of this node's tenant (any tenant when this node's tenant is a superuser
     * tenant -- the broker applies the same rule, so a tenant's node never mints another tenant's users).
     */
    void msgOutOnBehalfOf(org.osgi.framework.Bundle bundle, String id, MsgEvent msg, io.cresco.library.security.Caller caller) {
        String why = onBehalfRefusal(bundle, id, caller);
        if (why != null) {
            refused("send on behalf of a user", bundle.getSymbolicName() + ": " + why);
            throw new SecurityException(why);
        }
        io.cresco.agent.controller.communication.CallerStamps.stamp(msg,
                controllerEngine.securityOn() ? caller : io.cresco.library.security.Caller.anonymous());
        io.cresco.agent.controller.communication.CallerStamps.dropFiles(msg);   // a user names no file on this node's disk
        controllerEngine.markForSigning(msg);
        controllerEngine.msgIn(msg);
    }

    String onBehalfRefusal(org.osgi.framework.Bundle bundle, String id, io.cresco.library.security.Caller caller) {
        // review F5: the capability is the bundle's own -- installed by this controller, used by its own code -- not its name's
        if (!trustedNamedBundle(bundle)) {
            return "bundle " + bundle.getSymbolicName() + "#" + bundle.getBundleId() + " was not installed by this controller"
                    + " (from the agent jar or a verified signed jar): a symbolic name alone holds no capability";
        }
        String asker = CallerFrames.refusal(bundle);
        if (asker != null) return asker;
        return onBehalfRefusal(bundle.getSymbolicName(),
                io.cresco.agent.controller.communication.SecuritySettings.onBehalfBundles(plugin.getConfig()),
                pluginAdmin != null && pluginAdmin.bundleRunsPlugin(bundle.getBundleId(), id), id, caller, getTenant(),
                getSuperuserTenants());
    }

    /**
     * Does a capability bound to a bundle's symbolic name (on_behalf_bundles, the executor's stdin) hold for {@code bundle}:
     * did this controller install it itself? (review F5: a plugin can install a bundle of any name through its own context.)
     */
    boolean trustedNamedBundle(org.osgi.framework.Bundle bundle) {
        return bundle != null && pluginAdmin != null && pluginAdmin.installedByController(bundle.getBundleId());
    }

    /**
     * Why a bundle may not send on behalf of {@code caller}, or null: the bundle must be in {@code on_behalf_bundles}, run
     * the plugin it sends as, and send for an API caller of this node's tenant -- of any tenant only when this node's
     * tenant is a superuser tenant.
     */
    static String onBehalfRefusal(String bundleName, java.util.Set<String> allowed, boolean runsPlugin, String pluginId,
                                  io.cresco.library.security.Caller caller, String nodeTenant, java.util.Set<String> superuserTenants) {
        if (!allowed.contains(bundleName)) {
            return "bundle " + bundleName + " is not in on_behalf_bundles " + allowed;
        }
        if (caller == null || caller.getKind() != io.cresco.library.security.Caller.Kind.API) {
            return "on behalf of " + caller + ": only an API caller (api:<tenant>/<user>)";
        }
        if (!runsPlugin) {
            return "bundle " + bundleName + " does not run plugin " + pluginId;
        }
        if (!caller.getTenant().equals(nodeTenant) && !superuserTenants.contains(nodeTenant)) {
            return "a user of tenant " + caller.getTenant() + " on a node of tenant " + nodeTenant
                    + " (only a node of a superuser tenant sends for another tenant's users)";
        }
        return null;
    }

    /** A plugin sets the level of its own loggers ({@code <pluginid>:...}) only. */
    void setLogLevelFromBundle(org.osgi.framework.Bundle bundle, String logId, CLogger.Level level) {
        if (logId != null && pluginAdmin != null) {
            for (String own : pluginAdmin.pluginIdsOf(bundle.getBundleId())) {
                if (logId.toLowerCase(java.util.Locale.ROOT).startsWith(own.toLowerCase(java.util.Locale.ROOT) + ":")) {
                    setLogLevel(logId, level);
                    return;
                }
            }
        }
        refused("log level of another component", bundle.getSymbolicName() + " tried to set " + logId + " to " + level);
    }

    private final java.util.Map<org.osgi.framework.Bundle, io.cresco.library.data.DataPlaneService> bundleDataPlanes =
            java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    /** The dataplane {@code bundle} sees: every message it writes carries its plugin as the writer. */
    io.cresco.library.data.DataPlaneService dataPlaneFor(org.osgi.framework.Bundle bundle, io.cresco.library.data.DataPlaneService impl) {
        if (!(impl instanceof io.cresco.agent.data.DataPlaneServiceImpl)) return impl;
        final long bundleId = bundle.getBundleId();
        final String name = bundle.getSymbolicName();
        io.cresco.library.data.DataPlaneService d = new io.cresco.agent.data.BundleDataPlaneService(
                (io.cresco.agent.data.DataPlaneServiceImpl) impl, () -> {
                    java.util.SortedSet<String> ids = pluginAdmin == null ? new java.util.TreeSet<>() : pluginAdmin.pluginIdsOf(bundleId);
                    // a bundle running several plugin instances writes as the first of them (instances of one bundle
                    // are one code base); a bundle running none writes as nobody
                    return ids.isEmpty() ? null : controllerEngine.pluginCaller(ids.first());
                }, name, io.cresco.agent.controller.communication.SecuritySettings.onBehalfBundles(plugin.getConfig()).contains(name)
                        && trustedNamedBundle(bundle), trustedNamedBundle(bundle));
        bundleDataPlanes.put(bundle, d);
        return d;
    }

    boolean isCurrentDataPlane(io.cresco.library.data.DataPlaneService wrapper, io.cresco.library.data.DataPlaneService impl) {
        return wrapper instanceof io.cresco.agent.data.BundleDataPlaneService
                && ((io.cresco.agent.data.BundleDataPlaneService) wrapper).wraps(impl);
    }

    @Override
    public io.cresco.library.security.SecurityProfile getSecurityProfile() {
        return io.cresco.agent.controller.communication.SecuritySettings.profile(plugin == null ? null : plugin.getConfig());
    }

    @Override
    public boolean isSecurityEnabled() {
        return plugin == null || io.cresco.agent.controller.communication.SecuritySettings.brokerSecurity(plugin.getConfig());
    }

    /** This node's tenant ({@code tenant_id}). */
    @Override
    public String getTenant() {
        return plugin == null ? null : io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
    }

    @Override
    public java.util.Set<String> getSuperuserTenants() {
        return plugin == null ? java.util.Collections.emptySet()
                : java.util.Collections.unmodifiableSet(io.cresco.agent.controller.communication.SecuritySettings.superuserTenants(plugin.getConfig()));
    }

    private volatile java.security.KeyStore signingTrust;
    private volatile String signingTrustKey;

    /** {@code plugin_signing_truststore} (PKCS12 or JKS, trusted-certificate entries), re-read when the file changes. */
    @Override
    public java.security.KeyStore getPluginSigningTrust() {
        if (plugin == null) return null;
        String path = plugin.getConfig().getStringParam("plugin_signing_truststore", null);
        if (path == null || path.trim().isEmpty()) return null;
        try {
            Path p = Paths.get(path.trim()).toAbsolutePath();
            java.nio.file.attribute.BasicFileAttributes a = java.nio.file.Files.readAttributes(p, java.nio.file.attribute.BasicFileAttributes.class);
            String key = p + "|" + a.lastModifiedTime().toMillis() + "|" + a.size();
            if (key.equals(signingTrustKey) && signingTrust != null) return signingTrust;
            String pw = plugin.getConfig().getStringParam("plugin_signing_truststore_password", "");
            java.security.KeyStore ks = io.cresco.library.security.JarSignatures.loadTrustStore(p, pw == null ? new char[0] : pw.toCharArray());
            signingTrust = ks;
            signingTrustKey = key;
            return ks;
        } catch (Exception ex) {
            refused("plugin signing trust store", "plugin_signing_truststore " + path + " cannot be read (" + ex.getMessage()
                    + "): every jar arriving over the network is refused");
            return null;
        }
    }

    @Override
    public boolean isPluginSigningRequired() {
        return plugin == null || io.cresco.agent.controller.communication.SecuritySettings.pluginSigningRequired(plugin.getConfig());
    }

    /** Every boot says plainly what is not protected. */
    private void logSecurityModeWarnings() {
        io.cresco.library.plugin.Config c = plugin.getConfig();
        if (!io.cresco.agent.controller.communication.SecuritySettings.brokerSecurity(c)) {
            logger.warn("SECURITY IS DISABLED (broker_security_enabled=false): every caller is anonymous with every role; "
                    + "development only");
        }
        if (!isPluginSigningRequired()) {
            logger.warn("plugin_signing_required=false: jars arriving over the network are installed without a valid signature; "
                    + "development only");
        } else if (c.getStringParam("plugin_signing_truststore", null) == null) {
            logger.warn("plugin_signing_truststore is not set: every plugin or repository jar arriving over the network is "
                    + "refused (jars embedded in the agent are trusted)");
        }
        if (io.cresco.agent.controller.communication.SecuritySettings.fileAccessRoots(c).isEmpty()) {
            logger.info("file_access_roots is not set: getfiledata and getfileinfo are refused");
        }
        if (io.cresco.agent.controller.communication.SecuritySettings.brokerSecurity(c) && !c.getBooleanParam("msg_signing_required", true)) {
            logger.warn("msg_signing_required=false: messages are neither signed nor checked end to end (each hop's broker stamp "
                    + "only); development only");
        }
    }

    @Override
    public DataPlaneService getDataPlaneService() {
        return controllerEngine.getDataPlaneService();
    }



}