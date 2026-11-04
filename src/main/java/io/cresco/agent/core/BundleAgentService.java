package io.cresco.agent.core;

import io.cresco.library.agent.AgentService;
import io.cresco.library.agent.AgentState;
import io.cresco.library.data.DataPlaneService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.security.Caller;
import io.cresco.library.utilities.CLogger;
import org.osgi.framework.Bundle;

import java.security.KeyStore;
import java.util.Set;

/**
 * The AgentService one bundle sees (the controller registers a service factory, so every bundle gets its own). It knows
 * the calling bundle, so the controller knows who is sending: a message goes out as one of that bundle's plugins only
 * ({@code plugin:<tenant>/<region>/<agent>/<pluginid>}), never as another bundle's plugin or as the controller, and only a
 * bundle listed in {@code on_behalf_bundles} sends on behalf of an API user. See {@code docs/caller-authorization.md}.
 */
final class BundleAgentService implements AgentService {

    private final AgentServiceImpl core;
    private final Bundle bundle;
    private volatile DataPlaneService dataPlane;

    BundleAgentService(AgentServiceImpl core, Bundle bundle) {
        this.core = core;
        this.bundle = bundle;
    }

    Bundle bundle() { return bundle; }

    @Override public AgentState getAgentState() { return core.getAgentState(); }

    /** The dataplane, writing as this bundle's plugin (its messages carry that writer, never the controller's). */
    @Override
    public DataPlaneService getDataPlaneService() {
        DataPlaneService d = dataPlane;
        DataPlaneService impl = core.getDataPlaneService();
        if (impl == null) return null;
        if (d == null || !core.isCurrentDataPlane(d, impl)) {
            d = core.dataPlaneFor(bundle, impl);
            dataPlane = d;
        }
        return d;
    }

    @Override
    public CLogger getCLogger(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName, CLogger.Level level) {
        return core.getCLogger(pluginBuilder, baseClassName, issuingClassName, level);
    }

    @Override
    public CLogger getCLogger(PluginBuilder pluginBuilder, String baseClassName, String issuingClassName) {
        return core.getCLogger(pluginBuilder, baseClassName, issuingClassName);
    }

    @Override
    public void msgOut(String id, MsgEvent msg) {
        core.msgOutFromBundle(bundle, id, msg);
    }

    @Override
    public void msgOutOnBehalfOf(String id, MsgEvent msg, Caller caller) {
        core.msgOutOnBehalfOf(bundle, id, msg, caller);
    }

    @Override public void setLogLevel(String logId, CLogger.Level level) { core.setLogLevelFromBundle(bundle, logId, level); }
    @Override public String getAgentDataDirectory() { return core.getAgentDataDirectory(); }
    @Override public boolean isSecurityEnabled() { return core.isSecurityEnabled(); }
    @Override public String getTenant() { return core.getTenant(); }
    @Override public Set<String> getSuperuserTenants() { return core.getSuperuserTenants(); }
    @Override public KeyStore getPluginSigningTrust() { return core.getPluginSigningTrust(); }
    @Override public boolean isPluginSigningRequired() { return core.isPluginSigningRequired(); }
    @Override public io.cresco.library.security.SecurityProfile getSecurityProfile() { return core.getSecurityProfile(); }

    @Override
    public String toString() {
        return "AgentService[" + bundle.getSymbolicName() + "#" + bundle.getBundleId() + "]";
    }
}
