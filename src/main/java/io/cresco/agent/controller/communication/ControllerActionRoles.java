package io.cresco.agent.controller.communication;

import io.cresco.library.security.ActionAuthorizer;
import io.cresco.library.security.ActionAuthorizer.Direction;

/**
 * Who may call every controller action, and from where, per tier (docs/caller-authorization.md sections 3 and 3a). One
 * table per executor:
 * the agent tier (AgentExecutor, reached through the controller's PluginBuilder), the regional tier (RegionalExecutor,
 * which hands actions it does not serve to the agent executor) and the global tier (GlobalExecutor).
 *
 * <p>Every table fails closed: an action not listed here needs {@code infra} or {@code admin}. The privileged floor
 * ({@link ActionAuthorizer#PRIVILEGED}) applies on every tier. "any-tenant infra" marks the fabric mechanics a controller
 * serves to the nodes of every tenant (liveness, registration, keep-alive, KPI): nothing that reads or changes a tenant's
 * data. A drift test checks that every action an executor declares is listed here.
 *
 * <p>Every action also has a direction, declared here on purpose ({@link ActionAuthorizer.Direction}; authority follows the
 * tree): {@code DOWN} for management -- honoured only from this node itself or a controller above it; {@code UP} for what
 * a child reports to its parent -- only from this node itself or a node below it; {@code ANY} for what is open or only
 * read, and what controllers ask each other. An action nobody listed is {@code DOWN} when it is of the privileged floor
 * and {@code ANY} otherwise. {@code EveryActionHasADirectionTest} holds every action to a direction and the whole table
 * to a checked-in copy, so a change is deliberate.
 */
public final class ControllerActionRoles {

    private ControllerActionRoles() { }

    static final String C = "CONFIG", E = "EXEC";
    static final String INFRA = "infra", ADMIN = "admin", PLUGIN = "plugin", USER = "user";

    private static void down(ActionAuthorizer a, String type, String... actions) {
        for (String f : actions) a.direct(type, f, Direction.DOWN);
    }

    private static void up(ActionAuthorizer a, String type, String... actions) {
        for (String f : actions) a.direct(type, f, Direction.UP);
    }

    private static void any(ActionAuthorizer a, String type, String... actions) {
        for (String f : actions) a.direct(type, f, Direction.ANY);
    }

    /** The agent tier: the controller's own PluginBuilder table (the AgentExecutor's annotations are registered too). */
    public static ActionAuthorizer agentTier(ActionAuthorizer a) {
        a.setBundle(ActionAuthorizer.CONTROLLER_BUNDLE);
        a.setDefaultRoles(INFRA, ADMIN);
        // plugin lifecycle, controller/JVM, logging that exposes other callers' traffic: the privileged floor
        for (String f : new String[]{"pluginadd", "pluginremove", "pluginupload", "pluginrepopull", "setloglevel", "setlogdp",
                "controllerupdate", "stopcontroller", "restartcontroller", "restartframework", "killjvm"}) {
            a.require(C, f, INFRA, ADMIN);
        }
        a.require(C, "getislogdp", INFRA, ADMIN);
        // CEP queries run inside this controller over the tenant's dataplane
        a.require(C, "cepadd", INFRA, ADMIN, PLUGIN);
        a.require(C, "cepremove", INFRA, ADMIN, PLUGIN);
        a.require(C, "cepinput", INFRA, ADMIN, PLUGIN);
        a.require(C, "getagentinfo", INFRA, ADMIN, PLUGIN);       // the agent's data directory path
        a.open(C, "pluginlist");
        a.open(C, "pluginstatus");
        // logs, files, the network: never to a plain user or plugin
        a.require(E, "getlog", INFRA, ADMIN);
        a.require(E, "getfileinfo", INFRA, ADMIN);
        a.require(E, "getfiledata", INFRA, ADMIN);
        a.require(E, "getbroadcastdiscovery", INFRA, ADMIN);
        a.require(E, "listagents", INFRA, ADMIN);                 // the region's agents, of every tenant
        a.require(E, "getnetworkstate", INFRA, ADMIN);
        a.require(E, "getcoordinators", INFRA, ADMIN);
        a.require(E, "indirectprobe", INFRA);                     // makes this node probe another
        a.open(E, "getcontrollerstatus");
        a.open(E, "iscontrolleractive");
        a.open(E, "getcryptopolicy");
        a.open(E, "ping");
        a.allowAnyTenantInfra(E, "ping");
        a.open(E, "getmetricinventory");
        a.open(E, "gethealthinventory");
        a.open(E, "getcapabilities");
        a.open(E, "getcapabilityinventory");
        // no-ops at the agent tier, and plugins' prints
        for (String t : new String[]{"INFO", "WATCHDOG", "KPI", "DISCOVER", "ERROR"}) a.open(t, "*");

        // ---- from where (authority follows the tree)
        // management of this node: its plugins and the code they run, the controller and its JVM, its logging, its files
        // and logs, its data directory, a network scan from it
        down(a, C, "pluginadd", "pluginremove", "pluginupload", "pluginrepopull", "controllerupdate", "stopcontroller",
                "restartcontroller", "restartframework", "killjvm", "setloglevel", "setlogdp", "getislogdp", "getagentinfo");
        down(a, E, "getlog", "getfileinfo", "getfiledata", "getbroadcastdiscovery");
        // this node's view of the whole mesh (every tenant's links and coordinators), for infra and admin only: narrow
        down(a, E, "getnetworkstate", "getcoordinators");
        // open and read-only: status, lists, inventories; a local plugin's CEP queries (their role confines them); what
        // controllers ask each other
        any(a, C, "pluginlist", "pluginstatus", "cepadd", "cepremove", "cepinput");
        any(a, E, "listagents", "getcontrollerstatus", "iscontrolleractive", "getcryptopolicy", "ping", "indirectprobe",
                "getmetricinventory", "gethealthinventory", "getcapabilities", "getcapabilityinventory");
        for (String t : new String[]{"INFO", "DISCOVER", "ERROR"}) any(a, t, "*");
        // what a child reports (no-ops on this tier; the regional and global tiers serve them)
        up(a, "WATCHDOG", "*");
        up(a, "KPI", "*");
        return a;
    }

    /** The regional tier: its own functions, and the agent tier for the actions it hands to the agent executor. */
    public static ActionAuthorizer regionalTier() {
        ActionAuthorizer a = agentTier(new ActionAuthorizer(ActionAuthorizer.CONTROLLER_BUNDLE));
        // region membership: every tenant's agents register here; RegistryBinding confines each to its own records
        a.require(C, "agent_enable", INFRA, ADMIN);
        a.allowAnyTenantInfra(C, "agent_enable");
        a.require(C, "agent_disable", INFRA, ADMIN);
        a.allowAnyTenantInfra(C, "agent_disable");
        a.require(C, "rpipelinesubmit", INFRA, ADMIN);
        a.require("WATCHDOG", "*", INFRA);
        a.allowAnyTenantInfra("WATCHDOG", "*");
        a.require("KPI", "*", INFRA, PLUGIN);
        a.allowAnyTenantInfra("KPI", "*");
        a.allowAnyTenantInfra(E, "ping");

        // ---- from where: an agent registers, unregisters and reports to its own region controller; a pipeline is
        // submitted to a region at the region or from its global (WATCHDOG, KPI, ping and the rest: as the agent tier)
        up(a, C, "agent_enable", "agent_disable");
        down(a, C, "rpipelinesubmit");
        return a;
    }

    /** The global tier (GlobalExecutor). */
    public static ActionAuthorizer globalTier() {
        ActionAuthorizer a = new ActionAuthorizer(ActionAuthorizer.CONTROLLER_BUNDLE);
        a.setDefaultRoles(INFRA, ADMIN);
        // region federation: every region registers here; RegistryBinding confines each to its own records
        for (String f : new String[]{"region_enable", "region_disable", "regionalimport"}) {
            a.require(C, f, INFRA);
            a.allowAnyTenantInfra(C, f);
        }
        // scheduling and the repository: deploys code. setinodestatus rewrites an iNode's scheduling state: no fabric
        // mechanic (no controller sends it), so infra or admin of the tenant that schedules here only (review F4)
        for (String f : new String[]{"addplugin", "removeplugin", "gpipelinesubmit", "gpipelineremove", "plugindownload", "savetorepo",
                "setinodestatus"}) {
            a.require(C, f, INFRA, ADMIN);
        }
        // the fabric registry, read: open to the global's tenant (and a superuser tenant's infra/admin)
        for (String f : new String[]{"listregions", "listagents", "listplugins", "listpluginsbytype", "listpluginsrepo",
                "listrepoinstances", "plugininfo", "pluginkpi", "resourceinfo", "netresourceinfo", "getenvstatus",
                "getinodestatus", "resourceinventory", "plugininventory", "getgpipeline", "getgpipelineexport",
                "getgpipelinestatus", "getisassignmentinfo", "getmetricinventory", "gethealthinventory", "getcapabilities",
                "getcapabilityinventory"}) {
            a.open(E, f);
        }
        a.require(E, "getnetworkstate", INFRA, ADMIN);
        a.require(E, "getcoordinators", INFRA, ADMIN);
        a.open(E, "ping");
        a.allowAnyTenantInfra(E, "ping");
        a.open(E, "getcryptopolicy");
        a.allowAnyTenantInfra(E, "getcryptopolicy");
        a.require("WATCHDOG", "*", INFRA);
        a.allowAnyTenantInfra("WATCHDOG", "*");
        a.require("KPI", "*", INFRA, PLUGIN);
        a.allowAnyTenantInfra("KPI", "*");
        for (String t : new String[]{"INFO", "DISCOVER", "ERROR"}) a.open(t, "*");

        // ---- from where: a region reports to the global it joined; scheduling, the repository and an iNode's state are
        // managed at the global itself (nothing is above it); the fabric registry is read from anywhere
        up(a, C, "region_enable", "region_disable", "regionalimport");
        down(a, C, "addplugin", "removeplugin", "gpipelinesubmit", "gpipelineremove", "plugindownload", "savetorepo", "setinodestatus");
        down(a, E, "getnetworkstate", "getcoordinators");
        any(a, E, "listregions", "listagents", "listplugins", "listpluginsbytype", "listpluginsrepo",
                "listrepoinstances", "plugininfo", "pluginkpi", "resourceinfo", "netresourceinfo", "getenvstatus",
                "getinodestatus", "resourceinventory", "plugininventory", "getgpipeline", "getgpipelineexport",
                "getgpipelinestatus", "getisassignmentinfo", "getmetricinventory", "gethealthinventory", "getcapabilities",
                "getcapabilityinventory", "ping", "getcryptopolicy");
        up(a, "WATCHDOG", "*");
        up(a, "KPI", "*");
        for (String t : new String[]{"INFO", "DISCOVER", "ERROR"}) any(a, t, "*");
        return a;
    }
}
