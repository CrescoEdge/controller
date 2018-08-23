package io.cresco.agent.controller.db.testhelpers;

import io.cresco.agent.controller.core.ControllerEngine;

public class ControllerEngine4Test extends ControllerEngine {
    private DBInterface4Test gdb;
    private PluginBuilder4Test plugin;

    protected ControllerEngine4Test(DBInterface4Test gdb){
        this.gdb = gdb;
        this.plugin = new PluginBuilder4Test();
    }
    public DBInterface4Test getGdb(){return gdb;}
    public void setGdb(DBInterface4Test gdb){this.gdb = gdb;}

    public PluginBuilder4Test getPluginBuilder(){return plugin;}
}
