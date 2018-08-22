package io.cresco.agent.controller.db.testhelpers;

import java.util.HashMap;
import java.util.Map;

public class GDBConf {
    private String host;
    private String username;
    private String password;
    private String dbname;

    public GDBConf(String username,String password,String host,String dbname){
        this.host = host;
        this.username = username;
        this.password = password;
        this.dbname = dbname;
    }

    public GDBConf(Map<String,Object> conf) {
        this.host = (String)conf.get("gdb_host");
        this.username = (String)conf.get("gdb_username");
        this.password = (String)conf.get("gdb_password");
        this.dbname = (String)conf.get("gdb_dbname");
    }

    public Map<String,Object> getAsMap() {
        Map<String,Object> configMap = new HashMap<>();
        configMap.put("gdb_host",this.host);
        configMap.put("gdb_username",this.username);
        configMap.put("gdb_password",this.password);
        configMap.put("gdb_dbname",this.dbname);
        return configMap;
    }

    public String getRemoteURI(){
        return "remote:" + host + "/" + dbname;
    }

    public String getHost() {
        return host;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDbname() {
        return dbname;
    }
}
