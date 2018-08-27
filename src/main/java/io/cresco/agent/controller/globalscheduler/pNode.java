package io.cresco.agent.controller.globalscheduler;

import java.text.SimpleDateFormat;
import java.util.*;


public class pNode {

	  public String name;
	  public String jarfile;
	  public String md5;
	  public String version;


	  public List<Map<String, String>> repoServers = null;

	  public pNode(String name, String jarfile, String md5, String version)
	  {
	  	  repoServers = new ArrayList<>();
		  this.name = name;
		  this.jarfile = jarfile;
		  this.md5 = md5;
		  this.version = version;
	  }

	public pNode(String name, String jarfile, String md5, String version, List<Map<String, String>> repoServers)
	{
		this.repoServers = new ArrayList<>();
		this.repoServers.addAll(repoServers);
		this.name = name;
		this.jarfile = jarfile;
		this.md5 = md5;
		this.version = version;
	}

	public void addRepos(List<Map<String, String>> repoServers) {

	      for(Map<String,String> server : repoServers) {
	        if(!repoServers.contains(server)) {
	            repoServers.add(server);
            }
          }

	}

	public boolean isEqual (String name, String jarfile, String md5, String version) {

        return (this.name.equals(name)) && (this.jarfile.equals(jarfile)) && (this.md5.equals(md5)) && (this.version.equals(version));
    }

    public Date getBuildTime() {
        Date buildDate = null;
        try {
            String[] versionStr = version.split("\\.");
            String dataStr = versionStr[versionStr.length - 1];

            SimpleDateFormat format = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
            format.setTimeZone(TimeZone.getTimeZone("UTC"));

            buildDate = format.parse(dataStr);

        } catch (Exception ex) {
            buildDate = new Date(Long.MIN_VALUE);
        }
        return buildDate;
    }

	}