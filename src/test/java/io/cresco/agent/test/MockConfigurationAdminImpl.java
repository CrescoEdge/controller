package io.cresco.agent.test;

import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import java.io.IOException;

public class MockConfigurationAdminImpl implements ConfigurationAdmin
{


    // Configuration logConfig = confAdmin.getConfiguration("org.ops4j.pax.logging", null);

    public Configuration createFactoryConfiguration( String factoryPid ) throws IOException
    {
        return null;
    }


    public Configuration createFactoryConfiguration( String factoryPid, String location ) throws IOException
    {
        return null;
    }


    public Configuration getConfiguration( String pid ) throws IOException
    {
        return null;
    }


    public Configuration getConfiguration( String pid, String location ) throws IOException
    {
        System.out.println("MockConfigurationAdminImpl : getConfiguration : pid=" + pid + " location=" + location);
        //MockConfigurationAdminImpl : getConfiguration : pid=org.ops4j.pax.logging location=null
        return new MockConfiguration(pid);

    }


    public Configuration[] listConfigurations( String filter ) throws IOException, InvalidSyntaxException
    {
        return null;
    }


    public Configuration getFactoryConfiguration(String factoryPid, String name, String location) throws IOException
    {
       return null;
    }

    public Configuration getFactoryConfiguration(String factoryPid, String name) throws IOException {
        return null;
    }




}