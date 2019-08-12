package io.cresco.agent.test;

import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Set;

public class MockConfiguration implements Configuration
{

    private Dictionary<String, Object> dictionary;
    private String pid;

    public MockConfiguration() {
        dictionary = new Hashtable<>();
        pid = "test";
    }

    public MockConfiguration(String pid) {
        dictionary = new Hashtable<>();
        this.pid = pid;
    }

    public MockConfiguration(String pid, Dictionary<String, Object> dictionary) {
        this.dictionary = dictionary;
        this.pid = pid;
    }

    public String getPid()
    {
        return pid;
    }


    public String getFactoryPid()
    {
        return pid;
    }


    public String getBundleLocation()
    {
        return null;
    }


    public void setBundleLocation( String bundleLocation )
    {

    }


    public void update() throws IOException
    {

    }


    public void update( Dictionary<String, ?> properties ) throws IOException
    {
    }


    public Dictionary<String, Object> getProperties()
    {
        return dictionary;
    }


    public long getChangeCount()
    {
        return -1;
    }


    public void delete() throws IOException
    {
    }


    public boolean updateIfDifferent(final Dictionary<String, ?> properties) throws IOException
    {
        return false;
    }


    public void addAttributes(final ConfigurationAttribute... attrs) throws IOException
    {

    }


    public Set<ConfigurationAttribute> getAttributes()
    {
        return null;
    }


    public void removeAttributes(final ConfigurationAttribute... attrs) throws IOException
    {
    }

    public Dictionary<String, Object> getProcessedProperties(ServiceReference<?> sr)
    {
        return null;
    }

    public int hashCode()
    {
        return -1;
    }


    public boolean equals( Object obj )
    {
        return false;
    }

    public String toString()
    {
        return "unknown";
    }

}