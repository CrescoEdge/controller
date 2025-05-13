package io.cresco.agent.test;

import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.FrameworkUtil; // Added for filter creation

import java.io.IOException;
import java.util.ArrayList; // Added import
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List; // Added import
import java.util.Map;
import java.util.Set;

// --- MockConfiguration.java ---
public class MockConfiguration implements Configuration {

    private Dictionary<String, Object> properties;
    private String pid;
    private String factoryPid;
    private String bundleLocation; // Added to store bundle location

    public MockConfiguration(String pid) {
        this.pid = pid;
        this.properties = new Hashtable<>();
    }

    public MockConfiguration(String pid, Dictionary<String, Object> initialProperties) {
        this.pid = pid;
        this.properties = initialProperties != null ? initialProperties : new Hashtable<>();
    }

    @Override
    public String getPid() {
        return pid;
    }

    @Override
    public String getFactoryPid() {
        // In OSGi, a factory configuration has a factory PID.
        // For simplicity, we can return the PID or a dedicated factory PID if set.
        return factoryPid != null ? factoryPid : pid;
    }

    public void setFactoryPid(String factoryPid) {
        this.factoryPid = factoryPid;
    }

    @Override
    public Dictionary<String, Object> getProperties() {
        // Return a copy to prevent external modification if desired,
        // but for tests, returning direct reference might be fine.
        return properties;
    }

    @Override
    public void update(Dictionary<String, ?> newProperties) throws IOException {
        // Overwrite existing properties or add new ones.
        // OSGi spec says this should merge, but for tests, simple replace might be enough.
        // If newProperties is null, it should remove all properties.
        if (newProperties == null) {
            this.properties = new Hashtable<>();
        } else {
            // Create a new Hashtable to avoid issues with unmodifiable dictionaries
            Dictionary<String, Object> modifiableProps = new Hashtable<>();
            for (java.util.Enumeration<String> e = newProperties.keys(); e.hasMoreElements();) {
                String key = e.nextElement();
                modifiableProps.put(key, newProperties.get(key));
            }
            this.properties = modifiableProps;
        }
    }


    @Override
    public void delete() throws IOException {
        // Simulate deletion, perhaps by clearing properties or marking as deleted.
        this.properties = new Hashtable<>();
    }

    @Override
    public void setBundleLocation(String bundleLocation) {
        this.bundleLocation = bundleLocation;
    }

    @Override
    public String getBundleLocation() {
        return bundleLocation;
    }

    @Override
    public long getChangeCount() {
        return 1; // Or a mock value
    }

    @Override
    public boolean updateIfDifferent(Dictionary<String, ?> newProperties) throws IOException {
        if (this.properties == null && newProperties == null) {
            return false;
        }
        // A more robust equals check for Dictionaries might be needed if order matters
        // or if they contain complex objects. For simple key/value strings, this is often sufficient.
        boolean different = true;
        if (this.properties != null && newProperties != null && this.properties.size() == newProperties.size()) {
            different = false; // Assume same until proven different
            for (java.util.Enumeration<String> e = this.properties.keys(); e.hasMoreElements();) {
                String key = e.nextElement();
                if (!newProperties.get(key).equals(this.properties.get(key))) {
                    different = true;
                    break;
                }
            }
        }


        if (different) {
            update(newProperties);
            return true;
        }
        return false;
    }

    @Override
    public void addAttributes(ConfigurationAttribute... attrs) throws IOException {
        // Stub: Implement if attribute testing is needed
    }

    @Override
    public Set<ConfigurationAttribute> getAttributes() {
        // Stub: Implement if attribute testing is needed
        return null;
    }

    @Override
    public void removeAttributes(ConfigurationAttribute... attrs) throws IOException {
        // Stub: Implement if attribute testing is needed
    }

    @Override
    public Dictionary<String, Object> getProcessedProperties(ServiceReference<?> sr) {
        // Stub: Implement if processed properties testing is needed
        return getProperties(); // Simple mock behavior
    }

    @Override
    public void update() throws IOException {
        // In a real scenario, this might persist changes.
        // For a mock, this might not need to do anything unless you're testing this specific call.
    }
}
