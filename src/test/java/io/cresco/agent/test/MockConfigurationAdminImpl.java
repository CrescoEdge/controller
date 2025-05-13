package io.cresco.agent.test;

import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import java.io.IOException;
import java.util.*;


// --- MockConfigurationAdminImpl.java ---
class MockConfigurationAdminImpl implements ConfigurationAdmin {

    private Map<String, Configuration> configurations = new Hashtable<>();
    // No need for separate factoryConfigurations map if PIDs are unique and factory PID is stored in MockConfiguration
    // private Map<String, Configuration> factoryConfigurations = new Hashtable<>();


    @Override
    public Configuration createFactoryConfiguration(String factoryPid) throws IOException {
        String pid = factoryPid + "." + java.util.UUID.randomUUID().toString();
        MockConfiguration config = new MockConfiguration(pid);
        config.setFactoryPid(factoryPid);
        // factoryConfigurations.put(pid, config); // Not strictly needed if PID is unique
        configurations.put(pid, config); // Store all configs here
        return config;
    }

    @Override
    public Configuration createFactoryConfiguration(String factoryPid, String location) throws IOException {
        String pid = factoryPid + "." + (location != null ? location.hashCode() + "_" + java.util.UUID.randomUUID().toString() : java.util.UUID.randomUUID().toString());
        MockConfiguration config = new MockConfiguration(pid);
        config.setFactoryPid(factoryPid);
        config.setBundleLocation(location);
        // factoryConfigurations.put(pid, config); // Not strictly needed
        configurations.put(pid, config);
        return config;
    }


    @Override
    public Configuration getConfiguration(String pid, String location) throws IOException {
        // If location is null, it's a regular configuration.
        // If location is not null, it could be a factory instance tied to a bundle location.
        // This mock will primarily use pid.
        if (!configurations.containsKey(pid)) {
            MockConfiguration config = new MockConfiguration(pid);
            if (location != null) {
                config.setBundleLocation(location);
            }
            configurations.put(pid, config);
        }
        Configuration config = configurations.get(pid);
        // Ensure bundle location is set if provided, even for existing config
        if (location != null && config instanceof MockConfiguration) {
            ((MockConfiguration) config).setBundleLocation(location);
        }
        return config;
    }

    @Override
    public Configuration getConfiguration(String pid) throws IOException {
        return getConfiguration(pid, null);
    }

    @Override
    public Configuration[] listConfigurations(String filterString) throws IOException, InvalidSyntaxException {
        List<Configuration> matchingConfigs = new ArrayList<>();
        if (filterString == null) {
            return configurations.values().toArray(new Configuration[0]);
        }

        org.osgi.framework.Filter osgiFilter = FrameworkUtil.createFilter(filterString);

        for (Configuration config : configurations.values()) {
            Dictionary<String, Object> props = new Hashtable<>();
            props.put(org.osgi.framework.Constants.SERVICE_PID, config.getPid());
            if (config.getFactoryPid() != null) {
                props.put(ConfigurationAdmin.SERVICE_FACTORYPID, config.getFactoryPid());
            }
            if (config.getBundleLocation() != null) {
                props.put(ConfigurationAdmin.SERVICE_BUNDLELOCATION, config.getBundleLocation());
            }
            // Add all properties from the configuration itself for more comprehensive filtering
            if (config.getProperties() != null) {
                for (java.util.Enumeration<String> e = config.getProperties().keys(); e.hasMoreElements();) {
                    String key = e.nextElement();
                    props.put(key, config.getProperties().get(key));
                }
            }

            if (osgiFilter.match(props)) {
                matchingConfigs.add(config);
            }
        }
        return matchingConfigs.toArray(new Configuration[0]);
    }

    // Removed the local 'matches' method as its logic is now incorporated into listConfigurations

    @Override
    public Configuration getFactoryConfiguration(String factoryPid, String name) throws IOException {
        // A factory configuration instance is uniquely identified by its factory PID and its own PID.
        // The 'name' here is often used to derive or locate the instance's PID.
        // For this mock, we'll assume 'name' is the instance's specific PID part.
        // This is a simplification; real CM behavior can be more complex with how PIDs are managed.
        String instancePid = factoryPid + "." + name; // Common pattern, but not strictly defined by spec for 'name'

        for(Configuration conf : configurations.values()) {
            if(factoryPid.equals(conf.getFactoryPid()) && instancePid.equals(conf.getPid())) {
                return conf;
            }
        }
        // If not found, create it (OSGi spec says getConfiguration should create if not exists)
        return createFactoryConfiguration(factoryPid, name); // 'name' could act as location here
    }

    @Override
    public Configuration getFactoryConfiguration(String factoryPid, String name, String location) throws IOException {
        // Similar to above, but location provides more context.
        // We can construct a PID or search based on these.
        // For simplicity, let's assume a PID construction or search pattern.
        String instancePid = factoryPid + "." + name; // Or a more complex unique ID based on name and location

        for(Configuration conf : configurations.values()) {
            if(factoryPid.equals(conf.getFactoryPid()) &&
                    instancePid.equals(conf.getPid()) &&
                    (location == null || location.equals(conf.getBundleLocation()))) {
                return conf;
            }
        }
        return createFactoryConfiguration(factoryPid, location != null ? location : name);
    }
}

