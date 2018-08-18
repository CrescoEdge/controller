package io.cresco.agent.core;


import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

//import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * This class implements a simple bundle that utilizes the OSGi
 * framework's event mechanism to listen for service events. Upon
 * receiving a service event, it prints out the event's details.
 **/
public class Activator implements BundleActivator
{



    /**
     * Implements BundleActivator.start(). Prints
     * a message and adds itself to the bundle context as a service
     * listener.
     * @param context the framework context for the bundle.
     **/

    public void start(BundleContext context)
    {

        try {
            
            /*

            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
            java.util.logging.Logger ODBLogger = java.util.logging.Logger.getLogger("com.orientechnologies");
            ODBLogger.setLevel(Level.SEVERE);

            java.util.logging.Logger AMQLogger = java.util.logging.Logger.getLogger("org.apache.activemq");
            AMQLogger.setLevel(Level.SEVERE);

            java.util.logging.Logger apacheCommonsLogger = java.util.logging.Logger.getLogger("org.apache.commons.configuration");
            apacheCommonsLogger.setLevel(Level.SEVERE);

            */
            //org.apache.commons.configuration.DefaultFileSystem


        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Implements BundleActivator.stop(). Prints
     * a message and removes itself from the bundle context as a
     * service listener.
     * @param context the framework context for the bundle.
     **/
    public void stop(BundleContext context)
    {
        System.out.println("Stopped listening for service events.");

        // Note: It is not required that we remove the listener here,
        // since the framework will do it automatically anyway.

        /*
        ServiceReference configurationAdminReference =
                context.getServiceReference(ConfigurationAdmin.class.getName());
        */
    }


}
