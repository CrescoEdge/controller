package io.cresco.agent.core;

import io.cresco.agent.test.MockBundleContext;

class ActivatorTest {

    @org.junit.jupiter.api.Test
    void start() {

        try {

            MockBundleContext bundleContext = new MockBundleContext();
            Activator activator = new Activator();
            activator.start(bundleContext);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @org.junit.jupiter.api.Test
    void stop() {

        MockBundleContext bundleContext = new MockBundleContext();
        Activator activator = new Activator();
        activator.start(bundleContext);
        activator.stop(bundleContext);

    }
}