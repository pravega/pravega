/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.server.store;

import com.emc.pravega.common.util.ConfigBuilder;
import java.util.Properties;
import java.util.function.Consumer;
import org.junit.Test;

/**
 * Unit tests for the ServiceBuilderConfig class.
 */
public class ServiceBuilderConfigTests {
    private static final String NAMESPACE = "ns";

    /**
     * Tests the Builder for ServiceBuilderConfig.
     */
    @Test
    public void testBuilder() {
        // Create three raw Properties.
        // Save #1 to a temp file.
        // Create a builder and load up #1
        // Include #2
        // Include a test config builder with #3.
        // Verify results at each stage.
    }

    private static class TestConfigBuilder<T> extends ConfigBuilder<T> {
        private final Consumer<Properties> rawPropertiesCallback;

        TestConfigBuilder(ConfigConstructor<T> constructor, Consumer<Properties> rawPropertiesCallback) {
            super(NAMESPACE, constructor);
            this.rawPropertiesCallback = rawPropertiesCallback;
        }

        @Override
        public ConfigBuilder<T> rebase(Properties properties) {
            this.rawPropertiesCallback.accept(properties);
            return this;
        }
    }
}
