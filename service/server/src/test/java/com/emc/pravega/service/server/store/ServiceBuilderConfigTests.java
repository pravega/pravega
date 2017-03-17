/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.server.store;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.testcommon.AssertExtensions;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.val;
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
    public void testBuilder() throws Exception {
        final int propertyCount = 20;
        // Create three raw Properties.
        val p1 = buildProperties(0, propertyCount / 2, 0);
        val p2 = buildProperties(propertyCount / 4, propertyCount / 2, 100);
        val p3 = buildProperties(propertyCount / 2, propertyCount / 2, 1000);

        // Save #1 to a temp file.
        @Cleanup("delete")
        File tmpFile = File.createTempFile("pravega-", "-servicebuildertest");
        try (val writer = new FileWriter(tmpFile)) {
            p1.store(writer, "");
        }

        // Create a builder and load up #1 from file
        val b = ServiceBuilderConfig.builder().include(tmpFile.getPath());

        val expected = new Properties();
        expected.putAll(p1);
        verifyContents("Unexpected contents after: file. ", expected, b);

        // Include #2.
        b.include(p2);
        expected.putAll(p2);
        verifyContents("Unexpected contents after: file + Properties.", expected, b);

        // Include a test config builder with #3.
        b.include(new ConfigBuilderSupplier<>(p3, s -> s));
        expected.putAll(p3);
        verifyContents("Unexpected contents after: file + Properties + ConfigBuilder.", expected, b);
    }

    private void verifyContents(String message, Properties expected, ServiceBuilderConfig.Builder builder) {
        AtomicReference<Properties> rawProperties = new AtomicReference<>();
        builder.build().getConfig(() -> new TestConfigBuilder<>(s -> s, rawProperties::set));
        AssertExtensions.assertContainsSameElements(
                message,
                expected.entrySet(),
                rawProperties.get().entrySet(),
                (e1, e2) -> {
                    int keyCompare = ((String) e1.getKey()).compareTo((String) e2.getKey());
                    int valueCompare = ((String) e1.getValue()).compareTo((String) e2.getValue());
                    return keyCompare * valueCompare + keyCompare + valueCompare;
                });
        System.out.println();
    }

    private Properties buildProperties(int startId, int count, int base) {
        Properties p = new Properties();
        for (int i = startId; i < startId + count; i++) {
            p.setProperty(Integer.toString(i), Integer.toString(i + base));
        }

        return p;
    }

    /**
     * A config builder with a predefined set of Properties.
     */
    private static class ConfigBuilderSupplier<T> extends ConfigBuilder<T> {
        private final Properties properties;

        ConfigBuilderSupplier(Properties properties, ConfigConstructor<T> constructor) {
            super(NAMESPACE, constructor);
            this.properties = properties;
        }

        @Override
        public void copyTo(Map<Object, Object> target) {
            target.putAll(this.properties);
        }
    }

    /**
     * A config builder which extracts properties when rebased.
     */
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
