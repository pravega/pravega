/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.store;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.test.common.AssertExtensions;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the ServiceBuilderConfig class.
 */
public class ServiceBuilderConfigTests {
    private static final String NAMESPACE = "ns";
    private static final List<String> SUPPORTED_TYPE_NAMES = Stream
            .of(Integer.class.getName(), Long.class.getName(), String.class.getName(), Boolean.class.getName())
            .map(s -> Property.class.getSimpleName() + "<" + s + ">")
            .collect(Collectors.toList());
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

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

    /**
     * Verifies the include(ConfigBuilder) and getConfig() work properly. This test follows a bit of an unusual approach
     * in that it picks a few known config classes, populates them using their builders and reflection, then uses reflection
     * once again to compare expected output (generated using their builders) with output from the ServiceBuilderConfig.getConfig().
     * <p>
     * This verifies that the namespacing inside ServiceBuilderConfig.builder() works correctly, as well as the constructors
     * for various configs.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGetConfig() throws Exception {
        // Select a few classes to test dynamically.
        val testClasses = new HashMap<Class<?>, Supplier<ConfigBuilder<?>>>();
        testClasses.put(ReadIndexConfig.class, ReadIndexConfig::builder);
        testClasses.put(WriterConfig.class, WriterConfig::builder);
        testClasses.put(MetricsConfig.class, MetricsConfig::builder);
        testClasses.put(DurableLogConfig.class, DurableLogConfig::builder);
        testClasses.put(ServiceConfig.class, ServiceConfig::builder);

        // Value generator.
        val nextValue = new AtomicInteger(10);

        // Create instances of each test class and dynamically assign their properties some arbitrary values
        val expectedValues = new HashMap<Class<?>, Object>();
        val b = ServiceBuilderConfig.builder();
        for (Map.Entry<Class<?>, Supplier<ConfigBuilder<?>>> e : testClasses.entrySet()) {
            Class<?> c = e.getKey();
            ConfigBuilder<?> configBuilder = e.getValue().get();
            for (Field f : c.getDeclaredFields()) {
                // Property names are defined as static fields; find those that are of type Property and their generic
                // type contains one of the supported types.
                if (Modifier.isStatic(f.getModifiers())
                        && f.getType().isAssignableFrom(Property.class)
                        && isSupportedType(f.getGenericType().getTypeName())) {
                    @SuppressWarnings("rawtypes")
                    Property p = (Property) f.get(null);
                    if (p.getDefaultValue() != null && p.getDefaultValue() instanceof Boolean) {
                        configBuilder.with(p, nextValue.incrementAndGet() % 2 == 0);
                    } else {
                        //Property security.tls.protocolVersion cannot be an Integer.
                        if (p.equals(ServiceConfig.TLS_PROTOCOL_VERSION)) {
                            configBuilder.with(p, p.getDefaultValue());
                        } else {
                            // Any number can be interpreted as a string or number.
                            configBuilder.with(p, Integer.toString(nextValue.incrementAndGet()));
                        }
                    }
                }
            }

            // Collect the built config object for later use.
            expectedValues.put(c, configBuilder.build());

            // Include the builder in the main builder.
            b.include(configBuilder);
        }

        // Create the ServiceBuilderConfig, and verify that the created Config classes (using getConfig()) match the
        // expected ones.
        val builderConfig = b.build();
        for (Map.Entry<Class<?>, Supplier<ConfigBuilder<?>>> e : testClasses.entrySet()) {
            Class<?> c = e.getKey();
            Object expectedConfig = expectedValues.get(c);
            Object actualConfig = builderConfig.getConfig(e.getValue());

            // All the properties we care about are public getters with no arguments - only check those.
            for (Method m : c.getDeclaredMethods()) {
                if (m.getName().startsWith("get")
                        && m.getParameterCount() == 0
                        && !Modifier.isStatic(m.getModifiers())
                        && Modifier.isPublic(m.getModifiers())) {
                    Object expectedValue = m.invoke(expectedConfig);
                    Object actualValue = m.invoke(actualConfig);
                    if (expectedValue == null) {
                        Assert.assertNull("Expected a null value for " + getPropName(c, m), actualValue);
                    } else {
                        Assert.assertNotNull("Not expected a null value for " + getPropName(c, m), actualValue);
                    }

                    if (isSupportedType(expectedValue)) {
                        Assert.assertEquals("Unexpected value for " + getPropName(c, m), expectedValue, actualValue);
                    }
                }
            }
        }
    }

    private String getPropName(Class<?> c, Method m) {
        return c.getSimpleName() + "." + m.getName();
    }

    private boolean isSupportedType(String genericTypeName) {
        for (String s : SUPPORTED_TYPE_NAMES) {
            if (genericTypeName.contains(s)) {
                return true;
            }
        }

        return false;
    }

    private boolean isSupportedType(Object target) {
        return target instanceof Number || target instanceof String || target instanceof Boolean;
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
