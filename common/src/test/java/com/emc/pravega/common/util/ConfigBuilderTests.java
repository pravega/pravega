/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import java.util.Properties;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for ConfigBuilder.
 */
public class ConfigBuilderTests {
    /**
     * Tests the with() method.
     */
    @Test
    public void testWith() {
        final String namespace = "ns";
        final int propertyCount = 10;
        val builder = new ConfigBuilder<TestConfig>(namespace, TestConfig::new);
        for (int i = 0; i < propertyCount; i++) {
            val result = builder.with(new Property<>(Integer.toString(i)), i);
            Assert.assertEquals("with() did not return this instance.", builder, result);
        }

        TestConfig c = builder.build();
        for (int i = 0; i < propertyCount; i++) {
            val p = new Property<Integer>(Integer.toString(i));
            val actual = c.getProperties().getInt(p);
            Assert.assertEquals("Unexpected value in result.", i, actual);
        }
    }

    /**
     * Tests the rebase() method.
     */
    @Test
    public void testRebase() {
        final String namespace = "ns";
        final int propertyCount = 10;
        val builder1 = new ConfigBuilder<TestConfig>(namespace, TestConfig::new);
        for (int i = 0; i < propertyCount; i++) {
            builder1.with(new Property<>(Integer.toString(i)), i);
        }

        // Create a second builder and update that one too, but with different values.
        Properties p2 = new Properties();
        p2.setProperty(namespace + ".1", "a"); // Decoy - this should be overridden.
        val builder2 = builder1.rebase(p2);
        Assert.assertNotEquals("rebase() returned the same instance.", builder1, builder2);
        Assert.assertEquals("rebase() touched the target Properties object.", 1, p2.size());

        for (int i = 0; i < propertyCount; i++) {
            builder2.with(new Property<>(Integer.toString(i)), i * 10);
        }

        Assert.assertEquals("Unexpected number of properties copied.", propertyCount, p2.size());

        //Verify the original builder did not get modified, but that the second one, as well as the given props, were.
        TestConfig c1 = builder1.build();
        TestConfig c2 = builder2.build();
        for (int i = 0; i < propertyCount; i++) {
            val p = new Property<Integer>(Integer.toString(i));
            val actual1 = c1.getProperties().getInt(p);
            val actual2 = c2.getProperties().getInt(p);
            val actualProp2 = p2.getProperty(namespace + "." + Integer.toString(i));
            Assert.assertEquals("Rebased instance modified the original builder.", i, actual1);
            Assert.assertEquals("Rebased instance did not produce a correct result.", i * 10, actual2);
            Assert.assertEquals("Rebased instance did not produce a correct result.", Integer.toString(i * 10), actualProp2);
        }
    }

    /**
     * Tests the copyTo() method.
     */
    @Test
    public void testCopyTo() {
        final String namespace = "ns";
        final int propertyCount = 10;
        val builder = new ConfigBuilder<TestConfig>(namespace, TestConfig::new);
        for (int i = 0; i < propertyCount; i++) {
            builder.with(new Property<>(Integer.toString(i)), i);
        }

        Properties p2 = new Properties();
        p2.setProperty(namespace + ".1", "a"); // Decoy - this should be overridden.
        builder.copyTo(p2);
        Assert.assertEquals("Unexpected number of properties copied.", propertyCount, p2.size());
        for (int i = 0; i < propertyCount; i++) {
            val actualProp2 = p2.getProperty(namespace + "." + Integer.toString(i));
            Assert.assertEquals("CopyTo did not set the correct values.", Integer.toString(i), actualProp2);
        }
    }

    @RequiredArgsConstructor
    private static class TestConfig {
        @Getter
        private final TypedProperties properties;
    }
}
