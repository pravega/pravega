/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import org.junit.Test;

import static org.junit.Assert.assertSame;

public class ConfigurationOptionsExtractorTests {

    @Test
    public void testExtractInteger() {
        String propertyName = this.getClass().getSimpleName() + ".testExtractInteger." + ".test.property";
        try {
            System.setProperty(propertyName, "5");
            assertSame(5, ConfigurationOptionsExtractor.extractInt(propertyName,
                    "test_property_1", 1));
        } finally {
            System.clearProperty(propertyName);
        }
    }

    @Test
    public void testExtractString() {
        String propertyName = this.getClass().getSimpleName() + ".testExtractInteger." + ".test.property";
        try {
            System.setProperty(propertyName, "5");
            assertSame("5", ConfigurationOptionsExtractor.extractString(propertyName,
                    "test_property_1", "1"));
        } finally {
            System.clearProperty(propertyName);
        }
    }

    @Test
    public void testExtractIntegerDefaultValue() {
        String propertyName = this.getClass().getSimpleName() + ".testExtractIntegerDefaultValue." + ".test.property";
        assertSame(1, ConfigurationOptionsExtractor.extractInt(propertyName,
                "test_property_1", 1));
    }



}
