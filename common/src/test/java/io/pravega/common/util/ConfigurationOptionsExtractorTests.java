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
package io.pravega.common.util;

import org.junit.Test;

import static org.junit.Assert.assertSame;

public class ConfigurationOptionsExtractorTests {

    @Test
    public void testExtractIntWhenPropertyIsSetToAnInt() {
        String propertyName = this.getClass().getSimpleName() + ".testExtractIntWhenPropertyIsSetToAnInt."
                + ".test.property";
        try {
            System.setProperty(propertyName, "5");
            assertSame(5, ConfigurationOptionsExtractor.extractInt(propertyName,
                    "test_property_1", 1));
        } finally {
            System.clearProperty(propertyName);
        }
    }

    @Test
    public void testExtractIntReturnsDefaultValueWhenPropertyIsNotAString() {
        String propertyName = this.getClass().getSimpleName()
                + ".testExctactIntReturnsDefaultValueWhenPropertyIsNotAString." + ".test.property";
        try {
            System.setProperty(propertyName, "ABC");
            assertSame(1, ConfigurationOptionsExtractor.extractInt(propertyName,
                    "", 1));
        } finally {
            System.clearProperty(propertyName);
        }
    }

    @Test
    public void testExtractStringFromSystemPropertyWhenItIsSet() {
        String propertyName = this.getClass().getSimpleName() + ".testExtractString." + ".test.property";
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
