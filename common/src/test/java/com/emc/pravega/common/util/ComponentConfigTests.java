/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.common.util;

import java.util.ArrayList;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;

import org.junit.Assert;
import org.junit.Test;

import com.emc.pravega.testcommon.AssertExtensions;

/**
 * Unit tests for the ComponentConfig class.
 */
public class ComponentConfigTests {
    private static final int COMPONENT_COUNT = 5;
    private static final int PROPERTY_OF_TYPE_PER_COMPONENT_COUNT = 5;
    private static final String PROPERTY_PREFIX = "Property_";
    private static final ArrayList<Function<Integer, String>> GENERATOR_FUNCTIONS = new ArrayList<>();

    static {
        GENERATOR_FUNCTIONS.add(ComponentConfigTests::getStringValue);
        GENERATOR_FUNCTIONS.add(propertyId -> Integer.toString(ComponentConfigTests.getInt32Value(propertyId)));
        GENERATOR_FUNCTIONS.add(propertyId -> Long.toString(ComponentConfigTests.getInt64Value(propertyId)));
        GENERATOR_FUNCTIONS.add(propertyId -> Boolean.toString(ComponentConfigTests.getBooleanValue(propertyId)));
    }

    /**
     * Tests the ability to get a property as a String.
     */
    @Test
    public void testGetStringProperty() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, ComponentConfig::getProperty, value -> true);
    }

    /**
     * Tests the ability to get a property as an Int32.
     */
    @Test
    public void testGetInt32Property() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, ComponentConfig::getInt32Property, ComponentConfigTests::isInt32);
    }

    /**
     * Tests the ability to get a property as an Int64.
     */
    @Test
    public void testGetInt64Property() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, ComponentConfig::getInt64Property, ComponentConfigTests::isInt64);
    }

    /**
     * Tests the ability to get a property as a Boolean.
     */
    @Test
    public void testGetBooleanProperty() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, ComponentConfig::getBooleanProperty,
                value -> value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"));
    }

    private <T> void testData(Properties props, ExtractorFunction<T> methodToTest, Predicate<String> valueValidator)
            throws Exception {
        for (int componentId = 0; componentId < ComponentConfigTests.COMPONENT_COUNT; componentId++) {
            String componentCode = getComponentCode(componentId);
            ComponentConfig config = new TestConfig(props, componentCode);
            for (String fullyQualifiedPropertyName : props.stringPropertyNames()) {
                int propertyId = getPropertyId(fullyQualifiedPropertyName);
                String propName = getPropertyName(propertyId);
                String expectedValue = props.getProperty(fullyQualifiedPropertyName);
                if (fullyQualifiedPropertyName.startsWith(componentCode)) {
                    // This property belongs to this component. Check it out.
                    if (valueValidator.test(config.getProperty(propName))) {
                        // This is a value that should exist and be returned by methodToTest.
                        String actualValue = methodToTest.apply(config, propName).toString();
                        Assert.assertEquals("Unexpected value returned by extractor.", expectedValue, actualValue);
                    } else {
                        AssertExtensions.assertThrows(String.format(
                                "ComponentConfig returned property and interpreted it with the wrong " + "type. " +
                                        "PropertyName: %s, Value: %s.",
                                fullyQualifiedPropertyName, expectedValue), () -> methodToTest.apply(config, propName),
                                ex -> !(ex instanceof MissingPropertyException));
                    }
                } else {
                    // This is a different component. Make sure it is not included here.
                    AssertExtensions.assertThrows(String.format(
                            "ComponentConfig returned property that was for a different component. " +
                                    "PropertyName:" + " %s, Value: %s.",
                            fullyQualifiedPropertyName, expectedValue), () -> methodToTest.apply(config, propName),
                            ex -> ex instanceof MissingPropertyException);
                }
            }
        }
    }

    private void populateData(Properties props) {
        int propertyId = 0;
        for (int componentId = 0; componentId < ComponentConfigTests.COMPONENT_COUNT; componentId++) {
            String componentCode = getComponentCode(componentId);
            for (Function<Integer, String> gf : ComponentConfigTests.GENERATOR_FUNCTIONS) {
                populateSingleTypeData(props, componentCode, propertyId,
                        ComponentConfigTests.PROPERTY_OF_TYPE_PER_COMPONENT_COUNT, gf);
                propertyId += ComponentConfigTests.PROPERTY_OF_TYPE_PER_COMPONENT_COUNT;
            }
        }
    }

    private void populateSingleTypeData(Properties props, String code, int startIndex, int count, Function<Integer,
            String> valueGenerator) {
        for (int i = 0; i < count; i++) {
            int propertyId = i + startIndex;
            props.setProperty(getFullyQualifiedPropertyName(code, propertyId), valueGenerator.apply(propertyId));
        }
    }

    private static String getComponentCode(int componentId) {
        return String.format("Component_%s", componentId);
    }

    private static String getFullyQualifiedPropertyName(String code, int propertyId) {
        return String.format("%s.%s", code, getPropertyName(propertyId));
    }

    private static String getPropertyName(int propertyId) {
        return String.format("%s%d", PROPERTY_PREFIX, propertyId);
    }

    private static int getPropertyId(String fullyQualifiedPropertyName) {
        int pos = fullyQualifiedPropertyName.indexOf(PROPERTY_PREFIX);
        if (pos < 0) {
            Assert.fail(
                    "Internal test error: Unable to determine property if from property name " +
                            fullyQualifiedPropertyName);
        }

        return Integer.parseInt(fullyQualifiedPropertyName.substring(pos + PROPERTY_PREFIX.length()));
    }

    private static int getInt32Value(int propertyId) {
        return -propertyId;
    }

    private static long getInt64Value(int propertyId) {
        return propertyId + (long) Integer.MAX_VALUE * 2;
    }

    private static boolean getBooleanValue(int propertyId) {
        return propertyId % 2 == 1;
    }

    private static String getStringValue(int propertyId) {
        return "String_" + Integer.toHexString(propertyId);
    }

    private static boolean isInt32(String propertyValue) {
        return isInt64(propertyValue) && propertyValue.charAt(0) == '-'; // only getInt32Value generates negative
        // numbers.
    }

    private static boolean isInt64(String propertyValue) {
        char firstChar = propertyValue.charAt(0);
        return Character.isDigit(firstChar) || firstChar == '-'; // this will accept both Int32 and Int64.
    }

    private static class TestConfig extends ComponentConfig {
        public TestConfig(Properties properties, String componentCode) {
            super(properties, componentCode);
        }

        @Override
        protected void refresh() {
        }
    }

    private interface ExtractorFunction<R> {
        R apply(ComponentConfig config, String propName);
    }
}
