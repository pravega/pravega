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

import io.pravega.test.common.AssertExtensions;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the TypedProperties class.
 */
public class TypedPropertiesTests {
    private static final int COMPONENT_COUNT = 5;
    private static final int PROPERTY_OF_TYPE_PER_COMPONENT_COUNT = 5;
    private static final String PROPERTY_PREFIX = "Property_";
    private static final List<Function<Integer, String>> GENERATOR_FUNCTIONS = Arrays.asList(
            TypedPropertiesTests::getStringValue,
            propertyId -> toString(TypedPropertiesTests.getInt32Value(propertyId)),
            propertyId -> toString(TypedPropertiesTests.getInt64Value(propertyId)),
            propertyId -> toString(TypedPropertiesTests.getBooleanValue(propertyId)),
            propertyId -> toString(TypedPropertiesTests.getEnumValue(propertyId)));

    /**
     * Tests the ability to get a property as a String.
     */
    @Test
    public void testGetString() throws Exception {
        Properties props = new Properties();
        populateData(props);

        // Any type can be interpreted as String.
        testData(props, TypedProperties::get, value -> true);
    }

    /**
     * Tests the ability to get a property as an Integer.
     */
    @Test
    public void testGetInt() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, TypedProperties::getInt, TypedPropertiesTests::isInt32);
    }

    /**
     * Tests the ability to get a property as a Long.
     */
    @Test
    public void testGetLong() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, TypedProperties::getLong, TypedPropertiesTests::isInt64);
    }

    /**
     * Tests the ability to get a property as a Boolean.
     */
    @Test
    public void testGetBoolean() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, TypedProperties::getBoolean, TypedPropertiesTests::isBoolean);
    }

    /**
     * Tests the ability to get a property as an Enum.
     */
    @Test
    public void testGetEnum() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props,
                (TypedProperties config, Property<TestEnum> property) -> config.getEnum(property, TestEnum.class),
                TypedPropertiesTests::isEnum);
    }

    private <T> void testData(Properties props, ExtractorFunction<T> methodToTest, Predicate<String> valueValidator) throws Exception {
        for (int componentId = 0; componentId < TypedPropertiesTests.COMPONENT_COUNT; componentId++) {
            String componentCode = getComponentCode(componentId);
            TypedProperties config = new TypedProperties(props, componentCode);
            for (String fullyQualifiedPropertyName : props.stringPropertyNames()) {
                int propertyId = getPropertyId(fullyQualifiedPropertyName);
                String propName = getPropertyName(propertyId);
                Property<T> property = Property.named(propName);
                Property<String> stringProperty = Property.named(propName);
                String expectedValue = props.getProperty(fullyQualifiedPropertyName);
                if (fullyQualifiedPropertyName.startsWith(componentCode)) {
                    // This property belongs to this component. Check it out.
                    if (valueValidator.test(config.get(stringProperty))) {
                        // This is a value that should exist and be returned by methodToTest.
                        String actualValue = toString(methodToTest.apply(config, property));
                        Assert.assertEquals("Unexpected value returned by extractor.", expectedValue, actualValue);
                    } else {
                        AssertExtensions.assertThrows(
                                String.format("TypedProperties returned property and interpreted it with the wrong type. PropertyName: %s, Value: %s.", fullyQualifiedPropertyName, expectedValue),
                                () -> methodToTest.apply(config, property),
                                ex -> !(ex instanceof MissingPropertyException));
                    }
                } else {
                    // This is a different component. Make sure it is not included here.
                    AssertExtensions.assertThrows(
                            String.format("TypedProperties returned property that was for a different component. PropertyName: %s, Value: %s.", fullyQualifiedPropertyName, expectedValue),
                            () -> methodToTest.apply(config, property),
                            ex -> ex instanceof MissingPropertyException);
                }
            }
        }
    }

    private void populateData(Properties props) {
        int propertyId = 0;
        for (int componentId = 0; componentId < TypedPropertiesTests.COMPONENT_COUNT; componentId++) {
            String componentCode = getComponentCode(componentId);
            for (Function<Integer, String> gf : TypedPropertiesTests.GENERATOR_FUNCTIONS) {
                populateSingleTypeData(props, componentCode, propertyId, TypedPropertiesTests.PROPERTY_OF_TYPE_PER_COMPONENT_COUNT, gf);
                propertyId += TypedPropertiesTests.PROPERTY_OF_TYPE_PER_COMPONENT_COUNT;
            }
        }
    }

    private void populateSingleTypeData(Properties props, String code, int startIndex, int count, Function<Integer, String> valueGenerator) {
        for (int i = 0; i < count; i++) {
            int propertyId = i + startIndex;
            props.setProperty(getFullyQualifiedPropertyName(code, propertyId), valueGenerator.apply(propertyId));
        }
    }

    private static String getComponentCode(int componentId) {
        return String.format("Component_%s", componentId);
    }

    private static String getFullyQualifiedPropertyName(String code, int propertyId) {
        return getFullyQualifiedPropertyName(code, getPropertyName(propertyId));
    }

    private static String getFullyQualifiedPropertyName(String code, String propertyName) {
        return String.format("%s.%s", code, propertyName);
    }

    private static String getPropertyName(int propertyId) {
        return String.format("%s%d", PROPERTY_PREFIX, propertyId);
    }

    private static int getPropertyId(String fullyQualifiedPropertyName) {
        int pos = fullyQualifiedPropertyName.indexOf(PROPERTY_PREFIX);
        if (pos < 0) {
            Assert.fail("Internal test error: Unable to determine property if from property name " + fullyQualifiedPropertyName);
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

    private static TestEnum getEnumValue(int propertyId) {
        switch (propertyId % 3) {
            case 0:
                return TestEnum.Value1;
            case 1:
                return TestEnum.Value2;
            case 2:
            default:
                return TestEnum.Value3;
        }
    }

    private static String getStringValue(int propertyId) {
        return "String_" + Integer.toHexString(propertyId);
    }

    private static boolean isInt32(String propertyValue) {
        return isInt64(propertyValue) && propertyValue.charAt(0) == '-'; // only getInt32Value generates negative numbers.
    }

    private static boolean isInt64(String propertyValue) {
        char firstChar = propertyValue.charAt(0);
        return Character.isDigit(firstChar) || firstChar == '-'; // this will accept both Int32 and Int64.
    }

    private static boolean isBoolean(String propertyValue) {
        return propertyValue.equalsIgnoreCase("true") || propertyValue.equalsIgnoreCase("false");
    }

    private static boolean isEnum(String propertyValue) {
        try {
            TestEnum.valueOf(propertyValue);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String toString(Object o) {
        return o.toString();
    }

    private enum TestEnum {
        Value1,
        Value2,
        Value3
    }

    private interface ExtractorFunction<R> {
        R apply(TypedProperties config, Property<R> property);
    }
}
