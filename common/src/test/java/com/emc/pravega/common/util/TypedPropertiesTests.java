/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.common.util;

import com.emc.pravega.testcommon.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the TypedProperties class.
 */
public class TypedPropertiesTests {
    private static final int COMPONENT_COUNT = 5;
    private static final int PROPERTY_OF_TYPE_PER_COMPONENT_COUNT = 5;
    private static final String PROPERTY_PREFIX = "Property_";
    private static final ArrayList<Function<Integer, String>> GENERATOR_FUNCTIONS = new ArrayList<>();

    static {
        GENERATOR_FUNCTIONS.add(TypedPropertiesTests::getStringValue);
        GENERATOR_FUNCTIONS.add(propertyId -> toString(TypedPropertiesTests.getInt32Value(propertyId)));
        GENERATOR_FUNCTIONS.add(propertyId -> toString(TypedPropertiesTests.getInt64Value(propertyId)));
        GENERATOR_FUNCTIONS.add(propertyId -> toString(TypedPropertiesTests.getBooleanValue(propertyId)));
        GENERATOR_FUNCTIONS.add(propertyId -> toString(ComponentConfigTests.getRetryWithBackoffValue(propertyId)));
    }

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
     * Tests the ability to get a property as an Int32.
     */
    @Test
    public void testGetInt32() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, TypedProperties::getInt32, TypedPropertiesTests::isInt32);
    }

    /**
     * Tests the ability to get a property as an Int64.
     */
    @Test
    public void testGetInt64() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, TypedProperties::getInt64, TypedPropertiesTests::isInt64);
    }

    /**
     * Tests the ability to get a property as a Boolean.
     */
    @Test
    public void testGetBoolean() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, TypedProperties::getBoolean, ComponentConfigTests::isBoolean);
    }

    /**
     * Tests the ability to get a property that converts to Retry.RetryWithBackoff.
     */
    @Test
    public void testGetRetryWithBackoffProperty() throws Exception {
        Properties props = new Properties();
        populateData(props);
        testData(props, TypedProperties::getRetryWithBackoff, ComponentConfigTests::isRetry);
    }

    /**
     * Tests extracting and interpreting values as environment variables.
     */
    @Test
    public void testGetEnvironmentVariable() throws Exception {
        final String componentCode = "envvar";
        int index = 1;
        final String validEnvProp = getPropertyName(index++);
        final String invalidEnvProp1 = getPropertyName(index++);
        final String invalidEnvProp2 = getPropertyName(index++);
        final String invalidEnvProp3 = getPropertyName(index++);
        final String rawProp = getPropertyName(index++);
        val correctProperties = Collections.singleton(validEnvProp);
        val incorrectProperties = Arrays.asList(invalidEnvProp1, invalidEnvProp2, invalidEnvProp3, rawProp);
        Properties props = new Properties();
        props.setProperty(getFullyQualifiedPropertyName(componentCode, validEnvProp), "$env1$");
        props.setProperty(getFullyQualifiedPropertyName(componentCode, invalidEnvProp1), "env1$");
        props.setProperty(getFullyQualifiedPropertyName(componentCode, invalidEnvProp2), "$env1");
        props.setProperty(getFullyQualifiedPropertyName(componentCode, invalidEnvProp3), "a$env1$");
        props.setProperty(getFullyQualifiedPropertyName(componentCode, rawProp), "env1");

        val env = new HashMap<String, String>();
        props.forEach((key, value) -> env.put((String) value, "incorrect"));
        env.put("env1", "correct");

        TypedProperties config = new TypedProperties(props, componentCode, env::get);
        for (String p : correctProperties) {
            Property<String> property = new Property<>(p);
            Assert.assertEquals("Unexpected value from valid env var reference.", "correct", config.get(property));
            property = new Property<>(p, "incorrect");
            Assert.assertEquals("Unexpected value from valid env var reference (with default).", "correct", config.get(property));
        }

        for (String p : incorrectProperties) {
            String expectedValue = (String) props.get(getFullyQualifiedPropertyName(componentCode, p));
            Property<String> property = new Property<>(p);
            Assert.assertEquals("Unexpected value from invalid env var reference.", expectedValue, config.get(property));
            property = new Property<>(p, "correct");
            Assert.assertEquals("Unexpected value from invalid env var reference (with default).", expectedValue, config.get(property));
        }
    }

    private <T> void testData(Properties props, ExtractorFunction<T> methodToTest, Predicate<String> valueValidator) throws Exception {
        for (int componentId = 0; componentId < TypedPropertiesTests.COMPONENT_COUNT; componentId++) {
            String componentCode = getComponentCode(componentId);
            TypedProperties config = new TypedProperties(props, componentCode);
            for (String fullyQualifiedPropertyName : props.stringPropertyNames()) {
                int propertyId = getPropertyId(fullyQualifiedPropertyName);
                String propName = getPropertyName(propertyId);
                Property<T> property = new Property<>(propName);
                Property<String> stringProperty = new Property<>(propName);
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

    private static String getStringValue(int propertyId) {
        return "String_" + Integer.toHexString(propertyId);
    }

    private static Retry.RetryWithBackoff getRetryWithBackoffValue(int propertyId) {
        return Retry.withExpBackoff(propertyId, propertyId + 1, propertyId + 2, propertyId + 3);
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

    private static boolean isRetry(String propertyValue) {
        return propertyValue.contains(",") && propertyValue.contains("=");
    }

    private interface ExtractorFunction<R> {
        R apply(TypedProperties config, Property<R> property);
    }
}
