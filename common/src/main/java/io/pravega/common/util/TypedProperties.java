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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Properties;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * *
 * Wrapper for a java.util.Properties object, that sections it based on a namespace. Each property in the wrapped object
 * will be prefixed by a namespace.
 * <p>
 * Example:
 * <ul>
 * <li>foo.key1=value1
 * <li>foo.key2=value2
 * <li>bar.key1=value3
 * <li>bar.key3=value4
 * </ul>
 * Indicate that namespace "foo" has Key-Values (key1=value1, key2=value2), and namespace "bar" has (key1=value3, key3=value4).
 */
@Slf4j
public class TypedProperties {
    //region Members
    private static final String SEPARATOR = ".";

    private final String keyPrefix;
    private final Properties properties;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TypedProperties class.
     *
     * @param properties The java.util.Properties to wrap.
     * @param namespace  The namespace of this instance.
     */
    public TypedProperties(Properties properties, String namespace) {
        Preconditions.checkNotNull(properties, "properties");
        Exceptions.checkNotNullOrEmpty(namespace, "namespace");
        this.properties = properties;
        this.keyPrefix = namespace + SEPARATOR;
    }

    //endregion

    //region Getters

    /**
     * Gets the value of a String property.
     *
     * @param property The Property to get.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set.
     */
    public String get(Property<String> property) throws ConfigurationException {
        return tryGet(property, s -> s);
    }

    /**
     * Gets the value of an Integer property.
     *
     * @param property The Property to get.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as an Integer.
     */
    public int getInt(Property<Integer> property) throws ConfigurationException {
        return tryGet(property, Integer::parseInt);
    }

    /**
     * Gets the value of a Long property.
     *
     * @param property The Property to get.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as a Long.
     */
    public long getLong(Property<Long> property) throws ConfigurationException {
        return tryGet(property, Long::parseLong);
    }

    /**
     * Gets the value of a Double property.
     *
     * @param property The Property to get.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as a Double.
     */
    public double getDouble(Property<Double> property) throws ConfigurationException {
        return tryGet(property, Double::parseDouble);
    }
    
    /**
     * Gets the value of an Enumeration property.
     *
     * @param property  The Property to get.
     * @param enumClass Class defining return type.
     * @param <T>       Type of Enumeration.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as the given Enum.
     */
    public <T extends Enum<T>> T getEnum(Property<T> property, Class<T> enumClass) throws ConfigurationException {
        return tryGet(property, s -> Enum.valueOf(enumClass, s));
    }

    /**
     * Gets the value of a boolean property.
     * Notes:
     * <ul>
     * <li> "true", "yes" and "1" (case insensitive) map to boolean "true".
     * <li> "false", "no" and "0" (case insensitive) map to boolean "false".
     * </ul>
     *
     * @param property The Property to get.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as a Boolean.
     */
    public boolean getBoolean(Property<Boolean> property) throws ConfigurationException {
        return tryGet(property, this::parseBoolean);
    }

    /**
     * Gets the value of an Integer property only if it is greater than 0.
     *
     * @param property The Property to get.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as a positive Integer.
     */
    public int getPositiveInt(Property<Integer> property) {
        int value = getInt(property);
        if (value <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", property));
        }
        return value;
    }

    /**
     * Gets the value of an Integer property only if it is non-negative (greater than or equal to 0).
     *
     * @param property The Property to get.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as a
     *                                non-negative Integer.
     */
    public int getNonNegativeInt(Property<Integer> property) {
        int value = getInt(property);
        if (value < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a non-negative integer.", property));
        }
        return value;
    }

    /**
     * Gets the value of an Long property only if it is greater than 0.
     *
     * @param property The Property to get.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as a positive Long.
     */
    public long getPositiveLong(Property<Long> property) {
        long value = getLong(property);
        if (value <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive long.", property));
        }
        return value;
    }

    /**
     * Gets a Duration from an Integer property only if it is greater than 0.
     *
     * @param property The Property to get.
     * @param unit Temporal unit related to the value associated to this property (i.e, seconds, millis).
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed as a positive Integer.
     */
    public Duration getDuration(Property<Integer> property, TemporalUnit unit) {
        return Duration.of(getPositiveInt(property), unit);
    }

    private <T> T tryGet(Property<T> property, Function<String, T> converter) {
        String propNewName = this.keyPrefix + property.getName();
        String propOldName = this.keyPrefix + property.getLegacyName();
        String propValue = null;

        if (property.hasLegacyName()) {
            // Value of property with old name
            propValue = this.properties.getProperty(propOldName, null);
            if (propValue != null) {
                log.warn("Deprecated property name '{}' used. Please use '{}' instead. Support for the old " +
                        "property name will be removed in a future version of Pravega.", propOldName, propNewName);
            }
        }

        if (propValue == null) {
            // Value of property with new name
            propValue = this.properties.getProperty(propNewName, null);
        }

        // This property wasn't specified using either new or old name, so the property value is still null
        if (propValue == null) {
            if (property.hasDefaultValue()) {
                return property.getDefaultValue();
            } else {
                throw new MissingPropertyException(
                        String.format("Missing property with name [%s] (new name) / [%s] (old name}",
                        propNewName, propOldName));
            }
        }

        try {
            return converter.apply(propValue.trim());
        } catch (IllegalArgumentException ex) {
            throw new InvalidPropertyValueException(propNewName, propValue, ex);
        }
    }

    private boolean parseBoolean(String value) {
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("1")) {
            return true;
        } else if (value.equalsIgnoreCase("false") || value.equalsIgnoreCase("no") || value.equalsIgnoreCase("0")) {
            return false;
        } else {
            throw new IllegalArgumentException(String.format("String '%s' cannot be interpreted as a valid Boolean.", value));
        }
    }

    //endregion
}
