/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.shared.common.util;

import com.emc.pravega.shared.Exceptions;
import com.google.common.base.Preconditions;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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
     * Gets the value of a RetryWithExponentialBackoff property, serialized in the form: "{key}={value},{key}={value}...",
     * where {key} is one of: "initialMillis", "multiplier", "attempts", "maxDelay", all of which correspond to constructor
     * arguments to Retry.RetryWithBackoff.
     *
     * @param property The Property to get.
     * @return The deserialized property value of default value, if no such property is defined in the base Properties.
     * @throws ConfigurationException When the given property name does not exist within the current component and the property
     *                                does not have a default value set, or when the property cannot be parsed.
     */
    public Retry.RetryWithBackoff getRetryWithBackoff(Property<Retry.RetryWithBackoff> property) throws ConfigurationException {
        return tryGet(property, this::parseRetryWithBackoff);
    }

    private <T> T tryGet(Property<T> property, Function<String, T> converter) {
        String fullKeyName = this.keyPrefix + property.getName();

        // Get value from config.
        String value = this.properties.getProperty(fullKeyName, null);

        if (value == null) {
            // 2. Nothing in the configuration for this Property.
            if (property.hasDefaultValue()) {
                return property.getDefaultValue();
            } else {
                throw new MissingPropertyException(fullKeyName);
            }
        }

        try {
            return converter.apply(value.trim());
        } catch (IllegalArgumentException ex) {
            throw new InvalidPropertyValueException(fullKeyName, value, ex);
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

    private Retry.RetryWithBackoff parseRetryWithBackoff(String value) {
        AtomicLong initialMillis = new AtomicLong();
        AtomicInteger multiplier = new AtomicInteger();
        AtomicInteger attempts = new AtomicInteger();
        AtomicLong maxDelay = new AtomicLong();
        try {
            DelimitedStringParser.parser(",", "=")
                                 .extractLong("initialMillis", initialMillis::set)
                                 .extractInteger("multiplier", multiplier::set)
                                 .extractInteger("attempts", attempts::set)
                                 .extractLong("maxDelay", maxDelay::set)
                                 .parse(value);
            return Retry.withExpBackoff(initialMillis.get(), multiplier.get(), attempts.get(), maxDelay.get());
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("String '%s' cannot be interpreted as a valid Retry.RetryWithBackoff.", value), ex);
        }
    }

    //endregion
}
