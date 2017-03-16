/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
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
    private static final String ENV_VARIABLE_TOKEN = "$";

    private final String keyPrefix;
    private final Properties properties;
    private final Function<String, String> getEnv;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TypedProperties class.
     *
     * @param properties The java.util.Properties to wrap.
     * @param namespace  The namespace of this instance.
     */
    TypedProperties(Properties properties, String namespace) {
        this(properties, namespace, System::getenv);
    }

    /**
     * Creates a new instance of the TypedProperties class.
     *
     * @param properties The java.util.Properties to wrap.
     * @param namespace  The namespace of this instance.
     * @param getEnv     A Function that can be used to extract environment variables.
     */
    @VisibleForTesting
    TypedProperties(Properties properties, String namespace, Function<String, String> getEnv) {
        this.properties = properties;
        this.keyPrefix = namespace + SEPARATOR;
        this.getEnv = getEnv;
    }

    //endregion

    //region Getters

    /**
     * Gets the value of a String property. The order priority in descending order is as follows:
     * 1. Env variable  - if a property is defined in the env variable, it is given the highest precedence.
     * 2. Config file   - if the value is defined in a config file, it will be returned.
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

        // 1. Try to get value from Environment based on Property name.
        String value = this.getEnv.apply(fullKeyName.replace('.', '_'));
        if (value == null) {
            // 2. Nothing for the property in the environment; get value from config.
            value = this.properties.getProperty(fullKeyName, null);

            // 3. Attempt to interpret its value as an environment variable.
            value = extractEnvironmentVariable(value);
        }

        if (value == null) {
            // 4. Nothing in the environment or in the configuration for this Property.
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

    private String extractEnvironmentVariable(String value) {
        if (value != null
                && value.length() > ENV_VARIABLE_TOKEN.length() * 2
                && value.startsWith(ENV_VARIABLE_TOKEN)
                && value.endsWith(ENV_VARIABLE_TOKEN)) {
            value = this.getEnv.apply(value.substring(ENV_VARIABLE_TOKEN.length(), value.length() - ENV_VARIABLE_TOKEN.length()));
        }

        return value;
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
            StringUtils.parse(
                    value,
                    ",",
                    "=",
                    new StringUtils.Int64Extractor("initialMillis", initialMillis::set),
                    new StringUtils.Int32Extractor("multiplier", multiplier::set),
                    new StringUtils.Int32Extractor("attempts", attempts::set),
                    new StringUtils.Int64Extractor("maxDelay", maxDelay::set));
            return Retry.withExpBackoff(initialMillis.get(), multiplier.get(), attempts.get(), maxDelay.get());
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("String '%s' cannot be interpreted as a valid Retry.RetryWithBackoff.", value), ex);
        }
    }

    //endregion
}
