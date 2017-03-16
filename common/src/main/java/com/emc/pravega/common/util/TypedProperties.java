/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import com.emc.pravega.common.Exceptions;
import com.google.common.annotations.VisibleForTesting;
import java.util.Properties;
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

    TypedProperties(Properties properties, String namespace) {
        this(properties, namespace, System::getenv);
    }

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
     * @param name The name of the property (no component code prefix).
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws MissingPropertyException When the given property name does not exist within the current component.
     */
    public String get(String name) throws MissingPropertyException {
        String value = get(name, null);
        if (value == null) {
            throw new MissingPropertyException(getKey(name));
        }

        return value;
    }

    /**
     * Gets the value of a String property. The order priority in descending order is as follows:
     * 1. Env variable  - if a property is defined in the env variable, it is given the highest precedence.
     * 2. Config file   - if the value is defined in a config file, it is given precedence over the default value.
     * 3. Default value - if the value is not defined in env as well as config file, default value is used.
     * <p>
     * This arrangement is to ensure that values are passed easily to docker containers. Docker container deployment
     * engines (e.g. marathon) can define these variables and the Pravega Service docker container will pick it up
     * with out any changes to the container.
     *
     * @param name         The name of the property (no component code prefix).
     * @param defaultValue The default value for the property.
     * @return The property value or default value, if no such is defined in the base Properties.
     */
    public String get(String name, String defaultValue) {
        String fullKeyName = getKey(name);

        String value;
        value = this.getEnv.apply(fullKeyName.replace('.', '_'));
        if (value == null) {
            // Get value from config.
            value = this.properties.getProperty(fullKeyName, defaultValue);

            // See if it can be interpreted as an environment variable.
            value = extractEnvironmentVariable(value, defaultValue);
        }
        return value;
    }

    /**
     * Gets the value of an Int32 property.
     *
     * @param name The name of the property (no component code prefix).
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws MissingPropertyException      When the given property name does not exist within the current component.
     * @throws InvalidPropertyValueException When the property cannot be parsed as an Int32.
     */
    public int getInt32(String name) throws MissingPropertyException, InvalidPropertyValueException {
        String value = get(name).trim();

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            throw new InvalidPropertyValueException(getKey(name), value, ex);
        }
    }

    /**
     * Gets the value of an Int32 property.
     *
     * @param name         The name of the property (no component code prefix).
     * @param defaultValue The default value for the property.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws InvalidPropertyValueException When the property cannot be parsed as an Int32.
     */
    public int getInt32(String name, int defaultValue) throws InvalidPropertyValueException {
        String value = get(name, null);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            throw new InvalidPropertyValueException(getKey(name), value, ex);
        }
    }

    /**
     * Gets the value of an Int64 property.
     *
     * @param name The name of the property (no component code prefix).
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws MissingPropertyException      When the given property name does not exist within the current component.
     * @throws InvalidPropertyValueException When the property cannot be parsed as an Int64.
     */
    public long getInt64(String name) throws MissingPropertyException, InvalidPropertyValueException {
        String value = get(name).trim();

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            throw new InvalidPropertyValueException(getKey(name), value, ex);
        }
    }

    /**
     * Gets the value of an Int64 property.
     *
     * @param name         The name of the property (no component code prefix).
     * @param defaultValue The default value for the property.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws InvalidPropertyValueException When the property cannot be parsed as an Int64.
     */
    public long getInt64(String name, long defaultValue) throws InvalidPropertyValueException {
        String value = get(name, null);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            throw new InvalidPropertyValueException(getKey(name), value, ex);
        }
    }

    /**
     * Gets the value of a boolean property.
     * Notes:
     * <ul>
     * <li> "true", "yes" and "1" (case insensitive) map to boolean "true".
     * <li> "false", "no" and "0" (case insensitive) map to boolean "false".
     * </ul>
     *
     * @param name The name of the property (no component code prefix).
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws MissingPropertyException      When the given property name does not exist within the current component.
     * @throws InvalidPropertyValueException When the property cannot be parsed as a Boolean.
     */
    public boolean getBoolean(String name) throws MissingPropertyException, InvalidPropertyValueException {
        String value = get(name).trim();

        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("1")) {
            return true;
        } else if (value.equalsIgnoreCase("false") || value.equalsIgnoreCase("no") || value.equalsIgnoreCase("0")) {
            return false;
        } else {
            throw new InvalidPropertyValueException(getKey(name), value);
        }
    }

    /**
     * Gets the value of a boolean property.
     * Notes:
     * <ul>
     * <li> "true", "yes" and "1" (case insensitive) map to boolean "true".
     * <li> "false", "no" and "0" (case insensitive) map to boolean "false".
     * </ul>
     *
     * @param name         The name of the property (no component code prefix).
     * @param defaultValue The default value for the property.
     * @return The property value or default value, if no such is defined in the base Properties.
     * @throws InvalidPropertyValueException When the property cannot be parsed as a Boolean.
     */
    public boolean getBoolean(String name, boolean defaultValue) throws InvalidPropertyValueException {
        String value = get(name, null);
        if (value == null) {
            return defaultValue;
        }

        value = value.trim();
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("1")) {
            return true;
        } else if (value.equalsIgnoreCase("false") || value.equalsIgnoreCase("no") || value.equalsIgnoreCase("0")) {
            return false;
        } else {
            throw new InvalidPropertyValueException(getKey(name), value);
        }
    }

    private String getKey(String name) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        return this.keyPrefix + name;
    }

    private String extractEnvironmentVariable(String value, String defaultValue) {
        if (value != null
                && value.length() > ENV_VARIABLE_TOKEN.length() * 2
                && value.startsWith(ENV_VARIABLE_TOKEN)
                && value.endsWith(ENV_VARIABLE_TOKEN)) {
            value = this.getEnv.apply(value.substring(ENV_VARIABLE_TOKEN.length(), value.length() - ENV_VARIABLE_TOKEN.length()));
        }

        return value != null ? value : defaultValue;
    }

    //endregion
}
