/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.common.util;

import com.emc.pravega.common.Exceptions;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Properties;

/**
 * A builder for a generic Property-based configuration.
 *
 * @param <T> Type of the configuration.
 */
public class ConfigBuilder<T> {
    private final Properties properties;
    private final String namespace;
    private final ConfigConstructor<T> constructor;

    /**
     * Creates a new instance of the ConfigBuilder class.
     *
     * @param namespace   The configuration namespace to use.
     * @param constructor A Function that, given a TypedProperties object, returns a new instance of T using the given
     *                    property values.
     */
    public ConfigBuilder(String namespace, ConfigConstructor<T> constructor) {
        this(namespace, new Properties(), constructor);
    }

    /**
     * Creates a new instance of the ConfigBuilder class.
     *
     * @param namespace   The configuration namespace to use.
     * @param properties  A java.util.Properties object to wrap.
     * @param constructor A Function that, given a TypedProperties object, returns a new instance of T using the given
     *                    property values.
     */
    private ConfigBuilder(String namespace, Properties properties, ConfigConstructor<T> constructor) {
        Exceptions.checkNotNullOrEmpty(namespace, "namespace");
        Preconditions.checkNotNull(properties, "properties");
        Preconditions.checkNotNull(constructor, "constructor");
        this.properties = properties;
        this.namespace = namespace;
        this.constructor = constructor;
    }

    /**
     * Creates a new instance of the ConfigBuilder class that uses the given java.util.Properties object as a base.
     * This method does not touch the current instance.
     *
     * @param properties A java.util.Properties object to wrap.
     * @return A new instance of the ConfigBuilder class with the same arguments as the current one, except that it uses
     * the given Properties object.
     */
    public ConfigBuilder<T> rebase(Properties properties) {
        return new ConfigBuilder<>(this.namespace, properties, this.constructor);
    }

    /**
     * Includes the given property and its value in the builder.
     *
     * @param propertyName The name of the property.
     * @param value        The value of the property.
     * @return This instance.
     */
    public ConfigBuilder<T> with(String propertyName, Object value) {
        String key = String.format("%s.%s", this.namespace, propertyName);
        this.properties.setProperty(key, value.toString());
        return this;
    }

    /**
     * Copies the contents of this builder to the given target.
     *
     * @param target A Map to copy to.
     */
    public void copyTo(Map<Object, Object> target) {
        target.putAll(this.properties);
    }

    /**
     * Creates a new instance of the given Configuration class as defined by this builder with the information
     * contained herein.
     *
     * @return The newly created instance.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given properties collection),
     *                                  NumberFormatException (a Property has a value that is invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If namespace is an empty string..
     */
    public T build() throws ConfigurationException {
        return this.constructor.apply(new TypedProperties(this.properties, this.namespace));
    }

    @FunctionalInterface
    public interface ConfigConstructor<R> {
        R apply(TypedProperties var1) throws ConfigurationException;
    }
}
