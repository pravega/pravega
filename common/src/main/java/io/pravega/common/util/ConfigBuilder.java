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
     * @param property The property to set.
     * @param value    The value of the property. This must be of the same type as accepted by the Property.
     *                 In case a `null` value is sent, the value of the property will be set to empty string.
     * @param <V>      Type of the property.
     * @return This instance.
     */
    public <V> ConfigBuilder<T> with(Property<V> property, V value) {
        String key = String.format("%s.%s", this.namespace, property.getName());
        this.properties.setProperty(key, value == null ? "" : value.toString());
        return this;
    }

    /**
     * Includes the given property and its value in the builder, without Property-Value type-enforcement.
     *
     * @param property The property to set.
     * @param value    The value of the property.
     * @return This instance.
     */
    public ConfigBuilder<T> withUnsafe(Property<?> property, Object value) {
        String key = String.format("%s.%s", this.namespace, property.getName());
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
