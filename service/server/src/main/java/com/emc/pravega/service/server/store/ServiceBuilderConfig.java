/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.store;

import com.emc.pravega.common.util.ComponentConfig;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.function.Function;

/**
 * Configuration for ServiceBuilder.
 */
@Slf4j
public class ServiceBuilderConfig {
    //region Members

    private final Properties properties;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ServiceBuilderConfig class.
     *
     * @param properties The Properties object to wrap.
     */
    public ServiceBuilderConfig(Properties properties) {
        Preconditions.checkNotNull(properties, "properties");
        this.properties = properties;
    }

    //endregion

    /**
     * Gets a new instance of a ComponentConfig for this builder.
     *
     * @param constructor The constructor for the new instance.
     * @param <T>         The type of the ComponentConfig to instantiate.
     */
    public <T extends ComponentConfig> T getConfig(Function<Properties, ? extends T> constructor) {
        return constructor.apply(this.properties);
    }

    //region Default Configuration

    /**
     * Gets a set of configuration values from the default config file.
     *
     * @return Service builder config read from the default config file.
     * @throws IOException If the config file can not be read from.
     */
    public static ServiceBuilderConfig getConfigFromFile() throws IOException {
        FileReader reader = null;
        try {
            reader = new FileReader("config.properties");
            return getConfigFromStream(reader);
        } catch (IOException e) {
            log.warn("Unable to read configuration because of exception " + e.getMessage());
            throw e;
        }
    }

    /**
     * Gets a set of configuration values from a given InputStreamReader.
     *
     * @param reader the InputStreamReader from which to read the configuration.
     * @return A ServiceBuilderConfig object.
     * @throws IOException If an exception occurred during reading of the configuration.
     */
    public static ServiceBuilderConfig getConfigFromStream(InputStreamReader reader) throws IOException {
        Properties p = new Properties();
        p.load(reader);
        return new ServiceBuilderConfig(p);
    }

    /**
     * Gets a default set of configuration values, in absence of any real configuration.
     * These configuration values are the default ones from all component configurations, except that it will
     * create only one container to host segments.
     */
    public static ServiceBuilderConfig getDefaultConfig() {
        Properties p = new Properties();

        // General params.
        set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_CONTAINER_COUNT, "1");

        // All component configs should have defaults built-in, so no need to override them here.
        return new ServiceBuilderConfig(p);
    }

    /**
     * Sets the given property in the given Properties object using the format expected by ComponentConfig, rooted under
     * ServiceConfig.
     *
     * @param p             The Properties object to update.
     * @param componentCode The name of the component code.
     * @param propertyName  The name of the property.
     * @param value         The value of the property.
     */
    public static void set(Properties p, String componentCode, String propertyName, String value) {
        String key = String.format("%s.%s", componentCode, propertyName);
        p.setProperty(key, value);
    }

    //endregion
}
