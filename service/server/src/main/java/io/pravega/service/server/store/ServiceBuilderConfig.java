/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.store;

import io.pravega.common.util.ConfigBuilder;
import com.google.common.base.Preconditions;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

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
    private ServiceBuilderConfig(Properties properties) {
        Preconditions.checkNotNull(properties, "properties");
        this.properties = properties;
    }

    /**
     * Creates a new Builder for this class.
     *
     * @return The created Builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    //endregion

    /**
     * Gets a new instance of a Configuration for this builder.
     *
     * @param constructor A Supplier for a ConfigBuilder for the given Configuration.
     * @param <T>         The type of the Configuration to instantiate.
     */
    public <T> T getConfig(Supplier<? extends ConfigBuilder<? extends T>> constructor) {
        return constructor.get()
                          .rebase(this.properties)
                          .build();
    }

    //region Default Configuration

    /**
     * Gets a default set of configuration values, in absence of any real configuration.
     * These configuration values are the default ones from all component configurations, except that it will
     * create only one container to host segments.
     */
    public static ServiceBuilderConfig getDefaultConfig() {
        // All component configs should have defaults built-in, so no need to override them here.
        return new Builder()
                .include(ServiceConfig.builder().with(ServiceConfig.CONTAINER_COUNT, 1))
                .build();
    }

    //endregion

    //region Builder

    /**
     * Represents a Builder for the ServiceBuilderConfig.
     */
    public static class Builder {
        private final Properties properties;

        private Builder() {
            this.properties = new Properties();
        }

        /**
         * Loads configuration values from the given config file.
         *
         * @param filePath The path to the file to read form.
         * @return This instance.
         * @throws IOException If the config file can not be read from.
         */
        public Builder include(String filePath) throws IOException {
            try (FileReader reader = new FileReader(filePath)) {
                this.properties.load(reader);
            }

            return this;
        }

        /**
         * Includes the given Builder into this Builder.
         *
         * @param builder The Builder to include.
         * @param <T>     Type of the Configuration to include.
         * @return This instance.
         */
        public <T> Builder include(ConfigBuilder<T> builder) {
            builder.copyTo(this.properties);
            return this;
        }

        /**
         * Includes the given Properties into this Builder.
         *
         * @param p The properties to include.
         * @return This instance.
         */
        public Builder include(Properties p) {
            this.properties.putAll(p);
            return this;
        }

        /**
         * Creates a new instance of the ServiceBuilderConfig class with the information contained in this builder.
         *
         * @return The newly created ServiceBuilderConfig.
         */
        public ServiceBuilderConfig build() {
            return new ServiceBuilderConfig(this.properties);
        }
    }

    //endregion
}
