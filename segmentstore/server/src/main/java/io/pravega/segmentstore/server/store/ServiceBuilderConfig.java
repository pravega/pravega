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
package io.pravega.segmentstore.server.store;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ConfigBuilder;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Configuration for ServiceBuilder.
 */
@Slf4j
public class ServiceBuilderConfig {
    //region Members

    public static final String CONFIG_FILE_PROPERTY_NAME = "pravega.configurationFile";
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
     * @return The new instance of a Configuration for this builder.
     */
    public <T> T getConfig(Supplier<? extends ConfigBuilder<? extends T>> constructor) {
        return constructor.get()
                          .rebase(this.properties)
                          .build();
    }
    
    /**
     * Gets a new instance of a Builder for this builder.
     *
     * @param constructor A Supplier for a ConfigBuilder for the given Configuration.
     * @param <T>         The type of the Configuration to instantiate a builder for.
     * @return The new instance of a ConfigurationBuilder for this builder.
     */
    public <T> ConfigBuilder<? extends T> getConfigBuilder(Supplier<? extends ConfigBuilder<? extends T>> constructor) {
        return constructor.get()
                          .rebase(this.properties);
    }

    /**
     * Stores the contents of the ServiceBuilderConfig into the given File.
     *
     * @param file The file to store the contents in.
     * @throws IOException If an exception occurred.
     */
    public void store(File file) throws IOException {
        try (val s = new FileOutputStream(file, false)) {
            this.properties.store(s, "");
        }
    }

    /**
     * Executes the given BiConsumer on all non-default properties in this configuration.
     *
     * @param consumer The BiConsumer to execute.
     */
    public void forEach(BiConsumer<? super Object, ? super Object> consumer) {
        this.properties.forEach(consumer);
    }

    //region Default Configuration

    /**
     * Gets a default set of configuration values, in absence of any real configuration.
     * These configuration values are the default ones from all component configurations, except that it will
     * create only one container to host segments.
     * @return The default set of configuration values.
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
     * Returns the new instance of a Configuration.
     */
    public static class Builder {
        private final Properties properties;

        private Builder() {
            this.properties = new Properties();
        }

        /**
         * Creates a new instance of this class containing a copy of the existing configuration.
         *
         * @return A new instance of this class.
         */
        public Builder makeCopy() {
            return new Builder().include(this.properties);
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
