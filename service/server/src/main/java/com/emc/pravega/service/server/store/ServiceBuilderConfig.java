/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     */
    public static ServiceBuilderConfig getDefaultConfig() {
        FileReader reader = null;
        try {
            reader = new FileReader("config.properties");
            return getConfigFromStream(reader);
        } catch (IOException e) {
            log.warn("Unable to read configuration because of exception " + e.getMessage());
            return getDefaultConfigHardCoded();
        }
    }

    /**
     * Gets a set of configuration values from a given InputStreamReader.
     * @param  reader the InputStreamReader from which to read the configuration.
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
     */
    private static ServiceBuilderConfig getDefaultConfigHardCoded() {
        Properties p = new Properties();

        // General params
        set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_CONTAINER_COUNT, "1");
        set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_THREAD_POOL_SIZE, "50");
        set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_LISTENING_PORT, "12345");

        // DistributedLog params.
        set(p, "dlog", "hostname", "zk1");
        set(p, "dlog", "port", "2181");
        set(p, "dlog", "namespace", "messaging/distributedlog/mynamespace");

        //HDFS params
        set(p, "hdfs", "fs.default.name", "localhost:9000");
        set(p, "hdfs", "hdfsRoot", "");
        set(p, "hdfs", "pravegaId", "0");
        set(p, "hdfs", "replication", "1");
        set(p, "hdfs", "blockSize", "1048576");

        // DurableLogConfig, WriterConfig, ReadIndexConfig all have defaults built-in, so no need to override them here.
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
