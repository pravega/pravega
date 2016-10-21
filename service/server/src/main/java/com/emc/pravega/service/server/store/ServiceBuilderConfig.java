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
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.writer.WriterConfig;
import com.google.common.base.Preconditions;

import java.util.Properties;
import java.util.function.Function;

/**
 * Configuration for ServiceBuilder.
 */
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
     */
    public <T extends ComponentConfig> T getConfig(Function<Properties, ? extends T> constructor) {
        return constructor.apply(this.properties);
    }

    //region Default Configuration

    /**
     * Gets a default set of configuration values, in absence of any real configuration.
     */
    public static ServiceBuilderConfig getDefaultConfig() {
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
        set(p, "hdfs", "hdfsroot", "");
        set(p, "hdfs", "pravegaid", "0");
        set(p, "hdfs", "replication", "1");
        set(p, "hdfs", "blocksize", "1048576");

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
