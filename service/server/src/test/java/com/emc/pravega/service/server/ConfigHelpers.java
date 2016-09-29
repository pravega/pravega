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

package com.emc.pravega.service.server;

import com.emc.pravega.service.config.DurableLogConfig;
import com.emc.pravega.service.config.ReadIndexConfig;
import com.emc.pravega.service.config.ServiceBuilderConfig;
import com.emc.pravega.service.config.WriterConfig;

import java.util.Map;
import java.util.Properties;

/**
 * Helper class that can be used to quickly create Configurations.
 */
public class ConfigHelpers {
    /**
     * Creates a new instance of the DurableLogConfig class with given arguments.
     *
     * @param rawProperties The properties to include.
     * @return
     */
    public static DurableLogConfig createDurableLogConfig(PropertyBag rawProperties) {
        return new DurableLogConfig(convert(rawProperties, DurableLogConfig.COMPONENT_CODE));
    }

    /**
     * Creates a new instance of the ReadIndexConfig class with given arguments.
     *
     * @param rawProperties The properties to include.
     * @return
     */
    public static ReadIndexConfig createReadIndexConfigWithInfiniteCachePolicy(PropertyBag rawProperties) {
        rawProperties
                .with(ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_SIZE, Long.MAX_VALUE)
                .with(ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_TIME, Integer.MAX_VALUE)
                .with(ReadIndexConfig.PROPERTY_CACHE_POLICY_GENERATION_TIME, Integer.MAX_VALUE);

        return new ReadIndexConfig(convert(rawProperties, ReadIndexConfig.COMPONENT_CODE));
    }

    /**
     * Creates a new instance of the WriterConfig class with given arguments.
     *
     * @param rawProperties The properties to include.
     * @return
     */
    public static WriterConfig createWriterConfig(PropertyBag rawProperties) {
        return new WriterConfig(convert(rawProperties, WriterConfig.COMPONENT_CODE));
    }

    private static Properties convert(Properties rawProperties, String componentCode) {
        Properties p = new Properties();
        for (Map.Entry<Object, Object> e : rawProperties.entrySet()) {
            ServiceBuilderConfig.set(p, componentCode, e.getKey().toString(), e.getValue().toString());
        }

        return p;
    }
}
