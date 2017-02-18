/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.common.util.PropertyBag;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.writer.WriterConfig;

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
     */
    public static DurableLogConfig createDurableLogConfig(PropertyBag rawProperties) {
        return new DurableLogConfig(convert(rawProperties, DurableLogConfig.COMPONENT_CODE));
    }

    /**
     * Creates a new instance of the ReadIndexConfig class with given arguments.
     *
     * @param rawProperties The properties to include.
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
