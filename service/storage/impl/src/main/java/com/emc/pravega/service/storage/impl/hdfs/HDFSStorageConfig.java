package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;

import java.util.Properties;

public class HDFSStorageConfig extends ComponentConfig {

    public static final String COMPONENT_CODE = "hdfs";

    public HDFSStorageConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    @Override
    protected void refresh() throws ConfigurationException {

    }
}
