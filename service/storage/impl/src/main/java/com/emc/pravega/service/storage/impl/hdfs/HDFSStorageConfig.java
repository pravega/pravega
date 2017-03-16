/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.TypedProperties;
import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the HDFS Storage component.
 */
@Slf4j
public class HDFSStorageConfig {
    //region Config Names

    public static final String PROPERTY_HDFS_URL = "fs.default.name";
    public static final String PROPERTY_HDFS_ROOT = "hdfsRoot";
    public static final String PROPERTY_PRAVEGA_ID = "pravegaId";
    public static final String PROPERTY_REPLICATION = "replication";
    public static final String PROPERTY_BLOCK_SIZE = "blockSize";
    private static final String COMPONENT_CODE = "hdfs";

    private static final String DEFAULT_HDFS_URL = "localhost:9000";
    private static final String DEFAULT_HDFS_ROOT = "";
    private static final int DEFAULT_PRAVEGA_ID = -1;
    private static final int DEFAULT_REPLICATION = 1;
    private static final int DEFAULT_BLOCK_SIZE = 1048576;

    //endregion

    //region Members

    /**
     * HDFS host URL. This is generally in host:port format
     */
    @Getter
    private final String hdfsHostURL;

    /**
     * Root of the Pravega owned HDFS path. All the directories/files under this path will be exclusively
     * owned by Pravega.
     */
    @Getter
    private final String hdfsRoot;
    @Getter
    private final int pravegaId;

    /**
     * Decides the replication factor of the data stored on HDFS.
     * This can be used to control availability of HDFS data.
     */
    @Getter
    private final short replication;

    /**
     * The default block size that will be used to exchange data with HDFS.
     *
     * @return block size for HDFS
     */
    @Getter
    private final long blockSize;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private HDFSStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.hdfsHostURL = properties.get(PROPERTY_HDFS_URL, DEFAULT_HDFS_URL);
        this.hdfsRoot = properties.get(PROPERTY_HDFS_ROOT, DEFAULT_HDFS_ROOT);
        this.pravegaId = readPravegaID(properties);

        this.replication = (short) properties.getInt32(PROPERTY_REPLICATION, DEFAULT_REPLICATION);
        this.blockSize = properties.getInt32(PROPERTY_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<HDFSStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, HDFSStorageConfig::new);
    }

    private int readPravegaID(TypedProperties properties) {
        int id = properties.getInt32(PROPERTY_PRAVEGA_ID, DEFAULT_PRAVEGA_ID);

        //Generate the pravega id from the IP address. This has to be dynamic and can not be default.
        if (id == DEFAULT_PRAVEGA_ID) {
            try {
                int hashCode = InetAddress.getLocalHost().getHostName().hashCode();
                if (hashCode == Integer.MIN_VALUE) {
                    hashCode = 0;
                }
                id = Math.abs(hashCode);
            } catch (UnknownHostException e) {
                id = 0;
                log.warn("Exception {} while getting unique Pravega ID for this host. Using {} as default value",
                        e.getMessage(), this.pravegaId);
            }
        }
        return id;
    }

    //endregion
}
