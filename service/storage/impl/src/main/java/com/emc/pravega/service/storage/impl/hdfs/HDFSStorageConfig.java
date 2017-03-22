/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
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

    public static final Property<String> URL = Property.named("hdfsUrl", "localhost:9000");
    public static final Property<String> ROOT = Property.named("hdfsRoot", "");
    public static final Property<Integer> PRAVEGA_ID = Property.named("pravegaId", -1);
    public static final Property<Integer> REPLICATION = Property.named("replication", 3);
    public static final Property<Integer> BLOCK_SIZE = Property.named("blockSize", 1024 * 1024);
    private static final String COMPONENT_CODE = "hdfs";

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
        this.hdfsHostURL = properties.get(URL);
        this.hdfsRoot = properties.get(ROOT);
        this.pravegaId = readPravegaID(properties);

        this.replication = (short) properties.getInt(REPLICATION);
        this.blockSize = properties.getInt(BLOCK_SIZE);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<HDFSStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, HDFSStorageConfig::new);
    }

    private int readPravegaID(TypedProperties properties) {
        int id = properties.getInt(PRAVEGA_ID);

        //Generate the pravega id from the IP address. This has to be dynamic and can not be default.
        if (id == PRAVEGA_ID.getDefaultValue()) {
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
