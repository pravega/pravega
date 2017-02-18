/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the HDFS Storage component.
 */
@Slf4j
public class HDFSStorageConfig extends ComponentConfig {

    public static final String COMPONENT_CODE = "hdfs";
    public static final String PROPERTY_HDFS_URL = "fs.default.name";
    public static final String PROPERTY_HDFS_ROOT = "hdfsRoot";
    public static final String PROPERTY_PRAVEGA_ID = "pravegaId";
    public static final String PROPERTY_REPLICATION = "replication";
    public static final String PROPERTY_BLOCK_SIZE = "blockSize";

    private static final String DEFAULT_HDFS_URL = "localhost:9000";
    private static final String DEFAULT_HDFS_ROOT = "";
    private static final int DEFAULT_PRAVEGA_ID = -1;
    private static final int DEFAULT_REPLICATION = 1;
    private static final int DEFAULT_BLOCK_SIZE = 1048576;

    private String hdfsHostURL;
    private String hdfsRoot;
    private int pravegaId;
    private short replication;
    private long blockSize;

    /**
     * Creates a new instance of the HDFSStorageConfig class.
     *
     * @param properties Properties to wrap.
     * @throws ConfigurationException If a required configuration item is missing or invalid.
     */
    public HDFSStorageConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    @Override
    protected void refresh() throws ConfigurationException {
        this.hdfsHostURL = getProperty(PROPERTY_HDFS_URL, DEFAULT_HDFS_URL);
        this.hdfsRoot = getProperty(PROPERTY_HDFS_ROOT, DEFAULT_HDFS_ROOT);
        this.pravegaId = readPravegaID();

        this.replication = (short) getInt32Property(PROPERTY_REPLICATION, DEFAULT_REPLICATION);
        this.blockSize = getInt32Property(PROPERTY_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
    }

    private int readPravegaID() {
         int id = getInt32Property(PROPERTY_PRAVEGA_ID, DEFAULT_PRAVEGA_ID);

        //Generate the pravega id from the IP address. This has to be dynamic and can not be default.
        if ( id == DEFAULT_PRAVEGA_ID ) {
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

    /**
     * HDFS host URL. This is generally in host:port format
     *
     * @return URL pointing to the HDFS host.
     */
    public String getHDFSHostURL() {
        return this.hdfsHostURL;
    }

    /**
     * Root of the Pravega owned HDFS path. All the directories/files under this path will be exclusively
     * owned by Pravega.
     *
     * @return path at which all the HDFS data is stored.
     */
    public String getHdfsRoot() {
        return this.hdfsRoot;
    }

    public int getPravegaID() {
        return this.pravegaId;
    }

    /**
     * Decides the replication factor of the data stored on HDFS.
     * This can be used to control availability of HDFS data.
     *
     * @return Replication factor for HDFS storage
     */
    public short getReplication() {
        return this.replication;
    }

    /**
     * The default block size that will be used to exchange data with HDFS.
     *
     * @return block size for HDFS
     */
    public long getBlockSize() {
        return this.blockSize;
    }
}
