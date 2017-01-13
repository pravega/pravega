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
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

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
        this.pravegaId = getInt32Property(PROPERTY_PRAVEGA_ID, DEFAULT_PRAVEGA_ID);
        //Generate the pravega id from the IP address. This has to be dynamic and can not be default.
        if ( this.pravegaId == -1 ) {
            try {
                this.pravegaId = Math.abs(InetAddress.getLocalHost().getHostName().hashCode());
                } catch (UnknownHostException e) {
                this.pravegaId = 0;
                log.warn("Exception {} while getting unique Pravega ID for this host. Using {} as default value",
                        e.getMessage(), this.pravegaId);
            }
        }
        this.replication = (short) getInt32Property(PROPERTY_REPLICATION, DEFAULT_REPLICATION);
        this.blockSize = getInt32Property(PROPERTY_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
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
