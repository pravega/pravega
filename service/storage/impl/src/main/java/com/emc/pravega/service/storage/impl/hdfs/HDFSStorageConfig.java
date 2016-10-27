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

import java.util.Properties;

public class HDFSStorageConfig extends ComponentConfig {

    public static final String COMPONENT_CODE = "hdfs";
    private static final String PROPERTY_HDFSURL = "fs.default.name";
    private static final String PROPERTY_HDFSROOT = "hdfsroot";
    private static final String PROPERTY_PRAVEGAID = "pravegaid";
    private static final String PROPERTY_REPLICATION = "replication";
    private static final String PROPERTY_BLOCKSIZE = "blocksize";
    private String hdfsHostURL;
    private String hdfsRoot;
    private int pravegaID;
    private short replication;
    private long blocksize;

    public HDFSStorageConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    @Override
    protected void refresh() throws ConfigurationException {
        this.hdfsHostURL = getProperty(PROPERTY_HDFSURL);
        this.hdfsRoot    = getProperty(PROPERTY_HDFSROOT);
        this.pravegaID   = getInt32Property(PROPERTY_PRAVEGAID);
        this.replication = (short) getInt32Property(PROPERTY_REPLICATION);
        this.blocksize   = getInt32Property(PROPERTY_BLOCKSIZE);
    }

    /**
     * @return URL pointing to the HDFS host.
     */
    public String getHDFSHostURL() {
        return this.hdfsHostURL;
    }

    /**
     * @return path at which all the HDFS data is stored.
     */
    public String getHdfsRoot() {
        return this.hdfsRoot;
    }

    public int getPravegaID() {
        return this.pravegaID;
    }

    public short getReplication() {
        return this.replication;
    }

    public long getBlockSize() {
        return this.blocksize;
    }
}
