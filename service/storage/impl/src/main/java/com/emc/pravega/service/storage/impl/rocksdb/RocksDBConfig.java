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

package com.emc.pravega.service.storage.impl.rocksdb;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;

import java.util.Properties;

/**
 * Configuration for RocksDB-backed cache.
 */
public class RocksDBConfig extends ComponentConfig {
    //region Members

    public static final String COMPONENT_CODE = "rocksdb";
    public static final String PROPERTY_DATABASE_DIR = "dbDir";

    private static final String DEFAULT_DATABASE_DIR = "/tmp/pravega/cache";

    private String databaseDir;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RocksDBConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given
     *                                  properties collection), NumberFormatException (a Property has a value that is
     *                                  invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public RocksDBConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the path to the RocksDB database (in the local filesystem).
     */
    public String getDatabaseDir() {
        return this.databaseDir;
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.databaseDir = getProperty(PROPERTY_DATABASE_DIR, DEFAULT_DATABASE_DIR);
    }

    //endregion
}
