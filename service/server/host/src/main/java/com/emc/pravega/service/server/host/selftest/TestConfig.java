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

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.MissingPropertyException;

import java.util.Properties;

/**
 * Configuration for Self-Tester.
 */
public class TestConfig extends ComponentConfig {
    //region Members

    public final static String COMPONENT_CODE = "selftest";
    public static final String PROPERTY_OPERATION_COUNT = "operationCount";
    public static final String PROPERTY_SEGMENT_COUNT = "segmentCount";
    public static final String PROPERTY_TRANSACTION_FREQUENCY = "transactionFrequency";
    public static final String PROPERTY_MAX_TRANSACTION_SIZE = "maxTransactionSize";
    public static final String PROPERTY_PRODUCER_COUNT = "producerCount";

    private static final int DEFAULT_OPERATION_COUNT = 1000 * 1000;
    private static final int DEFAULT_SEGMENT_COUNT = 100;
    private static final int DEFAULT_TRANSACTION_FREQUENCY = 100;
    private static final int DEFAULT_MAX_TRANSACTION_APPEND_COUNT = 10;
    private static final int DEFAULT_PRODUCER_COUNT = 1;

    private int operationCount;
    private int segmentCount;
    private int transactionFrequency;
    private int maxTransactionAppendCount;
    private int producerCount;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws MissingPropertyException Whenever a required Property is missing from the given properties collection.
     * @throws NumberFormatException    Whenever a Property has a value that is invalid for it.
     * @throws NullPointerException     If any of the arguments are null.
     */
    public TestConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.operationCount = getInt32Property(PROPERTY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
        this.segmentCount = getInt32Property(PROPERTY_SEGMENT_COUNT, DEFAULT_SEGMENT_COUNT);
        this.transactionFrequency = getInt32Property(PROPERTY_TRANSACTION_FREQUENCY, DEFAULT_TRANSACTION_FREQUENCY);
        this.maxTransactionAppendCount = getInt32Property(PROPERTY_MAX_TRANSACTION_SIZE, DEFAULT_MAX_TRANSACTION_APPEND_COUNT);
        this.producerCount = getInt32Property(PROPERTY_PRODUCER_COUNT, DEFAULT_PRODUCER_COUNT);
    }

    //endregion

    //region Properties

    public int getOperationCount() {
        return this.operationCount;
    }

    public int getSegmentCount() {
        return this.segmentCount;
    }

    public int getTransactionFrequency() {
        return this.transactionFrequency;
    }

    public int getMaxTransactionAppendCount() {
        return this.maxTransactionAppendCount;
    }

    public int getProducerCount() {
        return this.producerCount;
    }

    //endregion
}
