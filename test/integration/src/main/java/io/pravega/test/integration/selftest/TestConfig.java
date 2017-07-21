/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.time.Duration;
import lombok.Getter;

/**
 * Configuration for Self-Tester.
 */
public class TestConfig {
    //region Config Names

    static final Property<Integer> OPERATION_COUNT = Property.named("operationCount", 100 * 1000);
    static final Property<Integer> SEGMENT_COUNT = Property.named("segmentCount", 100);
    static final Property<Integer> TRANSACTION_FREQUENCY = Property.named("transactionFrequency", 100);
    static final Property<Integer> MAX_TRANSACTION_SIZE = Property.named("maxTransactionSize", 10);
    static final Property<Integer> PRODUCER_COUNT = Property.named("producerCount", 1);
    static final Property<Integer> MIN_APPEND_SIZE = Property.named("minAppendSize", 100);
    static final Property<Integer> MAX_APPEND_SIZE = Property.named("maxAppendSize", 100);
    static final Property<Integer> THREAD_POOL_SIZE = Property.named("threadPoolSize", 100);
    static final Property<Integer> TIMEOUT_MILLIS = Property.named("timeoutMillis", 10 * 1000);
    static final Property<Integer> DATA_LOG_APPEND_DELAY = Property.named("dataLogAppendDelayMillis", 0);
    static final Property<Boolean> USE_BOOKKEEPER = Property.named("useBk", false);
    static final Property<Integer> ZK_PORT = Property.named("zkPort", 9001);
    static final Property<Integer> BK_PORT = Property.named("bkPort", 9002);
    static final Property<String> TEST_TYPE = Property.named("testType", TestType.SegmentStoreDirect.toString());
    static final Property<Integer> CLIENT_PORT = Property.named("clientPort", 9876);
    private static final String COMPONENT_CODE = "selftest";

    //endregion

    //region Members

    @Getter
    private int operationCount;
    @Getter
    private int segmentCount;
    @Getter
    private int transactionFrequency;
    @Getter
    private int maxTransactionAppendCount;
    @Getter
    private int producerCount;
    @Getter
    private int minAppendSize;
    @Getter
    private int maxAppendSize;
    @Getter
    private int threadPoolSize;
    @Getter
    private Duration timeout;
    @Getter
    private Duration dataLogAppendDelay;
    @Getter
    private boolean useBookKeeper;
    @Getter
    private int bkPort;
    @Getter
    private int zkPort;
    @Getter
    private TestType testType;
    @Getter
    private int clientPort;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private TestConfig(TypedProperties properties) throws ConfigurationException {
        this.operationCount = properties.getInt(OPERATION_COUNT);
        this.segmentCount = properties.getInt(SEGMENT_COUNT);
        this.transactionFrequency = properties.getInt(TRANSACTION_FREQUENCY);
        this.maxTransactionAppendCount = properties.getInt(MAX_TRANSACTION_SIZE);
        this.producerCount = properties.getInt(PRODUCER_COUNT);
        this.minAppendSize = properties.getInt(MIN_APPEND_SIZE);
        this.maxAppendSize = properties.getInt(MAX_APPEND_SIZE);
        this.threadPoolSize = properties.getInt(THREAD_POOL_SIZE);
        this.timeout = Duration.ofMillis(properties.getInt(TIMEOUT_MILLIS));
        this.dataLogAppendDelay = Duration.ofMillis(properties.getInt(DATA_LOG_APPEND_DELAY));
        this.useBookKeeper = properties.getBoolean(USE_BOOKKEEPER);
        this.bkPort = properties.getInt(BK_PORT);
        this.zkPort = properties.getInt(ZK_PORT);
        this.testType = TestType.valueOf(properties.get(TEST_TYPE));
        this.clientPort = properties.getInt(CLIENT_PORT);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    static ConfigBuilder<TestConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, TestConfig::new);
    }

    //endregion

    enum TestType {
        SegmentStoreDirect,
        InProcessClient,
        OutOfProcessClient
    }
}
