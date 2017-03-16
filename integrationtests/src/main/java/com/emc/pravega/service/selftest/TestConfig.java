/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.selftest;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
import com.emc.pravega.common.util.TypedProperties;
import java.time.Duration;
import lombok.Getter;

/**
 * Configuration for Self-Tester.
 */
class TestConfig {
    //region Config Names

    static final Property<Integer> OPERATION_COUNT = new Property<>("operationCount", 100 * 1000);
    static final Property<Integer> SEGMENT_COUNT = new Property<>("segmentCount", 100);
    static final Property<Integer> TRANSACTION_FREQUENCY = new Property<>("transactionFrequency", 100);
    static final Property<Integer> MAX_TRANSACTION_SIZE = new Property<>("maxTransactionSize", 10);
    static final Property<Integer> PRODUCER_COUNT = new Property<>("producerCount", 1);
    static final Property<Integer> MIN_APPEND_SIZE = new Property<>("minAppendSize", 100);
    static final Property<Integer> MAX_APPEND_SIZE = new Property<>("maxAppendSize", 100);
    static final Property<Integer> THREAD_POOL_SIZE = new Property<>("threadPoolSize", 100);
    static final Property<Integer> TIMEOUT_MILLIS = new Property<>("timeoutMillis", 10 * 1000);
    static final Property<Boolean> VERBOSE_LOGGING = new Property<>("verboseLogging", false);
    static final Property<Integer> DATA_LOG_APPEND_DELAY = new Property<>("dataLogAppendDelayMillis", 0);
    static final Property<Boolean> USE_CLIENT = new Property<>("useClient", false);
    static final Property<Integer> CLIENT_PORT = new Property<>("clientPort", 9876);
    static final Property<Boolean> CLIENT_AUTO_FLUSH = new Property<>("clientAutoFlush", true);
    static final Property<Integer> CLIENT_WRITER_COUNT = new Property<>("clientWriterCount", 1);
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
    private boolean verboseLoggingEnabled;
    @Getter
    private Duration dataLogAppendDelay;
    @Getter
    private boolean useClient;
    @Getter
    private int clientPort;
    @Getter
    private boolean clientAutoFlush;
    @Getter
    private int clientWriterCount;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private TestConfig(TypedProperties properties) throws ConfigurationException {
        this.operationCount = properties.getInt32(OPERATION_COUNT);
        this.segmentCount = properties.getInt32(SEGMENT_COUNT);
        this.transactionFrequency = properties.getInt32(TRANSACTION_FREQUENCY);
        this.maxTransactionAppendCount = properties.getInt32(MAX_TRANSACTION_SIZE);
        this.producerCount = properties.getInt32(PRODUCER_COUNT);
        this.minAppendSize = properties.getInt32(MIN_APPEND_SIZE);
        this.maxAppendSize = properties.getInt32(MAX_APPEND_SIZE);
        this.threadPoolSize = properties.getInt32(THREAD_POOL_SIZE);
        this.timeout = Duration.ofMillis(properties.getInt32(TIMEOUT_MILLIS));
        this.verboseLoggingEnabled = properties.getBoolean(VERBOSE_LOGGING);
        this.dataLogAppendDelay = Duration.ofMillis(properties.getInt32(DATA_LOG_APPEND_DELAY));
        this.useClient = properties.getBoolean(USE_CLIENT);
        this.clientPort = properties.getInt32(CLIENT_PORT);
        this.clientAutoFlush = properties.getBoolean(CLIENT_AUTO_FLUSH);
        this.clientWriterCount = properties.getInt32(CLIENT_WRITER_COUNT);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<TestConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, TestConfig::new);
    }

    //endregion
}
