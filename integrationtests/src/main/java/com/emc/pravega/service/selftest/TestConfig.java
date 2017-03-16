/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.selftest;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.TypedProperties;
import java.time.Duration;
import lombok.Getter;

/**
 * Configuration for Self-Tester.
 */
class TestConfig {
    //region Config Names

    static final String PROPERTY_OPERATION_COUNT = "operationCount";
    static final String PROPERTY_SEGMENT_COUNT = "segmentCount";
    static final String PROPERTY_TRANSACTION_FREQUENCY = "transactionFrequency";
    static final String PROPERTY_MAX_TRANSACTION_SIZE = "maxTransactionSize";
    static final String PROPERTY_PRODUCER_COUNT = "producerCount";
    static final String PROPERTY_MIN_APPEND_SIZE = "minAppendSize";
    static final String PROPERTY_MAX_APPEND_SIZE = "maxAppendSize";
    static final String PROPERTY_THREAD_POOL_SIZE = "threadPoolSize";
    static final String PROPERTY_TIMEOUT_MILLIS = "timeoutMillis";
    static final String PROPERTY_VERBOSE_LOGGING = "verboseLogging";
    static final String PROPERTY_DATA_LOG_APPEND_DELAY = "dataLogAppendDelayMillis";
    static final String PROPERTY_USE_CLIENT = "useClient";
    static final String PROPERTY_CLIENT_PORT = "clientPort";
    static final String PROPERTY_CLIENT_AUTO_FLUSH = "clientAutoFlush";
    static final String PROPERTY_CLIENT_WRITER_COUNT = "clientWriterCount"; // Per segment.
    private static final String COMPONENT_CODE = "selftest";

    private static final int DEFAULT_OPERATION_COUNT = 1000 * 1000;
    private static final int DEFAULT_SEGMENT_COUNT = 100;
    private static final int DEFAULT_TRANSACTION_FREQUENCY = 100;
    private static final int DEFAULT_MAX_TRANSACTION_APPEND_COUNT = 10;
    private static final int DEFAULT_PRODUCER_COUNT = 1;
    private static final int DEFAULT_MIN_APPEND_SIZE = 100;
    private static final int DEFAULT_MAX_APPEND_SIZE = 100;
    private static final int DEFAULT_THREAD_POOL_SIZE = 100;
    private static final int DEFAULT_TIMEOUT_MILLIS = 10 * 1000;
    private static final boolean DEFAULT_VERBOSE_LOGGING = false;
    private static final int DEFAULT_DATA_LOG_APPEND_DELAY = 0;
    private static final boolean DEFAULT_USE_CLIENT = false;
    private static final int DEFAULT_CLIENT_PORT = 9876;
    private static final boolean DEFAULT_CLIENT_AUTO_FLUSH = true;
    private static final int DEFAULT_CLIENT_WRITER_COUNT = 1;

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
        this.operationCount = properties.getInt32(PROPERTY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
        this.segmentCount = properties.getInt32(PROPERTY_SEGMENT_COUNT, DEFAULT_SEGMENT_COUNT);
        this.transactionFrequency = properties.getInt32(PROPERTY_TRANSACTION_FREQUENCY, DEFAULT_TRANSACTION_FREQUENCY);
        this.maxTransactionAppendCount = properties.getInt32(PROPERTY_MAX_TRANSACTION_SIZE, DEFAULT_MAX_TRANSACTION_APPEND_COUNT);
        this.producerCount = properties.getInt32(PROPERTY_PRODUCER_COUNT, DEFAULT_PRODUCER_COUNT);
        this.minAppendSize = properties.getInt32(PROPERTY_MIN_APPEND_SIZE, DEFAULT_MIN_APPEND_SIZE);
        this.maxAppendSize = properties.getInt32(PROPERTY_MAX_APPEND_SIZE, DEFAULT_MAX_APPEND_SIZE);
        this.threadPoolSize = properties.getInt32(PROPERTY_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
        int timeoutMillis = properties.getInt32(PROPERTY_TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS);
        this.timeout = Duration.ofMillis(timeoutMillis);
        this.verboseLoggingEnabled = properties.getBoolean(PROPERTY_VERBOSE_LOGGING, DEFAULT_VERBOSE_LOGGING);
        int appendDelayMillis = properties.getInt32(PROPERTY_DATA_LOG_APPEND_DELAY, DEFAULT_DATA_LOG_APPEND_DELAY);
        this.dataLogAppendDelay = Duration.ofMillis(appendDelayMillis);
        this.useClient = properties.getBoolean(PROPERTY_USE_CLIENT, DEFAULT_USE_CLIENT);
        this.clientPort = properties.getInt32(PROPERTY_CLIENT_PORT, DEFAULT_CLIENT_PORT);
        this.clientAutoFlush = properties.getBoolean(PROPERTY_CLIENT_AUTO_FLUSH, DEFAULT_CLIENT_AUTO_FLUSH);
        this.clientWriterCount = properties.getInt32(PROPERTY_CLIENT_WRITER_COUNT, DEFAULT_CLIENT_WRITER_COUNT);
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
