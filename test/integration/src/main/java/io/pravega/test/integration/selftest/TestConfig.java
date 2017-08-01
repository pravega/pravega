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

import com.google.common.base.Preconditions;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.val;

/**
 * Configuration for Self-Tester.
 */
public class TestConfig {
    //region Config Names

    public static final String BK_LEDGER_PATH = "/pravega/selftest/bookkeeper/ledgers";
    static final Property<Integer> OPERATION_COUNT = Property.named("operationCount", 100 * 1000);
    static final Property<Integer> CONTAINER_COUNT = Property.named("containerCount", 1);
    static final Property<Integer> STREAM_COUNT = Property.named("streamCount", 100);
    static final Property<Integer> TRANSACTION_FREQUENCY = Property.named("transactionFrequency", 100);
    static final Property<Integer> MAX_TRANSACTION_SIZE = Property.named("maxTransactionSize", 10);
    static final Property<Integer> PRODUCER_COUNT = Property.named("producerCount", 1);
    static final Property<Integer> PRODUCER_PARALLELISM = Property.named("producerParallelism", 1);
    static final Property<Integer> MIN_APPEND_SIZE = Property.named("minAppendSize", 100);
    static final Property<Integer> MAX_APPEND_SIZE = Property.named("maxAppendSize", 100);
    static final Property<Integer> THREAD_POOL_SIZE = Property.named("threadPoolSize", 100);
    static final Property<Integer> TIMEOUT_MILLIS = Property.named("timeoutMillis", 10 * 1000);
    static final Property<String> TEST_TYPE = Property.named("testType", TestType.SegmentStoreDirect.toString());
    static final Property<Integer> BOOKIE_COUNT = Property.named("bookieCount", 1);
    static final Property<Integer> CONTROLLER_COUNT = Property.named("controllerCount", 1);
    static final Property<Integer> SEGMENT_STORE_COUNT = Property.named("segmentStoreCount", 1);
    private static final Property<Integer> ZK_PORT = Property.named("zkPort", 9000);
    private static final Property<Integer> BK_PORT = Property.named("bkPort", 9100);
    private static final Property<Integer> CONTROLLER_BASE_PORT = Property.named("controllerPort", 9200);
    private static final Property<Integer> SEGMENT_STORE_BASE_PORT = Property.named("segmentStorePort", 9300);
    private static final String COMPONENT_CODE = "selftest";
    private static final String LOG_PATH_FORMAT = "/tmp/pravega/selftest.%s.log";

    //endregion

    //region Members

    @Getter
    private int operationCount;
    @Getter
    private int containerCount;
    @Getter
    private int streamCount;
    @Getter
    private int transactionFrequency;
    @Getter
    private int maxTransactionAppendCount;
    @Getter
    private int producerCount;
    @Getter
    private int producerParallelism;
    @Getter
    private int minAppendSize;
    @Getter
    private int maxAppendSize;
    @Getter
    private int threadPoolSize;
    @Getter
    private Duration timeout;
    @Getter
    private int bookieCount;
    @Getter
    private int controllerCount;
    @Getter
    private int segmentStoreCount;
    @Getter
    private int bkPort;
    @Getter
    private int zkPort;
    @Getter
    private int controllerBasePort;
    @Getter
    private int segmentStoreBasePort;
    @Getter
    private TestType testType;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private TestConfig(TypedProperties properties) throws ConfigurationException {
        this.operationCount = properties.getInt(OPERATION_COUNT);
        this.containerCount = properties.getInt(CONTAINER_COUNT);
        this.streamCount = properties.getInt(STREAM_COUNT);
        this.transactionFrequency = properties.getInt(TRANSACTION_FREQUENCY);
        this.maxTransactionAppendCount = properties.getInt(MAX_TRANSACTION_SIZE);
        this.producerCount = properties.getInt(PRODUCER_COUNT);
        this.producerParallelism = properties.getInt(PRODUCER_PARALLELISM);
        this.minAppendSize = properties.getInt(MIN_APPEND_SIZE);
        this.maxAppendSize = properties.getInt(MAX_APPEND_SIZE);
        this.threadPoolSize = properties.getInt(THREAD_POOL_SIZE);
        this.timeout = Duration.ofMillis(properties.getInt(TIMEOUT_MILLIS));
        this.bookieCount = properties.getInt(BOOKIE_COUNT);
        this.controllerCount = properties.getInt(CONTROLLER_COUNT);
        this.segmentStoreCount = properties.getInt(SEGMENT_STORE_COUNT);
        this.bkPort = properties.getInt(BK_PORT);
        this.zkPort = properties.getInt(ZK_PORT);
        this.controllerBasePort = properties.getInt(CONTROLLER_BASE_PORT);
        this.segmentStoreBasePort = properties.getInt(SEGMENT_STORE_BASE_PORT);
        this.testType = TestType.valueOf(properties.get(TEST_TYPE));
        checkOverlappingPorts();
    }

    //endregion

    //region Properties

    /**
     * Gets the path to a log file which can be used for redirecting Standard Out for a particular component.
     *
     * @param componentName The name of the component.
     * @param id            The Id of the component.
     * @return The path.
     */
    public String getComponentOutLogPath(String componentName, int id) {
        return getLogPath(String.format("%s_%d.out", componentName, id));
    }

    /**
     * Gets the path to a log file which can be used for redirecting Standard Err for a particular component.
     *
     * @param componentName The name of the component.
     * @param id            The Id of the component.
     * @return The path.
     */
    public String getComponentErrLogPath(String componentName, int id) {
        return getLogPath(String.format("%s_%d.err", componentName, id));
    }

    /**
     * Gets the path to the Tester Log file.
     *
     * @return The path.
     */
    public String getTestLogPath() {
        return getLogPath("test");
    }

    private String getLogPath(String componentName) {
        return String.format(LOG_PATH_FORMAT, componentName);
    }

    /**
     * Gets the BookKeeper port for the given BookieId.
     *
     * @param bookieId The indexed Id of the Bookie.
     * @return The port.
     */
    public int getBkPort(int bookieId) {
        Preconditions.checkElementIndex(bookieId, this.bookieCount, "bookieId must be less than bookieCount.");
        return this.bkPort + bookieId;
    }

    /**
     * Gets the Controller Listening port for the given Controller Id.
     *
     * @param controllerId The indexed Id of the Controller.
     * @return The port.
     */
    public int getControllerPort(int controllerId) {
        Preconditions.checkElementIndex(controllerId, this.controllerCount, "controllerId must be less than controllerCount.");
        return this.controllerBasePort + controllerId * 10;
    }

    /**
     * Gets the Controller REST port for the given Controller Id.
     *
     * @param controllerId The indexed Id of the Controller.
     * @return The port.
     */
    public int getControllerRestPort(int controllerId) {
        return getControllerPort(controllerId) + 1;
    }

    /**
     * Gets the Controller RPC port for the given Controller Id.
     *
     * @param controllerId The indexed Id of the Controller.
     * @return The port.
     */
    public int getControllerRpcPort(int controllerId) {
        return getControllerPort(controllerId) + 2;
    }

    /**
     * Gets the SegmentStore Service Listening port for the given SegmentStore Id.
     *
     * @param segmentStoreId The indexed Id of the SegmentStore.
     * @return The port.
     */
    public int getSegmentStorePort(int segmentStoreId) {
        Preconditions.checkElementIndex(segmentStoreId, this.segmentStoreCount, "segmentStoreId must be less than segmentStoreCount.");
        return this.segmentStoreBasePort + segmentStoreId;
    }

    private void checkOverlappingPorts() {
        val ports = new HashSet<Integer>();
        checkExistingPort(this.zkPort, ports);

        for (int i = 0; i < this.bookieCount; i++) {
            checkExistingPort(getBkPort(i), ports);
        }

        for (int i = 0; i < this.controllerCount; i++) {
            checkExistingPort(getControllerPort(i), ports);
            checkExistingPort(getControllerRestPort(i), ports);
            checkExistingPort(getControllerRpcPort(i), ports);
        }

        for (int i = 0; i < this.segmentStoreCount; i++) {
            checkExistingPort(getSegmentStorePort(i), ports);
        }
    }

    private void checkExistingPort(int newPort, Set<Integer> ports) {
        if (!ports.add(newPort)) {
            throw new ConfigurationException(String.format("Duplicate port found: %d.", newPort));
        }
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

    public enum TestType {
        SegmentStoreDirect,
        InProcessMockListener,
        InProcessStoreListener,
        OutOfProcessClient
    }
}
