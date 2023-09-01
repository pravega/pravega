/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.contracts.tables.TableStore;
import java.net.InetAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Configuration for Self-Tester.
 * See https://github.com/pravega/pravega/wiki/Local-Stress-Testing#arguments for detailed explanation.
 */
public class TestConfig {
    //region Config Names

    public static final String DEFAULT_CONFIG_FILE_NAME = "selftest.config.properties";
    public static final String BK_ZK_LEDGER_PATH = "/pravega/selftest/bookkeeper/ledgers";
    public static final String LOCALHOST = InetAddress.getLoopbackAddress().getHostName();
    static final String TMP_DIR = System.getProperty("java.io.tmp", "/tmp");
    static final Property<Integer> OPERATION_COUNT = Property.named("operationCount", 100 * 1000);
    static final Property<Integer> CONTAINER_COUNT = Property.named("containerCount", 1);
    static final Property<Integer> STREAM_COUNT = Property.named("streamCount", 100);
    static final Property<Integer> SEGMENTS_PER_STREAM = Property.named("segmentsPerStream", 1);
    static final Property<Integer> TRANSACTION_FREQUENCY = Property.named("transactionFrequency", Integer.MAX_VALUE);
    static final Property<Integer> MAX_TRANSACTION_SIZE = Property.named("maxTransactionSize", 20);
    static final Property<Integer> PRODUCER_COUNT = Property.named("producerCount", 1);
    static final Property<Integer> PRODUCER_PARALLELISM = Property.named("producerParallelism", 1);
    static final Property<Integer> CLIENT_WRITERS_PER_STREAM = Property.named("writersPerStream", -1);
    static final Property<Integer> MIN_APPEND_SIZE = Property.named("minAppendSize", 100);
    static final Property<Integer> MAX_APPEND_SIZE = Property.named("maxAppendSize", 100);
    static final Property<Boolean> TABLE_CONDITIONAL_UPDATES = Property.named("tableConditionalUpdates", false);
    static final Property<Integer> TABLE_REMOVE_PERCENTAGE = Property.named("tableRemovePercentage", 10); // 0..100
    static final Property<Integer> TABLE_NEW_KEY_PERCENTAGE = Property.named("tableNewKeyPercentage", 30); // 0..100
    static final Property<Integer> TABLE_CONSUMERS_PER_TABLE = Property.named("consumersPerTable", 10);
    static final Property<Integer> TABLE_KEY_LENGTH = Property.named("tableKeyLength", MIN_APPEND_SIZE.getDefaultValue());
    static final Property<String> TABLE_TYPE = Property.named("tableType", TableType.Hash.toString());
    static final Property<Integer> THREAD_POOL_SIZE = Property.named("threadPoolSize", 80);
    static final Property<Integer> TIMEOUT_MILLIS = Property.named("timeoutMillis", 3000);
    static final Property<String> TEST_TYPE = Property.named("testType", TestType.SegmentStore.toString());
    static final Property<Integer> WARMUP_PERCENTAGE = Property.named("warmupPercentage", 10);
    static final Property<Boolean> READS_ENABLED = Property.named("reads", true);
    static final Property<Boolean> METRICS_ENABLED = Property.named("metrics", false);
    static final Property<Integer> BOOKIE_COUNT = Property.named("bookieCount", 1);
    static final Property<Integer> CONTROLLER_COUNT = Property.named("controllerCount", 1);
    static final Property<Integer> SEGMENT_STORE_COUNT = Property.named("segmentStoreCount", 1);
    static final Property<String> CONTROLLER_HOST = Property.named("controllerHost", LOCALHOST);
    static final Property<Integer> CONTROLLER_BASE_PORT = Property.named("controllerPort", 9200);
    static final Property<Boolean> PAUSE_BEFORE_EXIT = Property.named("pauseBeforeExit", false);
    static final Property<String> BOOKIE_LEDGERS_DIR = Property.named("bkLedgersDir", "");
    static final Property<String> STORAGE_DIR = Property.named("storageDir", TMP_DIR + "/pravega/storage");
    static final Property<Boolean> CHUNKED_SEGMENT_STORAGE_ENABLED = Property.named("useChunkedSegmentStorage", true);
    private static final Property<Integer> ZK_PORT = Property.named("zkPort", 9000);
    private static final Property<Integer> BK_BASE_PORT = Property.named("bkBasePort", 9100);
    private static final Property<Integer> SEGMENT_STORE_BASE_PORT = Property.named("segmentStorePort", 9300);
    private static final Property<Boolean> ENABLE_SECURITY = Property.named("enableSecurity", false);
    private static final String TEST_OUTPUT_PATH = TMP_DIR + "/pravega";
    private static final String LOG_PATH_FORMAT = TEST_OUTPUT_PATH + "/selftest.%s.log";
    private static final String METRICS_PATH_FORMAT = TEST_OUTPUT_PATH + "/selftest.metrics.%s";
    private static final String COMPONENT_CODE = "selftest";
    public static final String CONFIG_FILE_PROPERTY_NAME = COMPONENT_CODE + ".configFile";

    //endregion

    //region Members

    @Getter
    private final int operationCount;
    @Getter
    private final int warmupCount;
    @Getter
    private final int containerCount;
    @Getter
    private final int streamCount;
    @Getter
    private final int segmentsPerStream;
    @Getter
    private final int transactionFrequency;
    @Getter
    private final int maxTransactionAppendCount;
    @Getter
    private final int producerCount;
    @Getter
    private final int producerParallelism;
    @Getter
    private final int clientWritersPerStream;
    @Getter
    private final int minAppendSize;
    @Getter
    private final int maxAppendSize;
    @Getter
    private final boolean tableConditionalUpdates;
    @Getter
    private final int tableRemovePercentage;
    @Getter
    private final int tableNewKeyPercentage;
    @Getter
    private final int tableKeyLength;
    @Getter
    private final TableType tableType;
    @Getter
    private final int consumersPerTable;
    @Getter
    private final int threadPoolSize;
    @Getter
    private final Duration timeout;
    @Getter
    private final int bookieCount;
    @Getter
    private final int controllerCount;
    @Getter
    private final int segmentStoreCount;
    private final int bkBasePort;
    @Getter
    private final int zkPort;
    @Getter
    private final String controllerHost;
    private final int controllerBasePort;
    private final int segmentStoreBasePort;
    @Getter
    private final TestType testType;
    @Getter
    private final boolean readsEnabled;
    @Getter
    private final boolean metricsEnabled;
    @Getter
    private final boolean pauseBeforeExit;
    @Getter
    private final boolean enableSecurity;
    @Getter
    private final String bookieLedgersDir;
    @Getter
    private final String storageDir;
    @Getter
    private final String testId = Long.toHexString(System.currentTimeMillis());
    @Getter
    private final boolean chunkedSegmentStorageEnabled;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private TestConfig(TypedProperties properties) throws ConfigurationException {
        this.operationCount = properties.getInt(OPERATION_COUNT);
        this.warmupCount = (int) (this.operationCount * (properties.getInt(WARMUP_PERCENTAGE) / 100.0));
        this.containerCount = properties.getInt(CONTAINER_COUNT);
        this.streamCount = properties.getInt(STREAM_COUNT);
        this.segmentsPerStream = properties.getInt(SEGMENTS_PER_STREAM);
        this.transactionFrequency = properties.getInt(TRANSACTION_FREQUENCY);
        this.maxTransactionAppendCount = properties.getInt(MAX_TRANSACTION_SIZE);
        this.producerCount = properties.getInt(PRODUCER_COUNT);
        this.producerParallelism = properties.getInt(PRODUCER_PARALLELISM);
        this.clientWritersPerStream = properties.getInt(CLIENT_WRITERS_PER_STREAM);
        this.minAppendSize = properties.getInt(MIN_APPEND_SIZE);
        this.maxAppendSize = properties.getInt(MAX_APPEND_SIZE);
        if (this.minAppendSize < Event.HEADER_LENGTH) {
            throw new ConfigurationException(String.format("Property '%s' (%s) must be at least %s.",
                    MIN_APPEND_SIZE, this.minAppendSize, Event.HEADER_LENGTH));
        }
        if (this.minAppendSize > this.maxAppendSize) {
            throw new ConfigurationException(String.format("Property '%s' (%s) must be smaller than '%s' (%s).",
                    MIN_APPEND_SIZE, this.minAppendSize, MAX_APPEND_SIZE, this.maxAppendSize));
        }
        this.tableConditionalUpdates = properties.getBoolean(TABLE_CONDITIONAL_UPDATES);
        this.tableRemovePercentage = properties.getInt(TABLE_REMOVE_PERCENTAGE);
        if (this.tableRemovePercentage < 0 || this.tableRemovePercentage > 100) {
            throw new ConfigurationException(String.format("Property '%s' must be a value between 0 and 100. Given %s.",
                    TABLE_REMOVE_PERCENTAGE, this.tableRemovePercentage));
        }
        this.tableNewKeyPercentage = properties.getInt(TABLE_NEW_KEY_PERCENTAGE);
        if (this.tableNewKeyPercentage < 0 || this.tableNewKeyPercentage > 100) {
            throw new ConfigurationException(String.format("Property '%s' must be a value between 0 and 100. Given %s.",
                    TABLE_NEW_KEY_PERCENTAGE, this.tableNewKeyPercentage));
        }
        this.tableKeyLength = properties.getInt(TABLE_KEY_LENGTH);
        if (this.tableKeyLength <= 0 || this.tableKeyLength > TableStore.MAXIMUM_KEY_LENGTH) {
            throw new ConfigurationException(String.format("Property '%s' must be a value between 0 and %s. Given %s.",
                    TABLE_KEY_LENGTH, TableStore.MAXIMUM_KEY_LENGTH, this.tableNewKeyPercentage));
        }
        this.tableType = TableType.valueOf(properties.get(TABLE_TYPE));
        this.consumersPerTable = properties.getInt(TABLE_CONSUMERS_PER_TABLE);
        this.threadPoolSize = properties.getInt(THREAD_POOL_SIZE);
        this.timeout = Duration.ofMillis(properties.getInt(TIMEOUT_MILLIS));
        this.bookieCount = properties.getInt(BOOKIE_COUNT);
        this.controllerCount = properties.getInt(CONTROLLER_COUNT);
        this.segmentStoreCount = properties.getInt(SEGMENT_STORE_COUNT);
        this.bkBasePort = properties.getInt(BK_BASE_PORT);
        this.zkPort = properties.getInt(ZK_PORT);
        this.controllerHost = properties.get(CONTROLLER_HOST);
        this.controllerBasePort = properties.getInt(CONTROLLER_BASE_PORT);
        this.segmentStoreBasePort = properties.getInt(SEGMENT_STORE_BASE_PORT);
        this.testType = TestType.valueOf(properties.get(TEST_TYPE));
        this.readsEnabled = properties.getBoolean(READS_ENABLED);
        this.metricsEnabled = properties.getBoolean(METRICS_ENABLED);
        this.pauseBeforeExit = properties.getBoolean(PAUSE_BEFORE_EXIT);
        this.enableSecurity = properties.getBoolean(ENABLE_SECURITY);
        this.bookieLedgersDir = properties.get(BOOKIE_LEDGERS_DIR);
        this.storageDir = properties.get(STORAGE_DIR);
        this.chunkedSegmentStorageEnabled = properties.getBoolean(CHUNKED_SEGMENT_STORAGE_ENABLED);
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
     * Gets the path to a CSV file which can be used for writing Metrics for a specified component.
     *
     * @param componentName The name of the component.
     * @param id            The Id of the component.
     * @return The path.
     */
    public String getComponentMetricsPath(String componentName, int id) {
        return String.format(METRICS_PATH_FORMAT, String.format("%s_%d", componentName, id));
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
        return this.bkBasePort + bookieId;
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

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public enum TestType {
        SegmentStore(false),
        SegmentStoreTable(true),
        InProcessMock(false),
        InProcessMockTable(true),
        InProcessStore(false),
        InProcessStoreTable(true),
        AppendProcessor(false),
        OutOfProcess(false),
        External(false),
        BookKeeper(false);
        @Getter
        private final boolean tablesTest;
    }

    public enum TableType {
        Hash,
    }
}
