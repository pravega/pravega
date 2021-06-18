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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.shared.metrics.MetricsConfig;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for Self Tester.
 * See https://github.com/pravega/pravega/wiki/Local-Stress-Testing for more details.
 */
public class SelfTestRunner {
    private static final long STARTUP_TIMEOUT_MILLIS = 60 * 1000;

    public static void main(String[] args) throws Exception {
        Properties shortcuts = Shortcuts.extract(System.getProperties());
        if (shortcuts.size() == 0 && !System.getProperties().containsKey(TestConfig.CONFIG_FILE_PROPERTY_NAME)) {
            printUsage();
            return;
        }

        // Load config & setup logging.
        ServiceBuilderConfig builderConfig = getConfig(shortcuts);
        TestConfig testConfig = builderConfig.getConfig(TestConfig::builder);
        setupLogging(testConfig);

        // Create a new SelfTest.
        @Cleanup
        SelfTest test = new SelfTest(testConfig, builderConfig);

        // Start the test.
        test.startAsync().awaitRunning(STARTUP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Wait for the test to finish.
        test.awaitFinished().join();
        if (testConfig.isPauseBeforeExit()) {
            System.out.println("Services are still running. Press any key to exit ...");
            System.in.read();
        }

        // Make sure the test is stopped.
        test.stopAsync().awaitTerminated();
    }

    /**
     * Gets a ServiceBuilderConfig containing test-related and SegmentStore-related configuration, using the following
     * priority order (low to high):
     * 1. Hardcoded defaults.
     * 2. Config file.
     * 3. System Properties.
     * 4. Explicit overrides (as passed in via an argument).
     *
     * @param overrides Explicit overrides.
     */
    private static ServiceBuilderConfig getConfig(Properties overrides) throws IOException {
        // 1. Hardcoded defaults.
        ServiceBuilderConfig.Builder b = getDefaultServiceBuilderConfig();

        // 2. File-based config (overriding defaults).
        File configFile = new File(System.getProperty(TestConfig.CONFIG_FILE_PROPERTY_NAME, TestConfig.DEFAULT_CONFIG_FILE_NAME));
        if (configFile.exists()) {
            b.include(System.getProperty(TestConfig.CONFIG_FILE_PROPERTY_NAME, TestConfig.DEFAULT_CONFIG_FILE_NAME));
        }

        // 3. System Property-based config (overriding defaults and File-based config).
        b.include(System.getProperties());

        // 4. Apply explicit overrides.
        b.include(overrides);

        // 5. Cross-apply common configuration that must be the same on all fronts.
        val testConfig = b.build().getConfig(TestConfig::builder);
        int bkWriteQuorum = Math.min(3, testConfig.getBookieCount());
        b.include(ServiceConfig.builder()
                               .with(ServiceConfig.CONTAINER_COUNT, testConfig.getContainerCount()));
        b.include(BookKeeperConfig.builder()
                                  .with(BookKeeperConfig.ZK_ADDRESS, TestConfig.LOCALHOST + ":" + testConfig.getZkPort())
                                  .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, bkWriteQuorum)
                                  .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, bkWriteQuorum)
                                  .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, bkWriteQuorum));
        if (testConfig.isMetricsEnabled()) {
            b.include(MetricsConfig.builder()
                                   .with(MetricsConfig.ENABLE_STATISTICS, true)
                                   .with(MetricsConfig.OUTPUT_FREQUENCY, 1));
        }

        return b.build();
    }

    /**
     * Generates a new ServiceBuilderConfig.Builder with hardcoded defaults, in case these are not supplied via other means,
     * such as a config file or System Properties.
     */
    private static ServiceBuilderConfig.Builder getDefaultServiceBuilderConfig() {
        return ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                                      .with(ServiceConfig.THREAD_POOL_SIZE, 80)
                                      .with(ServiceConfig.CACHE_POLICY_MAX_TIME, 30)
                                      .with(ServiceConfig.CACHE_POLICY_MAX_SIZE, 6 * 1024 * 1024 * 1024L)
                                      .with(ServiceConfig.CERT_FILE, "../config/cert.pem")
                                      .with(ServiceConfig.KEY_FILE, "../config/key.pem"))
                .include(DurableLogConfig.builder()
                                         .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                                         .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 100)
                                         .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 100 * 1024 * 1024L))
                .include(ReadIndexConfig.builder()
                                        .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, 128 * 1024))
                .include(WriterConfig.builder()
                        .with(WriterConfig.MAX_ITEMS_TO_READ_AT_ONCE, 10000)
                        .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 64 * 1024 * 1024)
                        .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, 64 * 1024 * 1024))
                .include(ContainerConfig.builder()
                                        .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS,
                                                ContainerConfig.MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS)
                                        .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, 500))
                // This is for those tests that use BookKeeper for Tier1.
                .include(BookKeeperConfig.builder()
                                         .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, Integer.MAX_VALUE)
                                         .with(BookKeeperConfig.ZK_METADATA_PATH, "/pravega/selftest/segmentstore/containers")
                        .with(BookKeeperConfig.BK_LEDGER_PATH, TestConfig.BK_ZK_LEDGER_PATH));
    }

    private static void setupLogging(TestConfig testConfig) {
        val logFile = new java.io.File(testConfig.getTestLogPath());
        if (logFile.delete()) {
            TestLogger.log("Main", "Deleted log file %s.", logFile.getAbsolutePath());
        }

        // Configure slf4j to not log anything (console or whatever). This interferes with the console interaction.
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).detachAndStopAllAppenders();

        val fa = new FileAppender<ILoggingEvent>();
        fa.setContext(context);
        fa.setName("selftest");
        fa.setFile(logFile.getAbsolutePath());

        val encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%date{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %level - %msg%n");
        encoder.start();
        fa.setEncoder(encoder);
        fa.start();

        context.getLoggerList().get(0).addAppender(fa);
        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();
    }

    private static void printUsage() {
        System.out.println("SelfTest arguments (set via System Properties):");
        System.out.println("- Any property defined in TestConfig or other *Config classes in SegmentStore.");
        System.out.println(String.format("- %s: Load up configuration from this file.", TestConfig.CONFIG_FILE_PROPERTY_NAME));
        System.out.println("- Shortcuts:");
        Shortcuts.forEach(s -> System.out.println(String.format("\t-%s: %s", s.key, s.property.getName())));
        System.out.println("At least one shortcut or a reference to a config file is required for the test.");
        System.out.println();
        System.out.println("Full user manual: https://github.com/pravega/pravega/wiki/Local-Stress-Testing.");
    }

    //region Shortcuts

    private static class Shortcuts {
        private static final Map<Supplier<ConfigBuilder<?>>, List<Shortcut>> SHORTCUTS;

        static {
            val s = new HashMap<Supplier<ConfigBuilder<?>>, List<Shortcut>>();
            s.put(TestConfig::builder, Arrays.asList(
                    new Shortcut("c", TestConfig.CONTAINER_COUNT),
                    new Shortcut("s", TestConfig.STREAM_COUNT),
                    new Shortcut("sc", TestConfig.SEGMENTS_PER_STREAM),
                    new Shortcut("o", TestConfig.OPERATION_COUNT),
                    new Shortcut("p", TestConfig.PRODUCER_COUNT),
                    new Shortcut("pp", TestConfig.PRODUCER_PARALLELISM),
                    new Shortcut("wps", TestConfig.CLIENT_WRITERS_PER_STREAM),
                    new Shortcut("ws", TestConfig.MIN_APPEND_SIZE),
                    new Shortcut("ws", TestConfig.MAX_APPEND_SIZE),
                    new Shortcut("target", TestConfig.TEST_TYPE),
                    new Shortcut("cc", TestConfig.CONTROLLER_COUNT),
                    new Shortcut("ssc", TestConfig.SEGMENT_STORE_COUNT),
                    new Shortcut("bkc", TestConfig.BOOKIE_COUNT),
                    new Shortcut("bkledgerdir", TestConfig.BOOKIE_LEDGERS_DIR),
                    new Shortcut("storagedir", TestConfig.STORAGE_DIR),
                    new Shortcut("controller", TestConfig.CONTROLLER_HOST),
                    new Shortcut("controllerport", TestConfig.CONTROLLER_BASE_PORT),
                    new Shortcut("metrics", TestConfig.METRICS_ENABLED),
                    new Shortcut("reads", TestConfig.READS_ENABLED),
                    new Shortcut("txnf", TestConfig.TRANSACTION_FREQUENCY),
                    new Shortcut("txnc", TestConfig.MAX_TRANSACTION_SIZE),
                    new Shortcut("tcu", TestConfig.TABLE_CONDITIONAL_UPDATES),
                    new Shortcut("tct", TestConfig.TABLE_CONSUMERS_PER_TABLE),
                    new Shortcut("slts", TestConfig.CHUNKED_SEGMENT_STORAGE_ENABLED),
                    new Shortcut("tkl", TestConfig.TABLE_KEY_LENGTH),
                    new Shortcut("tt", TestConfig.TABLE_TYPE),
                    new Shortcut("pause", TestConfig.PAUSE_BEFORE_EXIT)));

            SHORTCUTS = Collections.unmodifiableMap(s);
        }

        static Properties extract(Properties source) {
            Properties result = new Properties();
            for (val e : SHORTCUTS.entrySet()) {
                val builder = e.getKey().get();
                boolean any = false;
                for (val s : e.getValue()) {
                    val value = source.getProperty(s.key, null);
                    if (value != null) {
                        builder.withUnsafe(s.property, value);
                        any = true;
                    }
                }
                if (any) {
                    builder.copyTo(result);
                }
            }

            return result;
        }

        static void forEach(java.util.function.Consumer<Shortcut> consumer) {
            SHORTCUTS.values().forEach(l -> l.forEach(consumer));
        }

        @RequiredArgsConstructor
        private static class Shortcut {
            final String key;
            final Property<?> property;
        }
    }

    //endregion
}
