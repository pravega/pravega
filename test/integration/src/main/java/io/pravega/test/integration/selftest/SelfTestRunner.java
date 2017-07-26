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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.test.integration.selftest.adapters.SegmentStoreAdapter;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for Self Tester.
 */
public class SelfTestRunner {
    private static final long STARTUP_TIMEOUT_MILLIS = 60 * 1000;

    public static void main(String[] args) throws Exception {
        TestConfig testConfig = getTestConfig();
        setupLogging(testConfig);
        ServiceBuilderConfig builderConfig = getBaseSegmentStoreConfig(testConfig);

        // Create a new SelfTest.
        @Cleanup
        SelfTest test = new SelfTest(testConfig, builderConfig);

        // Start the test.
        test.startAsync().awaitRunning(STARTUP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Wait for the test to finish.
        test.awaitFinished().join();

        // Make sure the test is stopped.
        test.stopAsync().awaitTerminated();
    }

    private static ServiceBuilderConfig getBaseSegmentStoreConfig(TestConfig testConfig) {
        int bkWriteQuorum = Math.min(3, testConfig.getBookieCount());
        return ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                                      .with(ServiceConfig.CONTAINER_COUNT, testConfig.getContainerCount())
                                      .with(ServiceConfig.THREAD_POOL_SIZE, 30))
                .include(DurableLogConfig.builder()
                                         .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                                         .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 100)
                                         .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 100 * 1024 * 1024L))
                .include(ReadIndexConfig.builder()
                                        .with(ReadIndexConfig.CACHE_POLICY_MAX_TIME, 600 * 1000)
                                        .with(ReadIndexConfig.CACHE_POLICY_MAX_SIZE, 1024 * 1024 * 1024L)
                                        .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, 128 * 1024))
                .include(ContainerConfig.builder()
                                        .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS,
                                                ContainerConfig.MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS)
                                        .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, 500))
                // This is for those tests that use BookKeeper for Tier1.
                .include(BookKeeperConfig.builder()
                                         .with(BookKeeperConfig.MAX_CONCURRENT_WRITES, 4)
                                         .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, Integer.MAX_VALUE)
                                         .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + getTestConfig().getZkPort())
                                         .with(BookKeeperConfig.ZK_METADATA_PATH, "/pravega/selftest/segmentstore/containers")
                                         .with(BookKeeperConfig.BK_LEDGER_PATH, SegmentStoreAdapter.BK_LEDGER_PATH)
                                         .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, bkWriteQuorum)
                                         .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, bkWriteQuorum)
                                         .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, bkWriteQuorum))
                .build();
    }

    private static TestConfig getTestConfig() {
        return TestConfig
                .builder()
                // Test params.
                .with(TestConfig.PRODUCER_COUNT, 100)
                .with(TestConfig.OPERATION_COUNT, 100000)
                .with(TestConfig.STREAM_COUNT, 1)
                .with(TestConfig.MIN_APPEND_SIZE, 10000)
                .with(TestConfig.MAX_APPEND_SIZE, 10000)
                .with(TestConfig.TEST_TYPE, TestConfig.TestType.OutOfProcessClient.toString())

                // Transaction setup.
                .with(TestConfig.MAX_TRANSACTION_SIZE, 20)
                .with(TestConfig.TRANSACTION_FREQUENCY, Integer.MAX_VALUE)

                // Test setup.
                .with(TestConfig.THREAD_POOL_SIZE, 80)
                .with(TestConfig.TIMEOUT_MILLIS, 3000)

                // Tier1
                .with(TestConfig.BOOKIE_COUNT, 1)
                .build();
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
}
