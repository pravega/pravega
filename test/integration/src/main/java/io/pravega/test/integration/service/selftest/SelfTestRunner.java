/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.test.integration.service.selftest;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import io.pravega.service.server.containers.ContainerConfig;
import io.pravega.service.server.logs.DurableLogConfig;
import io.pravega.service.server.reading.ReadIndexConfig;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.service.server.store.ServiceConfig;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for Self Tester.
 */
public class SelfTestRunner {
    private static final String LOG_PATH = "/tmp/pravega/selftest.log";

    public static void main(String[] args) throws Exception {
        setupLogging();
        TestConfig testConfig = getTestConfig();
        ServiceBuilderConfig builderConfig = getBuilderConfig();

        // Create a new SelfTest.
        @Cleanup
        SelfTest test = new SelfTest(testConfig, builderConfig);

        // Start the test.
        test.startAsync().awaitRunning(testConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS);

        // Wait for the test to finish.
        test.awaitFinished().join();

        // Make sure the test is stopped.
        test.stopAsync().awaitTerminated();
    }

    private static ServiceBuilderConfig getBuilderConfig() {
        return ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                                      .with(ServiceConfig.CONTAINER_COUNT, 2)
                                      .with(ServiceConfig.THREAD_POOL_SIZE, 20))
                .include(DurableLogConfig.builder()
                                         .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                                         .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 100)
                                         .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 100 * 1024 * 1024L))
                .include(ReadIndexConfig.builder()
                                        .with(ReadIndexConfig.CACHE_POLICY_MAX_TIME, 60 * 1000)
                                        .with(ReadIndexConfig.CACHE_POLICY_MAX_SIZE, 128 * 1024 * 1024L)
                                        .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, 128 * 1024))
                .include(ContainerConfig.builder()
                                        .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS,
                                                ContainerConfig.MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS)
                                        .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, 500))
                .build();
    }

    private static TestConfig getTestConfig() {
        final int producers = 100;
        final boolean useClient = false;

        final int testThreadPoolAddition = useClient ? producers : 0;
        return TestConfig
                .builder()
                // Test params.
                .with(TestConfig.OPERATION_COUNT, 2000000)
                .with(TestConfig.SEGMENT_COUNT, 1)
                .with(TestConfig.MIN_APPEND_SIZE, 100)
                .with(TestConfig.MAX_APPEND_SIZE, 100)

                // Transaction setup.
                .with(TestConfig.MAX_TRANSACTION_SIZE, 20)
                .with(TestConfig.TRANSACTION_FREQUENCY, Integer.MAX_VALUE)

                // Test setup.
                .with(TestConfig.THREAD_POOL_SIZE, 50 + testThreadPoolAddition)
                .with(TestConfig.DATA_LOG_APPEND_DELAY, 0)
                .with(TestConfig.TIMEOUT_MILLIS, 3000)
                .with(TestConfig.VERBOSE_LOGGING, false)

                // Client-specific settings.
                .with(TestConfig.CLIENT_AUTO_FLUSH, false)
                .with(TestConfig.CLIENT_PORT, 9876)

                // Settings set via variables (see above).
                .with(TestConfig.PRODUCER_COUNT, producers)
                .with(TestConfig.USE_CLIENT, useClient)
                .with(TestConfig.CLIENT_WRITER_COUNT, producers)
                .build();
    }

    private static void setupLogging() {
        val logFile = new java.io.File(LOG_PATH);
        if (logFile.delete()) {
            TestLogger.log("Main", "Deleted log file %s.", LOG_PATH);
        }

        // Configure slf4j to not log anything (console or whatever). This interferes with the console interaction.
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).detachAndStopAllAppenders();

        val fa = new FileAppender<ILoggingEvent>();
        fa.setContext(context);
        fa.setName("selftest");
        fa.setFile(LOG_PATH);

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
