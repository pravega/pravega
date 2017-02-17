/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.selftest;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import com.emc.pravega.common.util.PropertyBag;
import com.emc.pravega.service.server.containers.ContainerConfig;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import java.util.Properties;
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

        // Star the test.
        test.startAsync().awaitRunning(testConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS);

        // Wait for the test to finish.
        test.awaitFinished().join();

        // Make sure the test is stopped.
        test.stopAsync().awaitTerminated();
    }

    private static ServiceBuilderConfig getBuilderConfig() {
        Properties p = new Properties();

        // Change Number of containers and Thread Pool Size for each test.
        ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_CONTAINER_COUNT, "2");
        ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_THREAD_POOL_SIZE, "20");

        // TODO: consider setting the following as defaults in their config classes.
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_COMMIT_COUNT, "100");
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, "100");
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, "104857600");

        ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_TIME, Integer.toString(60 * 1000));
        ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_SIZE, Long.toString(128 * 1024 * 1024));
        ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_MEMORY_READ_MIN_LENGTH, Integer.toString(128 * 1024));

        ServiceBuilderConfig.set(p, ContainerConfig.COMPONENT_CODE, ContainerConfig.PROPERTY_SEGMENT_METADATA_EXPIRATION_SECONDS,
                Integer.toString(ContainerConfig.MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS));

        // All component configs should have defaults built-in, so no need to override them here
        return new ServiceBuilderConfig(p);
    }

    private static TestConfig getTestConfig() {
        return new TestConfig(TestConfig.convert(TestConfig.COMPONENT_CODE,
                PropertyBag.create()
                           // Test params.
                           .with(TestConfig.PROPERTY_OPERATION_COUNT, 2000000)
                           .with(TestConfig.PROPERTY_SEGMENT_COUNT, 1)
                           .with(TestConfig.PROPERTY_PRODUCER_COUNT, 100)
                           .with(TestConfig.PROPERTY_MIN_APPEND_SIZE, 100)
                           .with(TestConfig.PROPERTY_MAX_APPEND_SIZE, 100)
                           //.with(TestConfig.PROPERTY_MIN_APPEND_SIZE, WireCommands.APPEND_BLOCK_SIZE)
                           //.with(TestConfig.PROPERTY_MAX_APPEND_SIZE, WireCommands.APPEND_BLOCK_SIZE)

                           // Transaction setup.
                           .with(TestConfig.PROPERTY_MAX_TRANSACTION_SIZE, 20)
                           .with(TestConfig.PROPERTY_TRANSACTION_FREQUENCY, Integer.MAX_VALUE)

                           // Test setup.
                           .with(TestConfig.PROPERTY_THREAD_POOL_SIZE, 50)
                           .with(TestConfig.PROPERTY_DATA_LOG_APPEND_DELAY, 0)
                           .with(TestConfig.PROPERTY_TIMEOUT_MILLIS, 3000)
                           .with(TestConfig.PROPERTY_VERBOSE_LOGGING, false)

                           // Client-specific settings.
                           .with(TestConfig.PROPERTY_USE_CLIENT, false)
                           .with(TestConfig.PROPERTY_CLIENT_AUTO_FLUSH, false)
                           .with(TestConfig.PROPERTY_CLIENT_PORT, 9876)));
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
