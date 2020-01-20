/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import com.google.common.collect.ImmutableMap;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.MetricsTags;
import java.util.Arrays;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Marker;

/**
 * Unit tests for the {@link MetricsLogAppender} class.
 */
@Slf4j
public class MetricsLogAppenderTests {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Before
    public void setUp() {
        MetricsProvider.initialize(MetricsConfig.builder()
                                                .with(MetricsConfig.ENABLE_STATISTICS, true)
                                                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
    }

    /**
     * Tests the {@link MetricsNames#LOG_WARNINGS} metric.
     */
    @Test
    public void testWarn() {
        testLog(Level.WARN, MetricsNames.LOG_WARNINGS, true);
    }

    /**
     * Tests the {@link MetricsNames#LOG_ERRORS} metric.
     */
    @Test
    public void testLogError() {
        testLog(Level.ERROR, MetricsNames.LOG_ERRORS, true);
    }

    /**
     * Tests that no other log levels result in any activity.
     */
    @Test
    public void testOtherLevels() {
        val metricsToCheck = Arrays.asList(MetricsNames.LOG_WARNINGS, MetricsNames.LOG_ERRORS);
        for (val m : metricsToCheck) {
            testLog(Level.TRACE, m, false);
            testLog(Level.DEBUG, m, false);
            testLog(Level.INFO, m, false);
        }
    }

    private void testLog(Level level, String metricName, boolean expectValues) {
        val classNames = ImmutableMap
                .<String, String>builder()
                .put("A", "A")
                .put("B.", "B.")
                .put(".C", "C")
                .put("D.E.F", "F")
                .build();
        val appender = new MetricsLogAppender();
        for (val e : classNames.entrySet()) {
            appender.doAppend(new TestEvent(level, e.getKey()));
        }

        for (val e : classNames.entrySet()) {
            val c = MetricRegistryUtils.getCounter(metricName, MetricsTags.classNameTag(e.getValue()));
            int expectedValue = expectValues ? 1 : 0;
            Assert.assertEquals("Unexpected counter value for " + e, expectedValue, (int) c.count());
        }
    }

    @Builder
    @Getter
    private static class TestEvent implements ILoggingEvent {
        private final Level level;
        private final String loggerName;

        @Override
        public String getThreadName() {
            return "Test Thread Name";
        }

        @Override
        public String getMessage() {
            return "Test Message";
        }

        @Override
        public Object[] getArgumentArray() {
            return new Object[0];
        }

        @Override
        public String getFormattedMessage() {
            return getMessage();
        }

        @Override
        public LoggerContextVO getLoggerContextVO() {
            return null;
        }

        @Override
        public IThrowableProxy getThrowableProxy() {
            return null;
        }

        @Override
        public StackTraceElement[] getCallerData() {
            return new StackTraceElement[0];
        }

        @Override
        public boolean hasCallerData() {
            return false;
        }

        @Override
        public Marker getMarker() {
            return null;
        }

        @Override
        public Map<String, String> getMDCPropertyMap() {
            return null;
        }

        @Override
        public Map<String, String> getMdc() {
            return null;
        }

        @Override
        public long getTimeStamp() {
            return 0;
        }

        @Override
        public void prepareForDeferredProcessing() {
        }
    }
}
