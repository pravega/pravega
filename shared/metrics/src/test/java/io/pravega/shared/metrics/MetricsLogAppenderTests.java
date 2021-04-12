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
package io.pravega.shared.metrics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.MetricsTags;
import io.pravega.test.common.SerializedClassRunner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.slf4j.Marker;

/**
 * Unit tests for the {@link MetricsLogAppender} class.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
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

    @After
    public void tearDown() {
        MetricsProvider.getMetricsProvider().close();
    }

    /**
     * Tests the {@link MetricsNames#LOG_WARNINGS} metric.
     */
    @Test
    public void testLogWarn() {
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

    /**
     * This test does nothing. It simply invokes all the other methods that are not used on this appender to please the
     * code coverage tool.
     */
    @Test
    public void testOtherMethods() {
        val appender = new MetricsLogAppender();
        appender.start();
        Assert.assertTrue(appender.isStarted());
        appender.stop();
        appender.setContext(null);
        Assert.assertNull(appender.getContext());
        Assert.assertEquals("Metrics Appender", appender.getName());
        appender.setName(null);
        appender.addStatus(null);
        appender.addInfo(null);
        appender.addInfo(null, null);
        appender.addWarn(null);
        appender.addWarn(null, null);
        appender.addError(null);
        appender.addError(null, null);
        appender.addFilter(null);
        appender.clearAllFilters();
        Assert.assertNull(appender.getCopyOfAttachedFiltersList());
        Assert.assertNull(appender.getFilterChainDecision(null));
    }

    private void testLog(Level level, String metricName, boolean expectValues) {
        val classNames = ImmutableMap
                .<String, String>builder()
                .put("A", "A")
                .put("B.", "B.")
                .put(".C", "C")
                .put("D.E.F", "F")
                .build();

        // Create a set of exceptions; add them as-is and then wrap them in either CompletionException or ExecutionException.
        val baseExceptions = Arrays.<Exception>asList(null, new Exception(), new IllegalStateException(), new NullPointerException());
        val exceptions = new ArrayList<Exception>();
        baseExceptions.forEach(t -> {
            exceptions.add(t);
            exceptions.add(new CompletionException(t));
            exceptions.add(new ExecutionException(t));
        });

        val appender = new MetricsLogAppender();
        for (val logClassName : classNames.entrySet()) {
            for (val ex : exceptions) {
                appender.doAppend(new TestEvent(level, logClassName.getKey(), ex == null ? null : new TestProxy(ex)));
            }
        }

        for (val logClassName : classNames.entrySet()) {
            for (val ex : exceptions) {
                val unwrapped = Exceptions.unwrap(ex);
                val c = MetricRegistryUtils.getMeter(metricName, MetricsTags.exceptionTag(logClassName.getValue(),
                        ex == null ? null : unwrapped.getClass().getName()));
                int expectedValue = expectValues ? 1 : 0;
                if (unwrapped != null && baseExceptions.contains(unwrapped)) {
                    // Exceptions are reported 3 times: once by themselves, and twice wrapped in others.
                    expectedValue *= 3;
                }
                Assert.assertNotNull(c);
                Assert.assertEquals("Unexpected counter value for " + logClassName + "-" + unwrapped, expectedValue, (int) c.totalAmount());
            }
        }
    }

    @RequiredArgsConstructor
    private static class TestProxy implements IThrowableProxy {
        private final Throwable t;

        @Override
        public String getMessage() {
            return t.getMessage();
        }

        @Override
        public String getClassName() {
            return t.getClass().getName();
        }

        @Override
        public IThrowableProxy getCause() {
            return t.getCause() == null ? null : new TestProxy(t.getCause());
        }

        @Override
        public StackTraceElementProxy[] getStackTraceElementProxyArray() {
            return new StackTraceElementProxy[0];
        }

        @Override
        public int getCommonFrames() {
            return 0;
        }

        @Override
        public IThrowableProxy[] getSuppressed() {
            return new IThrowableProxy[0];
        }
    }

    @Getter
    @RequiredArgsConstructor
    private static class TestEvent implements ILoggingEvent {
        private final Level level;
        private final String loggerName;
        private final IThrowableProxy throwableProxy;

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
        @SuppressWarnings("deprecation")
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
