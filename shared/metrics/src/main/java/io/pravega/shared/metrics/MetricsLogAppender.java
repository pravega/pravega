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
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.MetricsTags;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Log Appender that intercepts all events with {@link Level#ERROR} or {@link Level#WARN} and records a metric for them.
 */
public class MetricsLogAppender implements Appender<ILoggingEvent> {
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final String COMPLETION_EXCEPTION_NAME = CompletionException.class.getSimpleName();
    private static final String EXECUTION_EXCEPTION_NAME = ExecutionException.class.getSimpleName();

    //region Appender Implementation

    @Override
    public String getName() {
        return "Metrics Appender";
    }

    @Override
    public void doAppend(ILoggingEvent event) throws LogbackException {
        if (event.getLevel() == Level.ERROR) {
            recordEvent(MetricsNames.LOG_ERRORS, event);
        } else if (event.getLevel() == Level.WARN) {
            recordEvent(MetricsNames.LOG_WARNINGS, event);
        }
    }

    private void recordEvent(String metricName, ILoggingEvent event) {
        IThrowableProxy p = event.getThrowableProxy();
        while (shouldUnwrap(p)) {
            p = p.getCause();
        }

        DYNAMIC_LOGGER.recordMeterEvents(metricName, 1, MetricsTags.exceptionTag(event.getLoggerName(), p == null ? null : p.getClassName()));
    }

    private boolean shouldUnwrap(IThrowableProxy p) {
        // Most of our exceptions are wrapped in CompletionException or ExecutionException. Since these are not very useful
        // in terms of determining the root cause, we should unwrap them to get to the actual exception that caused the event.
        return p != null
                && p.getCause() != null
                && (p.getClassName().endsWith(COMPLETION_EXCEPTION_NAME) || p.getClassName().endsWith(EXECUTION_EXCEPTION_NAME));

    }

    //endregion

    //region Unimplemented Methods

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean isStarted() {
        return true;
    }

    @Override
    public void setName(String name) {
    }

    @Override
    public void setContext(Context context) {
    }

    @Override
    public Context getContext() {
        return null;
    }

    @Override
    public void addStatus(Status status) {
    }

    @Override
    public void addInfo(String msg) {
    }

    @Override
    public void addInfo(String msg, Throwable ex) {
    }

    @Override
    public void addWarn(String msg) {
    }

    @Override
    public void addWarn(String msg, Throwable ex) {
    }

    @Override
    public void addError(String msg) {
    }

    @Override
    public void addError(String msg, Throwable ex) {
    }

    @Override
    public void addFilter(Filter<ILoggingEvent> newFilter) {
    }

    @Override
    public void clearAllFilters() {
    }

    @Override
    public List<Filter<ILoggingEvent>> getCopyOfAttachedFiltersList() {
        return null;
    }

    @Override
    public FilterReply getFilterChainDecision(ILoggingEvent event) {
        return null;
    }

    //endregion
}