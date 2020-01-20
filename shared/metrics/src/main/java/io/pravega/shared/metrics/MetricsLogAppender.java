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
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.MetricsTags;
import java.util.List;

/**
 * Log Appender that intercepts all events with {@link Level#ERROR} or {@link Level#WARN} and records a metric for them.
 */
public class MetricsLogAppender implements Appender<ILoggingEvent> {
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    //region Appender Implementation

    @Override
    public String getName() {
        return "Metrics Appender";
    }

    @Override
    public void doAppend(ILoggingEvent event) throws LogbackException {
        if (event.getLevel() == Level.ERROR) {
            DYNAMIC_LOGGER.incCounterValue(MetricsNames.LOG_ERRORS, 1, MetricsTags.classNameTag(event.getLoggerName()));
        } else if (event.getLevel() == Level.WARN) {
            DYNAMIC_LOGGER.incCounterValue(MetricsNames.LOG_WARNINGS, 1, MetricsTags.classNameTag(event.getLoggerName()));
        }
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