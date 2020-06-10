/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.Assert;

public class ContainerMock implements SegmentContainer {

    private final AtomicReference<SegmentMock> segment;
    private final Supplier<SegmentMock> segmentCreator;
    private final ExecutorService executorService;
    private final AtomicBoolean closed;
    private final int containerId;

    ContainerMock(Supplier<SegmentMock> segmentCreator, int containerId, ScheduledExecutorService executorService) {
        this.segmentCreator = segmentCreator;
        this.containerId = containerId;
        this.executorService = executorService;
        this.segment = new AtomicReference<>();
        this.closed = new AtomicBoolean();
    }

    SegmentMock getSegment() {
        return segment.get();
    }

    @Override
    public int getId() {
        return containerId;
    }

    @Override
    public void close() {
        this.closed.set(true);
    }

    @Override
    public CompletableFuture<DirectSegmentAccess> forSegment(String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        SegmentMock segment = this.segment.get();
        if (segment == null) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
        }

        Assert.assertEquals("Unexpected segment name.", segment.getInfo().getName(), segmentName);
        return CompletableFuture.supplyAsync(() -> segment, executorService);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String segmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        if (this.segment.get() != null) {
            return Futures.failedFuture(new StreamSegmentExistsException(segmentName));
        }

        return CompletableFuture
                .runAsync(() -> {
                    SegmentMock segment = this.segmentCreator.get();
                    Assert.assertTrue(this.segment.compareAndSet(null, segment));
                }, executorService)
                .thenCompose(v -> this.segment.get().updateAttributes(attributes == null ? Collections.emptyList() : attributes, timeout));
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String segmentName, Duration timeout) {
        SegmentMock segment = this.segment.get();
        if (segment == null) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
        }
        Assert.assertEquals("Unexpected segment name.", segment.getInfo().getName(), segmentName);
        Assert.assertTrue(this.segment.compareAndSet(segment, null));
        return CompletableFuture.completedFuture(null);
    }

    //region Not Implemented Methods

    @Override
    public Collection<SegmentProperties> getActiveSegments() {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public Service startAsync() {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public boolean isRunning() {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public State state() {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public Service stopAsync() {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public void awaitRunning() {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public void awaitRunning(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public void awaitTerminated() {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public void awaitTerminated(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public Throwable failureCause() {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public void addListener(Listener listener, Executor executor) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> getAttributes(String streamSegmentName, Collection<UUID> attributeIds, boolean cache, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetSegmentName, String sourceSegmentName, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        throw new UnsupportedOperationException("Not Expected");
    }

    @Override
    public boolean isOffline() {
        throw new UnsupportedOperationException("Not Expected");
    }

    //endregion
}
