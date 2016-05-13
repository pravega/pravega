package com.emc.logservice;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Container for StreamSegments. All StreamSegments that are related (based on a hashing functions) will belong to the
 * same StreamSegmentContainer. Handles all operations that can be performed on such streams.
 */
public class StreamSegmentContainer implements StreamSegmentStore, Container {
    //region AutoCloseable Implementation

    @Override
    public void close() throws Exception {

    }

    //endregion

    //region Container Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> start(Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> stop(Duration timeout) {
        return null;
    }

    @Override
    public void registerFaultHandler(Consumer<Throwable> handler) {

    }

    @Override
    public ContainerState getState() {
        return null;
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, byte[] data, Duration timeout) {
        return null;
    }

    @Override
    public ReadResult read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<StreamSegmentInformation> getStreamInfo(String streamName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<String> createBatch(String parentStreamName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Long> mergeBatch(String batchName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamName, Duration timeout) {
        return null;
    }

    //endregion
}
