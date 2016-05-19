package com.emc.logservice.mocks;

import com.emc.logservice.Storage;
import com.emc.logservice.StreamSegmentInformation;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * In-Memory mock for Storage.
 */
public class InMemoryStorage implements Storage {
    @Override
    public CompletableFuture<StreamSegmentInformation> create(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<StreamSegmentInformation> seal(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<StreamSegmentInformation> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, String sourceStreamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        return null;
    }
}
