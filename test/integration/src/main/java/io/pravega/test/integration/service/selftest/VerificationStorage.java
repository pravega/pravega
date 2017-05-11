/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.service.selftest;

import io.pravega.common.Exceptions;
import io.pravega.common.function.CallbackHelpers;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.storage.SegmentHandle;
import io.pravega.service.storage.Storage;
import io.pravega.service.storage.StorageFactory;
import io.pravega.service.storage.TruncateableStorage;
import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import lombok.val;

/**
 * Wrapper Storage that accepts Segment Length change listeners.
 */
class VerificationStorage implements TruncateableStorage {
    //region Members

    private final TruncateableStorage baseStorage;
    private final HashMap<String, HashMap<Integer, SegmentUpdateListener>> updateListeners;
    private final AtomicBoolean closed;
    private final Object listenerLock = new Object();
    private final Executor executor;
    private int nextRegistrationId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the VerificationStorage class.
     *
     * @param baseStorage The wrapped Storage.
     * @param executor    An Executor to use for callback invocation.
     */
    private VerificationStorage(TruncateableStorage baseStorage, Executor executor) {
        Preconditions.checkNotNull(baseStorage, "baseStorage");
        Preconditions.checkNotNull(executor, "executor");
        this.baseStorage = baseStorage;
        this.updateListeners = new HashMap<>();
        this.closed = new AtomicBoolean();
        this.executor = executor;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            this.baseStorage.close();
            unregisterAllListeners();
            this.closed.set(true);
        }
    }

    //endregion

    //region Storage Implementation

    @Override
    public void initialize(long epoch) {
        // Nothing to do.
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return this.baseStorage.create(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return this.baseStorage.openWrite(streamSegmentName);
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return this.baseStorage.openRead(streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        CompletableFuture<Void> result = this.baseStorage.write(handle, offset, data, length, timeout);
        result.thenRunAsync(() -> triggerListeners(handle.getSegmentName(), offset + length, false), this.executor);
        return result;
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        CompletableFuture<Void> result = this.baseStorage.seal(handle, timeout);
        result.thenComposeAsync(v -> this.baseStorage.getStreamSegmentInfo(handle.getSegmentName(), timeout), this.executor)
              .thenAcceptAsync(sp -> triggerListeners(handle.getSegmentName(), sp.getLength(), sp.isSealed()), this.executor);
        return result;
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        unregisterAllListeners(sourceSegment);
        CompletableFuture<Void> result = this.baseStorage.concat(targetHandle, offset, sourceSegment, timeout);
        result.thenComposeAsync(v -> this.baseStorage.getStreamSegmentInfo(targetHandle.getSegmentName(), timeout), this.executor)
              .thenAcceptAsync(sp -> triggerListeners(targetHandle.getSegmentName(), sp.getLength(), false), this.executor);
        return result;
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        unregisterAllListeners(handle.getSegmentName());
        return this.baseStorage.delete(handle, timeout);
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return this.baseStorage.read(handle, offset, buffer, bufferOffset, length, timeout);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return this.baseStorage.getStreamSegmentInfo(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return this.baseStorage.exists(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> truncate(String segmentName, long offset, Duration timeout) {
        return this.baseStorage.truncate(segmentName, offset, timeout);
    }

    //endregion

    //region Listener Registration

    /**
     * Registers the given SegmentUpdateListener.
     *
     * @param listener The listener to register.
     */
    void registerListener(SegmentUpdateListener listener) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        synchronized (this.listenerLock) {
            HashMap<Integer, SegmentUpdateListener> segmentListeners = this.updateListeners.getOrDefault(listener.segmentName, null);
            if (segmentListeners == null) {
                segmentListeners = new HashMap<>();
                this.updateListeners.put(listener.segmentName, segmentListeners);
            }

            int registrationId = ++this.nextRegistrationId;
            listener.register(registrationId, this::unregisterListener);
            segmentListeners.put(registrationId, listener);
        }
    }

    /**
     * Triggers all registered SegmentUpdateListeners for the given segment.
     *
     * @param segmentName The name of the Segment to trigger for.
     * @param length      The current length of the Segment.
     */
    private void triggerListeners(String segmentName, long length, boolean sealed) {
        ArrayList<SegmentUpdateListener> listeners = null;
        synchronized (this.listenerLock) {
            val segmentListeners = this.updateListeners.getOrDefault(segmentName, null);
            if (segmentListeners != null) {
                listeners = new ArrayList<>(segmentListeners.values());
            }
        }

        if (listeners != null) {
            listeners.forEach(l -> CallbackHelpers.invokeSafely(l.callback, length, sealed, null));
        }
    }

    /**
     * Unregisters the given SegmentUpdateListener.
     *
     * @param listener The listener to unregister.
     */
    private void unregisterListener(SegmentUpdateListener listener) {
        synchronized (this.listenerLock) {
            val segmentListeners = this.updateListeners.getOrDefault(listener.segmentName, null);
            if (segmentListeners != null) {
                segmentListeners.remove(listener.registrationId);
                if (segmentListeners.size() == 0) {
                    this.updateListeners.remove(listener.segmentName);
                }
            }
        }
    }

    /**
     * Unregisters all listeners for the given Segment.
     *
     * @param segmentName The name of the Segment to unregister listeners for.
     */
    private void unregisterAllListeners(String segmentName) {
        synchronized (this.listenerLock) {
            val segmentListeners = this.updateListeners.remove(segmentName);
            if (segmentListeners != null) {
                // This isn't really necessary, but it's a good practice to call close() on anything that implements AutoCloseable.
                segmentListeners.values().forEach(SegmentUpdateListener::close);
            }
        }
    }

    /**
     * Unregisters all listeners for all segments.
     */
    private void unregisterAllListeners() {
        synchronized (this.listenerLock) {
            ArrayList<String> segmentNames = new ArrayList<>(this.updateListeners.keySet());
            segmentNames.forEach(this::unregisterAllListeners);
        }
    }

    //endregion

    //region SegmentUpdateListener

    static class SegmentUpdateListener implements AutoCloseable {
        private final String segmentName;
        private final BiConsumer<Long, Boolean> callback;
        private int registrationId;
        private java.util.function.Consumer<SegmentUpdateListener> unregisterCallback;

        SegmentUpdateListener(String segmentName, BiConsumer<Long, Boolean> callback) {
            Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
            Preconditions.checkNotNull(callback, "callback");

            this.segmentName = segmentName;
            this.callback = callback;
            this.registrationId = -1;
        }

        @Override
        public void close() {
            if (this.unregisterCallback != null) {
                CallbackHelpers.invokeSafely(this.unregisterCallback, this, null);
                this.registrationId = -1;
                this.unregisterCallback = null;
            }
        }

        private void register(int id, java.util.function.Consumer<SegmentUpdateListener> unregisterCallback) {
            Preconditions.checkState(this.unregisterCallback == null, "This SegmentUpdateListener is already registered.");
            this.registrationId = id;
            this.unregisterCallback = unregisterCallback;
        }
    }

    //endregion

    //region Factory

    static class Factory implements StorageFactory, AutoCloseable {
        private final AtomicBoolean closed;
        private final VerificationStorage storage;

        /**
         * Creates a new instance of the VerificationStorage.Factory class.
         *
         * @param baseStorage The wrapped Storage.
         * @param executor    An Executor to use for callback invocation.
         */
        Factory(TruncateableStorage baseStorage, Executor executor) {
            this.storage = new VerificationStorage(baseStorage, executor);
            this.closed = new AtomicBoolean();
        }

        @Override
        public Storage createStorageAdapter() {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.storage;
        }

        @Override
        public void close() {
            if (!this.closed.get()) {
                this.storage.close();
                this.closed.set(true);
            }
        }
    }

    //endregion
}
