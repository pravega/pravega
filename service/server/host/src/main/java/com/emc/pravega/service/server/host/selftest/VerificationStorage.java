/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.service.storage.StorageSegmentInformation;
import com.google.common.base.Preconditions;
import lombok.val;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Wrapper Storage that accepts Segment Length change listeners.
 */
class VerificationStorage implements Storage {
    //region Members

    private final Storage baseStorage;
    private final HashMap<String, HashMap<Integer, SegmentUpdateListener>> updateListeners;
    private final AtomicBoolean closed;
    private int nextRegistrationId;
    private final Object listenerLock = new Object();

    //endregion

    //region Constructor

    VerificationStorage(Storage baseStorage) {
        Preconditions.checkNotNull(baseStorage, "baseStorage");
        this.baseStorage = baseStorage;
        this.updateListeners = new HashMap<>();
        this.closed = new AtomicBoolean();
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
    public CompletableFuture<SegmentHandle> create(String segmentName, Duration timeout) {
        return this.baseStorage.create(segmentName, timeout);
    }

    @Override
    public CompletableFuture<SegmentHandle> open(String streamSegmentName, Duration timeout) {
        return this.baseStorage.open(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle segmentHandle, long offset, InputStream data, int length, Duration timeout) {
        CompletableFuture<Void> result = this.baseStorage.write(segmentHandle, offset, data, length, timeout);
        result.thenRun(() -> triggerListeners(segmentHandle, offset + length, false));
        return result;
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(SegmentHandle segmentHandle, Duration timeout) {
        CompletableFuture<SegmentProperties> result = this.baseStorage.seal(segmentHandle, timeout);
        result.thenAccept(sp -> triggerListeners(segmentHandle, sp.getLength(), sp.isSealed()));
        return result;
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetSegmentHandle, long offset, SegmentHandle sourceSegmentHandle, Duration timeout) {
        unregisterAllListeners(sourceSegmentHandle.getSegmentName());
        CompletableFuture<Void> result = this.baseStorage.concat(targetSegmentHandle, offset, sourceSegmentHandle, timeout);
        result.thenCompose(v -> this.baseStorage.getStreamSegmentInfo(targetSegmentHandle.getSegmentName(), timeout))
              .thenAccept(sp -> triggerListeners(targetSegmentHandle, sp.getLength(), false));
        return result;
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle segmentHandle, Duration timeout) {
        unregisterAllListeners(segmentHandle.getSegmentName());
        return this.baseStorage.delete(segmentHandle, timeout);
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle segmentHandle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return this.baseStorage.read(segmentHandle, offset, buffer, bufferOffset, length, timeout);
    }

    @Override
    public CompletableFuture<StorageSegmentInformation> getStreamSegmentInfo(String segmentName, Duration timeout) {
        return this.baseStorage.getStreamSegmentInfo(segmentName, timeout);
    }

    @Override
    public CompletableFuture<Boolean> exists(String segmentName, Duration timeout) {
        return this.baseStorage.exists(segmentName, timeout);
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
     * @param segmentHandle The SegmentHandle for which to trigger for.
     * @param length        The current length of the Segment.
     */
    private void triggerListeners(SegmentHandle segmentHandle, long length, boolean sealed) {
        ArrayList<SegmentUpdateListener> listeners = null;
        synchronized (this.listenerLock) {
            val segmentListeners = this.updateListeners.getOrDefault(segmentHandle.getSegmentName(), null);
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

    static class Factory implements StorageFactory {
        private final AtomicBoolean closed;
        private final VerificationStorage storage;

        Factory(Storage baseStorage) {
            this.storage = new VerificationStorage(baseStorage);
            this.closed = new AtomicBoolean();
        }

        @Override
        public Storage getStorageAdapter() {
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
