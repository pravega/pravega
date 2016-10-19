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
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import lombok.val;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

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
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return this.baseStorage.create(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        CompletableFuture<Void> result = this.baseStorage.write(streamSegmentName, offset, data, length, timeout);
        result.thenRun(() -> triggerListeners(streamSegmentName, offset + length));
        return result;
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        unregisterAllListeners(sourceStreamSegmentName);
        CompletableFuture<Void> result = this.baseStorage.concat(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout);
        result.thenCompose(v -> this.baseStorage.getStreamSegmentInfo(targetStreamSegmentName, timeout))
              .thenAccept(sp -> triggerListeners(targetStreamSegmentName, sp.getLength()));
        return result;
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        unregisterAllListeners(streamSegmentName);
        return this.baseStorage.delete(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return this.baseStorage.read(streamSegmentName, offset, buffer, bufferOffset, length, timeout);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return this.baseStorage.getStreamSegmentInfo(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return this.baseStorage.exists(streamSegmentName, timeout);
    }

    //endregion

    void registerListener(SegmentUpdateListener listener) {
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

    private void triggerListeners(String segmentName, long length) {
        ArrayList<SegmentUpdateListener> listeners = null;
        synchronized (this.listenerLock) {
            val segmentListeners = this.updateListeners.getOrDefault(segmentName, null);
            if (segmentListeners != null) {
                listeners = new ArrayList<>(segmentListeners.values());
            }
        }

        if (listeners != null) {
            listeners.forEach(l -> CallbackHelpers.invokeSafely(l.callback, length, null));
        }
    }

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

    private void unregisterAllListeners(String segmentName) {
        synchronized (this.listenerLock) {
            val segmentListeners = this.updateListeners.remove(segmentName);
            if (segmentListeners != null) {
                // This isn't really necessary, but it's a good practice to call close() on anything that implements AutoCloseable.
                segmentListeners.values().forEach(SegmentUpdateListener::close);
            }
        }
    }

    private void unregisterAllListeners() {
        synchronized (this.listenerLock) {
            ArrayList<String> segmentNames = new ArrayList<>(this.updateListeners.keySet());
            segmentNames.forEach(this::unregisterAllListeners);
        }
    }

    //region SegmentUpdateListener

    static class SegmentUpdateListener implements AutoCloseable {
        private final String segmentName;
        private final java.util.function.Consumer<Long> callback;
        private int registrationId;
        private java.util.function.Consumer<SegmentUpdateListener> unregisterCallback;

        SegmentUpdateListener(String segmentName, java.util.function.Consumer<Long> callback) {
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
}
