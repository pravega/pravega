/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;

/**
 * In-Memory mock for Storage. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorage implements SyncStorage, ListenableStorage {
    //region Members

    @GuardedBy("offsetTriggers")
    private final HashMap<String, HashMap<Long, CompletableFuture<Void>>> offsetTriggers;
    @GuardedBy("sealTriggers")
    private final HashMap<String, CompletableFuture<Void>> sealTriggers;
    @GuardedBy("lock")
    private final HashMap<String, StreamSegmentData> streamSegments = new HashMap<>();
    private final Object lock = new Object();
    private final AtomicLong currentOwnerId;
    private final SyncContext syncContext;
    private final AtomicBoolean initialized;
    private final AtomicBoolean closed;
    private final ScheduledExecutorService triggerExecutor = Executors.newScheduledThreadPool(1);

    //endregion

    //region Constructor

    public InMemoryStorage() {
        this.offsetTriggers = new HashMap<>();
        this.sealTriggers = new HashMap<>();
        this.currentOwnerId = new AtomicLong(0);
        this.syncContext = new SyncContext(this.currentOwnerId::get);
        this.initialized = new AtomicBoolean();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.triggerExecutor.shutdown();
        }
    }

    //endregion

    //region Misc methods

    public static SegmentHandle newHandle(String segmentName, boolean readOnly) {
        return new InMemorySegmentHandle(segmentName, readOnly);
    }

    //endregion

    //region Storage Implementation

    @Override
    public void initialize(long epoch) {
        // InMemoryStorage does not use epochs; we don't do anything with it.
        Preconditions.checkArgument(epoch > 0, "epoch must be a positive number. Given %s.", epoch);
        Preconditions.checkState(this.initialized.compareAndSet(false, true), "InMemoryStorage is already initialized.");
    }

    @Override
    public SegmentProperties create(String streamSegmentName) throws StreamSegmentExistsException {
        ensurePreconditions();
        synchronized (this.lock) {
            if (this.streamSegments.containsKey(streamSegmentName)) {
                throw new StreamSegmentExistsException(streamSegmentName);
            }

            StreamSegmentData data = new StreamSegmentData(streamSegmentName, this.syncContext);
            data.openWrite();
            this.streamSegments.put(streamSegmentName, data);
            return data.getInfo();
        }
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentNotExistsException {
        ensurePreconditions();
        return getStreamSegmentData(streamSegmentName).openWrite();
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentNotExistsException {
        ensurePreconditions();
        return getStreamSegmentData(streamSegmentName).openRead();
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws BadOffsetException, StreamSegmentNotExistsException,
            StreamSegmentSealedException {
        ensurePreconditions();
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot write using a read-only handle.");

        getStreamSegmentData(handle.getSegmentName()).write(offset, data, length);
        this.triggerExecutor.execute(() -> fireOffsetTriggers(handle.getSegmentName(), offset + length));
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentNotExistsException {
        ensurePreconditions();
        return getStreamSegmentData(handle.getSegmentName()).read(offset, buffer, bufferOffset, length);
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentNotExistsException {
        ensurePreconditions();
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot seal using a read-only handle.");
        getStreamSegmentData(handle.getSegmentName()).markSealed();
        this.triggerExecutor.execute(() -> {
            fireSealTrigger(handle.getSegmentName());
            cancelOffsetTriggersDueToSeal(handle.getSegmentName());
        });
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentNotExistsException {
        ensurePreconditions();
        return getStreamSegmentData(streamSegmentName).getInfo();
    }

    @Override
    public boolean exists(String streamSegmentName) {
        ensurePreconditions();
        synchronized (this.lock) {
            return this.streamSegments.containsKey(streamSegmentName);
        }
    }

    @Override
    public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws BadOffsetException, StreamSegmentNotExistsException,
            StreamSegmentSealedException {
        ensurePreconditions();
        Preconditions.checkArgument(!targetHandle.isReadOnly(), "Cannot concat using a read-only handle.");
        AtomicLong newLength = new AtomicLong();
            StreamSegmentData sourceData = getStreamSegmentData(sourceSegment);
            StreamSegmentData targetData = getStreamSegmentData(targetHandle.getSegmentName());
            targetData.concat(sourceData, offset);
            deleteInternal(new InMemorySegmentHandle(sourceSegment, false));
            newLength.set(targetData.getInfo().getLength());

        this.triggerExecutor.execute(() -> {
            fireOffsetTriggers(targetHandle.getSegmentName(), newLength.get());
            fireSealTrigger(sourceSegment);
        });
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentNotExistsException {
        ensurePreconditions();

        // If we are given a read-only handle, we must ensure the segment is sealed. If the segment can accept modifications
        // (it is not sealed), then we require a read-write handle.
        boolean canDelete = !handle.isReadOnly();
        if (!canDelete) {
            synchronized (this.lock) {
                if (this.streamSegments.containsKey(handle.getSegmentName())) {
                    canDelete = this.streamSegments.get(handle.getSegmentName()).isSealed();
                }
            }
        }

        Preconditions.checkArgument(canDelete, "Cannot delete using a read-only handle, unless the segment is sealed.");
        deleteInternal(handle);
    }

    @Override
    public void truncate(SegmentHandle handle, long offset) throws StreamSegmentNotExistsException {
        ensurePreconditions();
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot truncate using a read-only handle.");
        getStreamSegmentData(handle.getSegmentName()).truncate(offset);
    }

    @Override
    public boolean supportsTruncation() {
        return true;
    }

    /**
     * Appends the given data to the end of the StreamSegment.
     * Note: since this is a custom operation exposed only on InMemoryStorage, there is no need to make it return a Future.
     *
     * @param handle A read-write handle for the segment to append to.
     * @param data   An InputStream representing the data to append.
     * @param length The length of the data to append.
     */
    @SneakyThrows(StreamSegmentException.class)
    public void append(SegmentHandle handle, InputStream data, int length) {
        ensurePreconditions();
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot append using a read-only handle.");
        getStreamSegmentData(handle.getSegmentName()).append(data, length);
        this.triggerExecutor.execute(() -> {
            try {
                long segmentLength = getStreamSegmentData(handle.getSegmentName()).getInfo().getLength();
                fireOffsetTriggers(handle.getSegmentName(), segmentLength);
            } catch (StreamSegmentNotExistsException ex) {
                //Ignore.
            }
        });
    }

    /**
     * Changes the current owner of the Storage Adapter. After calling this, all calls to existing Segments will fail
     * until open() is called again on them.
     */
    public void changeOwner() {
        this.currentOwnerId.incrementAndGet();
    }

    private StreamSegmentData getStreamSegmentData(String streamSegmentName) throws StreamSegmentNotExistsException {
        synchronized (this.lock) {
            StreamSegmentData data = this.streamSegments.getOrDefault(streamSegmentName, null);
            if (data == null) {
                throw new StreamSegmentNotExistsException(streamSegmentName);
            }

            return data;
        }
    }

    private void deleteInternal(SegmentHandle handle) throws StreamSegmentNotExistsException {
        synchronized (this.lock) {
            if (!this.streamSegments.containsKey(handle.getSegmentName())) {
                throw new StreamSegmentNotExistsException(handle.getSegmentName());
            }

            this.streamSegments.remove(handle.getSegmentName());
        }

        cancelTriggers(handle.getSegmentName());
    }

    //endregion

    //region Size & seal triggers

    @Override
    @SneakyThrows(StreamSegmentNotExistsException.class)
    public CompletableFuture<Void> registerSizeTrigger(String segmentName, long offset, Duration timeout) {
        CompletableFuture<Void> result;
        boolean newTrigger = false;
        synchronized (this.offsetTriggers) {
            HashMap<Long, CompletableFuture<Void>> segmentTriggers = this.offsetTriggers.getOrDefault(segmentName, null);
            if (segmentTriggers == null) {
                segmentTriggers = new HashMap<>();
                this.offsetTriggers.put(segmentName, segmentTriggers);
            }

            result = segmentTriggers.getOrDefault(offset, null);
            if (result == null) {
                result = createSizeTrigger(segmentName, offset, timeout);
                segmentTriggers.put(offset, result);
                newTrigger = true;
            }
        }

        if (newTrigger && !result.isDone()) {
            // Do the check now to see if we already exceed the trigger threshold.
            val si = getStreamSegmentInfo(segmentName);

            if (si.getLength() >= offset) {
                fireOffsetTriggers(segmentName, si.getLength());
            }
        }

        return result;
    }

    @Override
    @SneakyThrows(StreamSegmentNotExistsException.class)
    public CompletableFuture<Void> registerSealTrigger(String segmentName, Duration timeout) {
        CompletableFuture<Void> result;
        boolean newTrigger = false;
        synchronized (this.sealTriggers) {
            result = this.sealTriggers.getOrDefault(segmentName, null);
            if (result == null) {
                result = createSealTrigger(segmentName, timeout);
                this.sealTriggers.put(segmentName, result);
                newTrigger = true;
            }
        }

        if (newTrigger && !result.isDone()) {
            // Do the check now to see if we are already sealed.
            val si = getStreamSegmentInfo(segmentName);
            if (si.isSealed()) {
                fireSealTrigger(segmentName);
            }
        }

        return result;
    }

    private void fireOffsetTriggers(String segmentName, long currentOffset) {
        val toTrigger = new ArrayList<CompletableFuture<Void>>();
        synchronized (this.offsetTriggers) {
            HashMap<Long, CompletableFuture<Void>> segmentTriggers = this.offsetTriggers.getOrDefault(segmentName, null);
            if (segmentTriggers != null) {
                segmentTriggers.entrySet().forEach(e -> {
                    if (e.getKey() <= currentOffset) {
                        toTrigger.add(e.getValue());
                    }
                });
            }
        }

        toTrigger.forEach(c -> c.complete(null));
    }

    private void fireSealTrigger(String segmentName) {
        CompletableFuture<Void> toTrigger;
        synchronized (this.sealTriggers) {
            toTrigger = this.sealTriggers.getOrDefault(segmentName, null);
        }

        if (toTrigger != null) {
            toTrigger.complete(null);
        }
    }

    private void cancelOffsetTriggersDueToSeal(String segmentName) {
        val toCancel = new ArrayList<CompletableFuture<Void>>();
        synchronized (this.offsetTriggers) {
            HashMap<Long, CompletableFuture<Void>> segmentTriggers = this.offsetTriggers.getOrDefault(segmentName, null);
            if (segmentTriggers != null) {
                toCancel.addAll(segmentTriggers.values());
            }
        }

        val exception = new StreamSegmentSealedException(segmentName);
        toCancel.forEach(c -> c.completeExceptionally(exception));
    }

    private void cancelTriggers(String segmentName) {
        val toCancel = new ArrayList<CompletableFuture>();
        synchronized (this.sealTriggers) {
            val trigger = this.sealTriggers.remove(segmentName);
            if (trigger != null) {
                toCancel.add(trigger);
            }
        }

        synchronized (this.offsetTriggers) {
            val trigger = this.offsetTriggers.remove(segmentName);
            if (trigger != null) {
                toCancel.addAll(trigger.values());
            }
        }

        toCancel.forEach(c -> c.cancel(true));
    }

    private CompletableFuture<Void> createSizeTrigger(String segmentName, long minSize, Duration timeout) {
        CompletableFuture<Void> result = FutureHelpers.futureWithTimeout(timeout, segmentName, this.triggerExecutor);
        result.whenComplete((r, ex) -> {
            synchronized (this.offsetTriggers) {
                HashMap<Long, CompletableFuture<Void>> segmentTriggers = this.offsetTriggers.getOrDefault(segmentName, null);
                if (segmentTriggers != null) {
                    segmentTriggers.remove(minSize);

                    if (segmentTriggers.size() == 0) {
                        this.offsetTriggers.remove(segmentName);
                    }
                }
            }
        });

        return result;
    }

    private CompletableFuture<Void> createSealTrigger(String segmentName, Duration timeout) {
        CompletableFuture<Void> result = FutureHelpers.futureWithTimeout(timeout, segmentName, this.triggerExecutor);
        result.whenComplete((r, ex) -> {
            synchronized (this.sealTriggers) {
                this.sealTriggers.remove(segmentName);
            }
        });

        return result;
    }

    private void ensurePreconditions() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.initialized.get(), "InMemoryStorage is not initialized.");
    }

    //endregion

    //region StreamSegmentData

    private static class StreamSegmentData {
        private static final int BUFFER_SIZE = 16 * 1024;
        private final String name;
        @GuardedBy("lock")
        private final ArrayList<byte[]> data;
        private final Object lock = new Object();
        private final SyncContext context;
        @GuardedBy("lock")
        private long currentOwnerId;
        @GuardedBy("lock")
        private long length;
        @GuardedBy("lock")
        private boolean sealed;
        @GuardedBy("lock")
        private long truncateOffset;
        @GuardedBy("lock")
        private int firstBufferOffset;

        StreamSegmentData(String name, SyncContext context) {
            this.name = name;
            this.data = new ArrayList<>();
            this.length = 0;
            this.sealed = false;
            this.context = context;
            this.currentOwnerId = Long.MIN_VALUE;
            this.truncateOffset = 0;
            this.firstBufferOffset = 0;
        }

        SegmentHandle openWrite() {
            synchronized (this.lock) {
                // Get the current InMemoryStorageAdapter owner id and keep track of it; it will be used for validation.
                this.currentOwnerId = this.context.getCurrentOwnerId.get();
                return new InMemorySegmentHandle(this.name, this.sealed);
            }
        }

        SegmentHandle openRead() {
            // No need to acquire any locks.
            return new InMemorySegmentHandle(this.name, true);
        }

        void write(long startOffset, InputStream data, int length) throws BadOffsetException, StreamSegmentSealedException {
            synchronized (this.lock) {
                checkOpened();
                writeInternal(startOffset, data, length);
            }
        }

        @SneakyThrows(BadOffsetException.class)
        void append(InputStream data, int length) throws StreamSegmentSealedException {
            synchronized (this.lock) {
                write(this.length, data, length);
            }
        }

        int read(long startOffset, byte[] target, int targetOffset, int length) {
            synchronized (this.lock) {
                Exceptions.checkArrayRange(targetOffset, length, target.length, "targetOffset", "length");
                Exceptions.checkArrayRange(startOffset, length, this.length, "startOffset", "length");
                Preconditions.checkArgument(startOffset >= this.truncateOffset, "startOffset (%s) is before the truncation offset (%s).", startOffset, this.truncateOffset);

                long offset = startOffset;
                int readBytes = 0;
                while (readBytes < length) {
                    OffsetLocation ol = getOffsetLocation(offset);
                    int bytesToCopy = Math.min(BUFFER_SIZE - ol.bufferOffset, length - readBytes);
                    System.arraycopy(this.data.get(ol.bufferSequence), ol.bufferOffset, target, targetOffset + readBytes, bytesToCopy);

                    readBytes += bytesToCopy;
                    offset += bytesToCopy;
                }

                return readBytes;
            }
        }

        void markSealed() {
            synchronized (this.lock) {
                checkOpened();
                this.sealed = true;
            }
        }

        boolean isSealed() {
            synchronized (this.lock) {
                return this.sealed;
            }
        }

        void concat(StreamSegmentData other, long offset) throws BadOffsetException, StreamSegmentSealedException {
            synchronized (this.context.syncRoot) {
                // In order to do a proper concat, we need to lock on both the source and the target segments. But since
                // there's always a possibility of two concurrent calls to concat with swapped arguments, there is a chance
                // this could deadlock in certain scenarios. One way to avoid that is to ensure that only one call to concat()
                // can be in progress at any given time (for any instance of InMemoryStorage), thus the need to synchronize
                // on SyncContext.syncRoot.
                synchronized (other.lock) {
                    Preconditions.checkState(other.sealed, "Cannot concat segment '%s' into '%s' because it is not sealed.", other.name, this.name);
                    Preconditions.checkState(other.truncateOffset == 0, "Cannot concat segment '%s' into '%s' because it is truncated.", other.name, this.name);
                    other.checkOpened();
                    synchronized (this.lock) {
                        checkOpened();
                        if (offset != this.length) {
                            throw new BadOffsetException(this.name, this.length, offset);
                        }

                        long bytesCopied = 0;
                        int currentBlockIndex = 0;
                        while (bytesCopied < other.length) {
                            byte[] currentBlock = other.data.get(currentBlockIndex);
                            int length = (int) Math.min(currentBlock.length, other.length - bytesCopied);
                            ByteArrayInputStream bis = new ByteArrayInputStream(currentBlock, 0, length);
                            writeInternal(this.length, bis, length);
                            bytesCopied += length;
                            currentBlockIndex++;
                        }
                    }
                }
            }
        }

        void truncate(long offset) {
            synchronized (this.lock) {
                Preconditions.checkArgument(offset >= 0 && offset <= this.length, "Offset (%s) must be non-negative and less than or equal to the Segment's length (%s).", offset, this.length);

                // Adjust the 'firstBufferOffset' to point to the first byte that will not be truncated after this is done.
                this.firstBufferOffset += offset - this.truncateOffset;

                // Trim away, from the beginning, all data buffers until we can no longer trim.
                while (this.firstBufferOffset >= BUFFER_SIZE && this.data.size() > 0) {
                    this.data.remove(0);
                    this.firstBufferOffset -= BUFFER_SIZE;
                }

                assert this.firstBufferOffset < BUFFER_SIZE : "Not all bytes were correctly truncated";
                this.truncateOffset = offset;
            }
        }

        SegmentProperties getInfo() {
            synchronized (this.lock) {
                return StreamSegmentInformation.builder().name(this.name).length(this.length).sealed(this.sealed).build();
            }
        }

        @GuardedBy("lock")
        private void ensureAllocated(long startOffset, int length) {
            long endOffset = startOffset + length;
            int desiredSize = getOffsetLocation(endOffset).bufferSequence + 1;
            while (this.data.size() < desiredSize) {
                this.data.add(new byte[BUFFER_SIZE]);
            }
        }

        @GuardedBy("lock")
        private OffsetLocation getOffsetLocation(long offset) {
            // Adjust for truncation offset and first buffer offset.
            offset += this.firstBufferOffset - this.truncateOffset;
            return new OffsetLocation((int) (offset / BUFFER_SIZE), (int) (offset % BUFFER_SIZE));
        }

        @GuardedBy("lock")
        @SneakyThrows(IOException.class)
        private void writeInternal(long startOffset, InputStream data, int length) throws BadOffsetException, StreamSegmentSealedException {
            Exceptions.checkArgument(length >= 0, "length", "bad length");
            if (startOffset != this.length) {
                throw new BadOffsetException(this.name, this.length, startOffset);
            }

            if (this.sealed) {
                throw new StreamSegmentSealedException(this.name);
            }

            long offset = startOffset;
            ensureAllocated(offset, length);

            int writtenBytes = 0;
            while (writtenBytes < length) {
                OffsetLocation ol = getOffsetLocation(offset);
                int readBytes = data.read(this.data.get(ol.bufferSequence), ol.bufferOffset, Math.min(length, BUFFER_SIZE - ol.bufferOffset));
                if (readBytes < 0) {
                    throw new IOException("reached end of stream while still expecting data");
                }

                writtenBytes += readBytes;
                offset += readBytes;
            }

            this.length = Math.max(this.length, startOffset + length);
        }

        @GuardedBy("lock")
        @SneakyThrows(StorageNotPrimaryException.class)
        private void checkOpened() {
            if (this.currentOwnerId != this.context.getCurrentOwnerId.get()) {
                throw new StorageNotPrimaryException(this.name);
            }
        }

        @Override
        public String toString() {
            return String.format("%s: Length = %d, Sealed = %s", this.name, this.length, this.sealed);
        }

        @Data
        private static class OffsetLocation {
            final int bufferSequence;
            final int bufferOffset;
        }
    }

    @Data
    private static class SyncContext {
        final Supplier<Long> getCurrentOwnerId;
        final Object syncRoot = new Object();
    }

    //endregion

    @Data
    private static class InMemorySegmentHandle implements SegmentHandle {
        private final String segmentName;
        private final boolean readOnly;
    }
}