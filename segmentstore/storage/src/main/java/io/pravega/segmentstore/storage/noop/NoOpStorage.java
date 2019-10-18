/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.noop;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;

/**
 * Storage adapter for testing scenario without interference from tier 2.
 *
 * All the writes to tier 2 are no-oped unless its for metadata (system segments).
 * Reads are not supported.
 * Can also add latency to write operation to make the no-oped write as close to real as possible.
 */
@Slf4j
class NoOpStorage implements SyncStorage {

    //Hidden in memory storage to verify the correctness of no-operation.
    private final SyncStorage noopInMemoryStorage;

    private final SyncStorage baseStorage;
    private final int writeNoOpLatencyMill;

    /**
     * Creates a new instance of the NoOpStorage class.
     *
     * @param config   The configuration to use.
     */
    NoOpStorage(StorageExtraConfig config, SyncStorage baseStorage) {
        Preconditions.checkNotNull(config, "config");
        this.writeNoOpLatencyMill = config.getStorageWriteNoOpLatencyMill();
        this.baseStorage = Preconditions.checkNotNull(baseStorage, "baseStorage");

        noopInMemoryStorage = new InMemoryStorage();
    }

    @Override
    public void close() {
        baseStorage.close();
        if (noopInMemoryStorage != null) {
            noopInMemoryStorage.close();
        }
    }

    @Override
    public void initialize(long epoch) {
        baseStorage.initialize(epoch);
        if (noopInMemoryStorage != null) {
            noopInMemoryStorage.initialize(1);
        }
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return baseStorage.getStreamSegmentInfo(streamSegmentName);
        } else {
            if (noopInMemoryStorage != null) {
                return noopInMemoryStorage.getStreamSegmentInfo(streamSegmentName);
            } else {
                throw new UnsupportedOperationException("getStreamSegmentInfo() for non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public boolean exists(String streamSegmentName) {
        if (isSystemSegment(streamSegmentName)) {
            return baseStorage.exists(streamSegmentName);
        } else {
            if (noopInMemoryStorage != null) {
                return noopInMemoryStorage.exists(streamSegmentName);
            } else {
                throw new UnsupportedOperationException("exists() for non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            return baseStorage.read(handle, offset, buffer, bufferOffset, length);
        } else {
            if (noopInMemoryStorage != null) {
                return noopInMemoryStorage.read(handle, offset, buffer, bufferOffset, length);
            } else {
                throw new UnsupportedOperationException("read() of non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return baseStorage.openRead(streamSegmentName);
        } else {
            if (noopInMemoryStorage != null) {
                return noopInMemoryStorage.openRead(streamSegmentName);
            } else {
                throw new UnsupportedOperationException("openRead() of non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            baseStorage.seal(handle);
        } else {
            if (noopInMemoryStorage != null) {
                noopInMemoryStorage.seal(handle);
            } else {
                throw new UnsupportedOperationException("seal() of non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public void unseal(SegmentHandle handle) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            baseStorage.unseal(handle);
        } else {
            if (noopInMemoryStorage != null) {
                noopInMemoryStorage.unseal(handle);
            } else {
                throw new UnsupportedOperationException("unseal() of non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public void concat(SegmentHandle target, long offset, String sourceSegment) throws StreamSegmentException {
        if (isSystemSegment(target.getSegmentName()) && isSystemSegment(sourceSegment)) {
            baseStorage.concat(target, offset, sourceSegment);
        } else {
            if (noopInMemoryStorage != null) {
                noopInMemoryStorage.concat(target, offset, sourceSegment);
            } else {
                throw new UnsupportedOperationException("concat() for non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            baseStorage.delete(handle);
        } else {
            if (noopInMemoryStorage != null) {
                noopInMemoryStorage.delete(handle);
            } else {
                throw new UnsupportedOperationException("delete() non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public void truncate(SegmentHandle handle, long offset) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            baseStorage.truncate(handle, offset);
        } else {
            if (noopInMemoryStorage != null) {
                noopInMemoryStorage.truncate(handle, offset);
            } else {
                throw new UnsupportedOperationException("truncate() non-system segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public boolean supportsTruncation() {
        return baseStorage.supportsTruncation();
    }

    /**
     *
     * @param handle A read-write SegmentHandle that points to a Segment to write to.
     * @param offset The offset in the StreamSegment to write data at.
     * @param data   An InputStream representing the data to write.
     * @param length The length of the InputStream.
     * @throws StreamSegmentException
     */
    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {

        if (isSystemSegment(handle.getSegmentName())) {
            baseStorage.write(handle, offset, data, length);
        } else {
            if (noopInMemoryStorage != null) {
                noopInMemoryStorage.write(handle, offset, data, length);
            } else {
                Uninterruptibles.sleepUninterruptibly(this.writeNoOpLatencyMill, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return baseStorage.openWrite(streamSegmentName);
        } else {
            if (noopInMemoryStorage != null) {
                return noopInMemoryStorage.openWrite(streamSegmentName);
            } else {
                return new NoOpSegmentHandle(streamSegmentName);
            }
        }
    }

    @Override
    public SegmentHandle create(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return baseStorage.create(streamSegmentName);
        } else {
            if (noopInMemoryStorage != null) {
                return noopInMemoryStorage.create(streamSegmentName);
            } else {
                return new NoOpSegmentHandle(streamSegmentName);
            }
        }
    }

    private boolean isSystemSegment(String segmentName) {
        Preconditions.checkNotNull(segmentName);
        return segmentName.startsWith(INTERNAL_SCOPE_NAME);
    }

    static class NoOpSegmentHandle implements SegmentHandle {

        private final String segmentName;

        public NoOpSegmentHandle(String segmentName) {
            this.segmentName = segmentName;
        }

        @Override
        public String getSegmentName() {
            return segmentName;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }
    }

}
