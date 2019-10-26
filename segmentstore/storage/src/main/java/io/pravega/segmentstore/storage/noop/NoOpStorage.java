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

import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.NameUtils.INTERNAL_NAME_PREFIX;

/**
 * Storage adapter for testing scenario without interference from tier 2.
 *
 * . When this storage is used, all system segments are written to system storage.
 * . If userStorage is present, user data segments are written to it; otherwise no-op.
 * . If userStorage is present, user data segments are read from userStorage; otherwise UnsupportedOperationException thrown.
 * . For no-oped write operation, latency is applied in order to make the no-op as real as possible.
 */
@Slf4j
class NoOpStorage implements SyncStorage {

    // Storage for system segments; must not null.
    private final SyncStorage systemStorage;
    // Optional storage for user data segments, for testing of NoOpStorage only.
    private final SyncStorage userStorage;
    // latency in milliseconds to be applied to no-oped write operation.
    private final int writeNoOpLatencyMill;

    /**
     * Creates a new instance of the NoOpStorage class.
     *
     * @param config   The configuration to use.
     */
    NoOpStorage(StorageExtraConfig config, SyncStorage systemStorage, SyncStorage userStorage) {
        Preconditions.checkNotNull(config, "config");
        this.writeNoOpLatencyMill = config.getStorageWriteNoOpLatencyMill();
        this.systemStorage = Preconditions.checkNotNull(systemStorage, "systemStorage");
        this.userStorage = userStorage;
    }

    @Override
    public void close() {
        systemStorage.close();
        if (userStorage != null) {
            userStorage.close();
        }
    }

    @Override
    public void initialize(long epoch) {
        systemStorage.initialize(epoch);
        if (userStorage != null) {
            userStorage.initialize(1);
        }
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return systemStorage.getStreamSegmentInfo(streamSegmentName);
        } else {
            if (userStorage != null) {
                return userStorage.getStreamSegmentInfo(streamSegmentName);
            } else {
                throw new UnsupportedOperationException("getStreamSegmentInfo() for user segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public boolean exists(String streamSegmentName) {
        if (isSystemSegment(streamSegmentName)) {
            return systemStorage.exists(streamSegmentName);
        } else {
            if (userStorage != null) {
                return userStorage.exists(streamSegmentName);
            } else {
                throw new UnsupportedOperationException("exists() for user segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            return systemStorage.read(handle, offset, buffer, bufferOffset, length);
        } else {
            if (userStorage != null) {
                return userStorage.read(handle, offset, buffer, bufferOffset, length);
            } else {
                throw new UnsupportedOperationException("read() of user segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return systemStorage.openRead(streamSegmentName);
        } else {
            if (userStorage != null) {
                return userStorage.openRead(streamSegmentName);
            } else {
                throw new UnsupportedOperationException("openRead() of user segment is not supported in NO-OP mode.");
            }
        }
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            systemStorage.seal(handle);
        } else {
            if (userStorage != null) {
                userStorage.seal(handle);
            } else {
                noOp();
            }
        }
    }

    @Override
    public void unseal(SegmentHandle handle) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            systemStorage.unseal(handle);
        } else {
            if (userStorage != null) {
                userStorage.unseal(handle);
            } else {
                noOp();
            }
        }
    }

    @Override
    public void concat(SegmentHandle target, long offset, String sourceSegment) throws StreamSegmentException {
        if (isSystemSegment(target.getSegmentName())) {
            systemStorage.concat(target, offset, sourceSegment);
        } else {
            if (userStorage != null) {
                userStorage.concat(target, offset, sourceSegment);
            } else {
                noOp();
            }
        }
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            systemStorage.delete(handle);
        } else {
            if (userStorage != null) {
                userStorage.delete(handle);
            } else {
                noOp();
            }
        }
    }

    @Override
    public void truncate(SegmentHandle handle, long offset) throws StreamSegmentException {
        if (isSystemSegment(handle.getSegmentName())) {
            systemStorage.truncate(handle, offset);
        } else {
            if (userStorage != null) {
                userStorage.truncate(handle, offset);
            } else {
                noOp();
            }
        }
    }

    @Override
    public boolean supportsTruncation() {
        return systemStorage.supportsTruncation();
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
            systemStorage.write(handle, offset, data, length);
        } else {
            if (userStorage != null) {
                userStorage.write(handle, offset, data, length);
            } else {
                noOp();
            }
        }
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return systemStorage.openWrite(streamSegmentName);
        } else {
            if (userStorage != null) {
                return userStorage.openWrite(streamSegmentName);
            } else {
                return new NoOpSegmentHandle(streamSegmentName);
            }
        }
    }

    @Override
    public SegmentHandle create(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return systemStorage.create(streamSegmentName);
        } else {
            if (userStorage != null) {
                return userStorage.create(streamSegmentName);
            } else {
                return new NoOpSegmentHandle(streamSegmentName);
            }
        }
    }

    private boolean isSystemSegment(String segmentName) {
        Preconditions.checkNotNull(segmentName);
        return segmentName.startsWith(INTERNAL_NAME_PREFIX);
    }

    private void noOp() {
        Uninterruptibles.sleepUninterruptibly(this.writeNoOpLatencyMill, TimeUnit.MILLISECONDS);
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
