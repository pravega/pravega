/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.noop;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.NameUtils.INTERNAL_NAME_PREFIX;

/**
 * Storage adapter for testing scenario without interference from storage.
 *
 * When this storage adapter is used, all system segments are written to the underlying base storage.
 *
 * However, in the unit test of NoOpStorage, it is essential to read the supposedly no-oped segments in order to ensure
 * the no-op is done properly. storageForNoOp can be supplied in this case, and then:
 * . user segments are written to it instead of being no-oped.
 * . user segments are read from it instead of throwing UnsupportedOperationException.
 *
 * For no-oped write operation, latency is applied in order to make the no-op as real as possible.
 */
@Slf4j
class NoOpStorage implements SyncStorage {

    /*
     *  Base storage for the adapter, must not null.
     */
    private final SyncStorage baseStorage;
    /*
     * Optional storage to store those supposedly no-oped segments, in case the verification of no-oped segment is essential.
     * This is only used for the unit test of NoOpStorage itself.
     */
    private final SyncStorage storageForNoOp;
    /*
     * latency in milliseconds to be applied to no-oped write operation.
     */
    private final int writeNoOpLatencyMills;

    /**
     * Creates a new instance of the NoOpStorage class.
     *
     * @param config   The configuration to use.
     */
    NoOpStorage(StorageExtraConfig config, SyncStorage baseStorage, SyncStorage storageForNoOp) {
        Preconditions.checkNotNull(config, "config");
        this.writeNoOpLatencyMills = config.getStorageWriteNoOpLatencyMillis();
        this.baseStorage = Preconditions.checkNotNull(baseStorage, "baseStorage");
        this.storageForNoOp = storageForNoOp;
    }

    @Override
    public void close() {
        baseStorage.close();
        if (storageForNoOp != null) {
            storageForNoOp.close();
        }
    }

    @Override
    public void initialize(long epoch) {
        baseStorage.initialize(epoch);
        if (storageForNoOp != null) {
            storageForNoOp.initialize(epoch);
        }
    }

    private interface ThrowingFunction<Storage, Result, E extends Throwable> {
        Result accept(Storage storage) throws E;
    }

    private interface NoOpFunction<Storage, E extends Throwable> {
        void accept(Storage storage) throws E;
    }

    private <R> R delegate(String segmentName, ThrowingFunction<SyncStorage, R, StreamSegmentException> f,
                           String errorMessage) throws StreamSegmentException {
        if (isSystemSegment(segmentName)) {
            return f.accept(this.baseStorage);
        } else {
            if (storageForNoOp != null) {
                return f.accept(this.storageForNoOp);
            } else {
                throw new UnsupportedOperationException(errorMessage);
            }
        }
    }

    private <R> R delegateNoCheckedException(String segmentName, Function<SyncStorage, R> f, String errorMessage) {
        if (isSystemSegment(segmentName)) {
            return f.apply(this.baseStorage);
        } else {
            if (storageForNoOp != null) {
                return f.apply(this.storageForNoOp);
            } else {
                throw new UnsupportedOperationException(errorMessage);
            }
        }
    }

    private void delegateNoOp(String segmentName, NoOpFunction<SyncStorage, StreamSegmentException> f) throws StreamSegmentException {
        if (isSystemSegment(segmentName)) {
            f.accept(this.baseStorage);
        } else {
            if (storageForNoOp != null) {
                f.accept(this.storageForNoOp);
            } else {
                noOp();
            }
        }
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return this.baseStorage.getStreamSegmentInfo(streamSegmentName);
        } else {
            if (this.storageForNoOp != null) {
                return storageForNoOp.getStreamSegmentInfo(streamSegmentName);
            } else {
                return StreamSegmentInformation.builder().name(streamSegmentName).build();
            }
        }
    }

    @Override
    public boolean exists(String streamSegmentName) {
        return delegateNoCheckedException(streamSegmentName, storage -> storage.exists(streamSegmentName), "exists() for user segment is not supported in NO-OP mode.");
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        return delegate(handle.getSegmentName(), storage -> storage.read(handle, offset, buffer, bufferOffset, length),
                "read() of user segment is not supported in NO-OP mode.");
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return this.baseStorage.openRead(streamSegmentName);
        } else {
            if (this.storageForNoOp != null) {
                return storageForNoOp.openRead(streamSegmentName);
            } else {
                return new NoOpSegmentHandle(streamSegmentName);
            }
        }
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        delegateNoOp(handle.getSegmentName(), storage -> storage.seal(handle));
    }

    @Override
    public void unseal(SegmentHandle handle) throws StreamSegmentException {
        delegateNoOp(handle.getSegmentName(), storage -> storage.unseal(handle));
    }

    @Override
    public void concat(SegmentHandle target, long offset, String sourceSegment) throws StreamSegmentException {
        delegateNoOp(target.getSegmentName(), storage -> storage.concat(target, offset, sourceSegment));
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        delegateNoOp(handle.getSegmentName(), storage -> storage.delete(handle));
    }

    @Override
    public void truncate(SegmentHandle handle, long offset) throws StreamSegmentException {
        delegateNoOp(handle.getSegmentName(), storage -> storage.truncate(handle, offset));
    }

    @Override
    public boolean supportsTruncation() {
        return baseStorage.supportsTruncation();
    }

    @Override
    public Iterator<SegmentProperties> listSegments() throws IOException {
        return baseStorage.listSegments();
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
        delegateNoOp(handle.getSegmentName(), storage -> storage.write(handle, offset, data, length));
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        if (isSystemSegment(streamSegmentName)) {
            return baseStorage.openWrite(streamSegmentName);
        } else {
            if (storageForNoOp != null) {
                return storageForNoOp.openWrite(streamSegmentName);
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
            if (storageForNoOp != null) {
                return storageForNoOp.create(streamSegmentName);
            } else {
                return new NoOpSegmentHandle(streamSegmentName);
            }
        }
    }

    /**
     * Check whether the given segment is internal system segment (including table segment)
     *
     * @param segmentName which may not be qualified (containing scope name) in test codes.
     * @return
     */
    private boolean isSystemSegment(String segmentName) {
        Preconditions.checkNotNull(segmentName);
        return segmentName.startsWith(INTERNAL_NAME_PREFIX) || segmentName.contains("_tables");
    }

    private void noOp() {
        Uninterruptibles.sleepUninterruptibly(this.writeNoOpLatencyMills, TimeUnit.MILLISECONDS);
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
