package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.TruncateableStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.val;

public class RollingStorage implements Storage, TruncateableStorage {
    //region Members

    @Getter
    private final Storage baseStorage;
    private final RollingPolicy defaultRollingPolicy;
    private final Executor executor;
    private final AtomicBoolean closed;

    //endregion

    //region RollingStorage

    /**
     * Creates a new instance of the RollingStorage class.
     *
     * @param baseStorageFactory   A StorageFactory that will be used to instantiate the inner Storage implementation,
     *                             on top of which this RollingStorage will operate.
     * @param defaultRollingPolicy A RollingPolicy to apply to every StreamSegment that does not have its own policy
     *                             defined.
     * @param executor             An Executor to run async tasks on.
     */
    public RollingStorage(StorageFactory baseStorageFactory, RollingPolicy defaultRollingPolicy, Executor executor) {
        Preconditions.checkNotNull(baseStorageFactory, "baseStorageFactory");
        this.defaultRollingPolicy = Preconditions.checkNotNull(defaultRollingPolicy, "defaultRollingPolicy");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.baseStorage = baseStorageFactory.createStorageAdapter();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.baseStorage.close();
        }
    }

    //endregion

    //region ReadOnlyStorage Implementation

    @Override
    public void initialize(long containerEpoch) {
        this.baseStorage.initialize(containerEpoch);
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return null;
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return null;
    }

    //endregion

    //region Storage Implementation

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return create(streamSegmentName, this.defaultRollingPolicy, timeout);
    }

    /**
     * Creates a new StreamSegment with given RollingPolicy.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param rollingPolicy     The Rolling Policy to apply to this StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the StreamSegment has been created (and will
     * contain a StreamSegmentInformation for an empty Segment). If the operation failed, it will contain the cause of the
     * failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentExistsException: When the given Segment already exists in Storage.
     * </ul>
     */
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, RollingPolicy rollingPolicy, Duration timeout) {
        // Create Header File. If Failed, re-throw immediately.
        // Delete any existing data files - This may not work because we can't do listing.
        return null;
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        // OpenWrite Header file (fence it out).
        // Read contents of Header file and stash it into the Handle.
        // OpenWrite last file (active file).
        return null;
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        // If no active file, rollover (create new one).
        // Write to active file.
        // If active file length > Policy.MaxLength, rollover.
        return null;
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        // Seal Active file.
        // Seal Header file.
        // (update handle)
        return null;
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        // OpenWrite source and Check source sealed.
        // Verify source not truncated.
        // Append contents of Source Handle (files) to targetHandle (files) and update header file. We do not rename source
        // files.
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        // Delete all files, in order (truncate each other out).
        // Delete header file.
        return null;
    }

    //endregion

    //region TruncateableStorage Implementation

    @Override
    public CompletableFuture<Void> truncate(String streamSegmentName, long offset, Duration timeout) {
        // Delete all files, in order, as long as their last offset < truncation offset.
        return null;
    }

    //endregion

    private CompletableFuture<RollingStorageHandle> open(String streamSegmentName, boolean readOnly, Duration timeout) {
        String headerSegment = StreamSegmentNameUtils.getHeaderSegmentName(streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.baseStorage
                .getStreamSegmentInfo(headerSegment, timer.getRemaining())
                .thenComposeAsync(si -> readHeader(si, readOnly, timer.getRemaining()), this.executor)
                .thenComposeAsync(handle -> openActiveSubSegment(handle, timer.getRemaining()), this.executor);
    }

    private CompletableFuture<RollingStorageHandle> readHeader(SegmentProperties si, boolean readOnly, Duration timeout) {
        byte[] readBuffer = new byte[(int) si.getLength()];
        val openHeader = readOnly ? this.baseStorage.openRead(si.getName()) : this.baseStorage.openWrite(si.getName());
        return openHeader.thenComposeAsync(headerHandle ->
                        this.baseStorage.read(headerHandle, 0, readBuffer, 0, readBuffer.length, timeout)
                                        .thenApplyAsync(v -> {
                                            String s = new String(readBuffer, Charsets.UTF_8);
                                            RollingStorageHandle handle = HandleSerializer.deserialize(s, headerHandle);
                                            if (si.isSealed()) {
                                                handle.markSealed();
                                            }

                                            return handle;
                                        }, this.executor),
                this.executor);
    }

    private CompletableFuture<RollingStorageHandle> openActiveSubSegment(RollingStorageHandle handle, Duration timeout) {
        // For all but the last SubSegment we can infer the lengths by doing some simple arithmetic.
        val previous = new AtomicReference<RollingStorageHandle.SubSegment>();
        handle.forEachSubSegment(s -> {
            RollingStorageHandle.SubSegment p = previous.getAndSet(s);
            if (p != null) {
                p.setLength(s.getStartOffset() - p.getStartOffset());
                p.markSealed();
            }

            previous.set(s);
        });

        // For the last one, we need to actually check the file.
        RollingStorageHandle.SubSegment activeSubSegment = handle.getLastSubSegment();
        if (activeSubSegment != null) {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.baseStorage.getStreamSegmentInfo(activeSubSegment.getName(), timer.getRemaining())
                                   .thenComposeAsync(si -> {
                                       activeSubSegment.setLength(si.getLength());
                                       if (si.isSealed()) {
                                           activeSubSegment.markSealed();
                                           return CompletableFuture.completedFuture(null);
                                       } else {
                                           return this.baseStorage.openWrite(activeSubSegment.getName());
                                       }
                                   })
                                   .thenApply(activeHandle -> {
                                       handle.setActiveSubSegmentHandle(activeHandle);
                                       return handle;
                                   });
            // TODO: finish this up.
        } else {
            // No SubSegments - return as is.
            return CompletableFuture.completedFuture(handle);
        }
    }
}
