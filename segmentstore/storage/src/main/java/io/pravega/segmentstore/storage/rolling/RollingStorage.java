/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CollectionHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.TruncateableStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

public class RollingStorage implements Storage, TruncateableStorage {
    //region Members

    private static final Duration OPEN_TIMEOUT = Duration.ofSeconds(30);
    @Getter
    private final Storage baseStorage;
    private final SegmentRollingPolicy defaultRollingPolicy;
    private final Executor executor;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RollingStorage class.
     *
     * @param baseStorageFactory   A StorageFactory that will be used to instantiate the inner Storage implementation,
     *                             on top of which this RollingStorage will operate.
     * @param defaultRollingPolicy A SegmentRollingPolicy to apply to every StreamSegment that does not have its own policy
     *                             defined.
     * @param executor             An Executor to run async tasks on.
     */
    public RollingStorage(StorageFactory baseStorageFactory, SegmentRollingPolicy defaultRollingPolicy, Executor executor) {
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
    public CompletableFuture<SegmentHandle> openRead(String segmentName) {
        return open(segmentName, true, OPEN_TIMEOUT).thenApply(Function.identity());
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        val h = asReadableHandle(handle);
        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }

        TimeoutTimer timer = new TimeoutTimer(timeout);
        if (h.isReadOnly() && !h.isSealed() && offset + length > h.length()) {
            // We have a non-sealed read-only handle. It's possible that the SubSegments may have been modified since the
            // last time we refreshed it, and we received a request for a read beyond our last known offset. Reload the
            // handle before attempting the read.
            return open(handle.getSegmentName(), true, timer.getRemaining())
                    .thenCompose(newHandle -> {
                        h.refresh(newHandle);
                        return readInternal(h, offset, buffer, bufferOffset, length, timer);
                    });
        } else {
            return readInternal(h, offset, buffer, bufferOffset, length, timer);
        }
    }

    private CompletableFuture<Integer> readInternal(RollingSegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, TimeoutTimer timer) {
        if (offset + length > handle.length()) {
            // We can't read past the end of the Segment.
            return FutureHelpers.failedFuture(new IllegalArgumentException(
                    String.format("Offset %d + length %d is beyond the last offset %d of the segment.", offset, length, handle.length())));
        }

        // Read in a loop, from each SubSegment, until we can't read anymore.
        // If at any point we encounter a StreamSegmentNotExistsException, fail immediately with StreamSegmentTruncatedException (+inner).
        val subSegments = handle.subSegments();
        AtomicInteger currentIndex = new AtomicInteger(
                CollectionHelpers.binarySearch(subSegments, s -> offset < s.getStartOffset() ? -1 : (offset >= s.getLastOffset() ? 1 : 0)));
        assert currentIndex.get() >= 0 : "unable to locate first SubSegment index.";

        AtomicInteger bytesRead = new AtomicInteger();
        val r = FutureHelpers.loop(
                () -> bytesRead.get() < length && currentIndex.get() < subSegments.size(),
                () -> {
                    // Verify if this is a known truncated SubSegment; if so, bail out quickly.
                    SubSegment current = subSegments.get(currentIndex.get());
                    checkTruncatedSegment(null, handle, current);
                    long readOffset = offset + bytesRead.get() - current.getStartOffset();
                    int readLength = (int) Math.min(length - bytesRead.get(), current.getLength() - readOffset);
                    assert readOffset >= 0 && readLength >= 0 : "negative readOffset or readLength";

                    // Read from the actual SubSegment into the given buffer.
                    return this.baseStorage
                            .openRead(current.getName())
                            .thenCompose(sh -> this.baseStorage.read(sh, readOffset, buffer, bufferOffset + bytesRead.get(), readLength, timer.getRemaining()))
                            .handle((count, ex) -> {
                                checkTruncatedSegment(ex, handle, current);
                                bytesRead.addAndGet(count);
                                if (readOffset + count >= current.getLength()) {
                                    currentIndex.incrementAndGet();
                                }
                                return null;
                            });
                },
                this.executor);
        return r.thenApply(v -> bytesRead.get());
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String segmentName, Duration timeout) {
        return open(segmentName, false, timeout)
                .thenApply(handle -> StreamSegmentInformation
                        .builder()
                        .name(handle.getSegmentName())
                        .sealed(handle.isSealed())
                        .length(handle.length())
                        .startOffset(0L) // TODO: we can't get this reliably without calling exists on all SubSegments.
                        .build());
    }

    @Override
    public CompletableFuture<Boolean> exists(String segmentName, Duration timeout) {
        // A Segment Exists only if its header file exists and is not empty.
        String headerName = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        return this.baseStorage.getStreamSegmentInfo(headerName, timeout)
                .thenApply(si -> si.getLength() > 0)
                .exceptionally(ex -> ignore(ex, StreamSegmentNotExistsException.class, false));

    }

    //endregion

    //region Storage Implementation

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return create(streamSegmentName, this.defaultRollingPolicy, timeout);
    }

    /**
     * Creates a new StreamSegment with given SegmentRollingPolicy.
     *
     * @param segmentName   The full name of the StreamSegment.
     * @param rollingPolicy The Rolling Policy to apply to this StreamSegment.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the StreamSegment has been created (and will
     * contain a StreamSegmentInformation for an empty Segment). If the operation failed, it will contain the cause of the
     * failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentExistsException: When the given Segment already exists in Storage.
     * </ul>
     */
    public CompletableFuture<SegmentProperties> create(String segmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        Preconditions.checkNotNull(rollingPolicy, "rollingPolicy");
        String headerFile = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Create the header file, and then serialize the contents to it.
        // If the header file already exists, then it's OK if it's empty (probably a remnant from a previously failed
        // attempt); in that case we ignore it and let the creation proceed.
        AtomicReference<SegmentHandle> headerHandle = new AtomicReference<>();
        return FutureHelpers.exceptionallyCompose(
                this.baseStorage
                        .create(headerFile, timer.getRemaining())
                        .handle((v2, ex) -> ignoreEmptyFile(ex, headerFile, timer.getRemaining()))
                        .thenCompose(ignored -> this.baseStorage.openWrite(headerFile))
                        .thenCompose(hh -> {
                            headerHandle.set(hh);
                            return serializeHandle(new RollingSegmentHandle(hh, rollingPolicy, Collections.emptyList()),
                                    timer.getRemaining());
                        }),
                ex -> {
                    // If we encountered an error while writing the handle file, delete it before returning the exception,
                    // otherwise we'll leave behind an empty file.
                    if (!(ExceptionHelpers.getRealException(ex) instanceof StreamSegmentExistsException)
                            && headerHandle.get() != null) {
                        return this.baseStorage.delete(headerHandle.get(), timer.getRemaining())
                                .thenCompose(v2 -> FutureHelpers.failedFuture(ex));
                    } else {
                        // Some other kind of exception - rethrow.
                        return FutureHelpers.failedFuture(ex);
                    }
                })
                .thenApply(v -> StreamSegmentInformation.builder().name(segmentName).build());
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String segmentName) {
        return open(segmentName, false, OPEN_TIMEOUT).thenApply(Function.identity());
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        val h = asWritableHandle(handle);
        if (h.isSealed()) {
            return FutureHelpers.failedFuture(new StreamSegmentSealedException(handle.getSegmentName()));
        }

        // We run this in a loop because we may have to split the write over multiple SubSegments in order to avoid exceeding
        // any SubSegment's maximum length.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        AtomicInteger writtenLength = new AtomicInteger();
        return FutureHelpers.loop(
                () -> writtenLength.get() < length,
                () -> {
                    long writeOffset = offset + writtenLength.get();
                    int writeLength = length - writtenLength.get();
                    if (h.getActiveSubSegmentHandle() == null || h.lastSubSegment().getLength() >= h.getRollingPolicy().getMaxLength()) {
                        return rollover(h, timer.getRemaining())
                                .thenCompose(v -> writeToActiveSubSegment(h, writeOffset, data, writeLength, timer.getRemaining()));
                    } else {
                        return writeToActiveSubSegment(h, writeOffset, data, writeLength, timer.getRemaining());
                    }
                },
                writtenLength::addAndGet,
                this.executor);

    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        val h = asWritableHandle(handle);
        if (h.isSealed()) {
            return FutureHelpers.failedFuture(new StreamSegmentSealedException(handle.getSegmentName()));
        }

        if (h.getActiveSubSegmentHandle() != null) {
            // Seal Active file, then Seal Header.
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.baseStorage
                    .seal(h.getActiveSubSegmentHandle(), timer.getRemaining())
                    .exceptionally(ex -> ignore(ex, StreamSegmentSealedException.class))
                    .thenCompose(v -> {
                        h.setActiveSubSegmentHandle(null).lastSubSegment().markSealed();
                        return sealHeader(h, timer.getRemaining());
                    });
        } else {
            // No active file, only need to seal the Header.
            return sealHeader(h, timeout);
        }
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long targetOffset, String sourceSegment, Duration timeout) {
        val th = asWritableHandle(targetHandle);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        AtomicReference<RollingSegmentHandle> sh = new AtomicReference<>();
        return open(sourceSegment, false, timer.getRemaining())
                .thenCompose(sourceHandle -> {
                    sh.set(sourceHandle);
                    // Verify target Segment (offset and not sealed).
                    if (targetOffset != th.length()) {
                        return FutureHelpers.failedFuture(new BadOffsetException(th.getSegmentName(), th.length(), targetOffset));
                    }

                    if (th.isSealed()) {
                        return FutureHelpers.failedFuture(new StreamSegmentSealedException(th.getSegmentName()));
                    }

                    // We can only use a Segment as a concat source if it is Sealed.
                    Preconditions.checkState(sourceHandle.isSealed(), "Cannot concat segment '%s' into '%s' because it is not sealed.",
                            sourceSegment, th.getSegmentName());
                    if (sourceHandle.length() == 0) {
                        //Source is empty; do not bother with concatenation.
                        return delete(sourceHandle, timer.getRemaining());
                    }

                    // We can only use a Segment as a concat source if all of its SubSegments exist.
                    return refreshSubSegmentExistence(sourceHandle, timer.getRemaining());
                })
                .thenCompose(v -> {
                    Preconditions.checkState(sh.get().subSegments().stream().allMatch(SubSegment::exists),
                            "Cannot use segment '%s' as concat source because it is truncated.", sh.get().getSegmentName());

                    // Generate new SubSegment entries from the SubSegments of the Source Segment(but update their start offsets).
                    List<SubSegment> newSubSegments = rebase(sh.get().subSegments(), th.length());

                    return sealActiveSubSegment(th, timer)
                            .thenCompose(v2 -> serializeBeginConcat(th, sh.get(), timer.getRemaining()))
                            .thenCompose(v2 -> this.baseStorage.concat(th.getHeaderHandle(), th.getHeaderLength(),
                                    sh.get().getHeaderHandle().getSegmentName(), timer.getRemaining()))
                            .thenRun(() -> th.increaseHeaderLength(sh.get().getHeaderLength()).addSubSegments(newSubSegments));
                });
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        // We need to seal the whole Segment to prevent anyone else from creating new SubSegments while we're deleting them,
        // after which we delete all SubSegments and finally the header file.
        val h = asWritableHandle(handle);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return seal(handle, timer.getRemaining())
                .exceptionally(ex -> ignore(ex, StreamSegmentSealedException.class))
                .thenCompose(v -> deleteSubSegments(h, s -> true, timer.getRemaining()))
                .thenCompose(v -> this.baseStorage.delete(h.getHeaderHandle(), timer.getRemaining()));
    }

    //endregion

    //region TruncateableStorage Implementation

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long truncationOffset, Duration timeout) {
        // Delete all SubSegments which are entirely before the truncation offset.
        val h = asWritableHandle(handle);
        val last = h.lastSubSegment();
        if (last != null && canTruncate(last, truncationOffset)) {
            // If we were asked to truncate the entire Segment, then rollover at this point so we can delete all existing
            // data.
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return rollover(h, timer.getRemaining())
                    .thenCompose(v -> deleteSubSegments(h, s -> canTruncate(s, truncationOffset), timeout));
        } else {
            return deleteSubSegments(h, s -> canTruncate(s, truncationOffset), timeout);
        }
    }

    //endregion

    //region SubSegment Operations

    /**
     * Opens the active SubSegment (last one) for the given Segment (if a non-readonly handle) and updates the handle
     * with necessary info (SubSegment lengths, seal status, etc.).
     */
    private CompletableFuture<RollingSegmentHandle> openActiveSubSegment(RollingSegmentHandle handle, Duration timeout) {
        // For all but the last SubSegment we can infer the lengths by doing some simple arithmetic.
        val previous = new AtomicReference<SubSegment>();
        handle.subSegments().forEach(s -> {
            SubSegment p = previous.getAndSet(s);
            if (p != null) {
                p.setLength(s.getStartOffset() - p.getStartOffset());
                p.markSealed();
            }

            previous.set(s);
        });

        // For the last one, we need to actually check the file.
        SubSegment activeSubSegment = handle.lastSubSegment();
        if (activeSubSegment != null) {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.baseStorage
                    .getStreamSegmentInfo(activeSubSegment.getName(), timer.getRemaining())
                    .thenCompose(si -> {
                        activeSubSegment.setLength(si.getLength());
                        if (si.isSealed()) {
                            // Last segment is Sealed, so we can't have a Write Handle for it.
                            activeSubSegment.markSealed();
                            return CompletableFuture.completedFuture(null);
                        } else {
                            // Open-Write the active SubSegment, and get its handle.
                            return this.baseStorage.openWrite(activeSubSegment.getName());
                        }
                    })
                    .thenApply(handle::setActiveSubSegmentHandle);
        } else {
            // No SubSegments - return as is.
            return CompletableFuture.completedFuture(handle);
        }
    }

    private CompletableFuture<Integer> writeToActiveSubSegment(RollingSegmentHandle handle, long segmentOffset, InputStream data, int length, Duration timeout) {
        if (segmentOffset != handle.length()) {
            return FutureHelpers.failedFuture(new BadOffsetException(handle.getSegmentName(), handle.length(), segmentOffset));
        }

        SubSegment last = handle.lastSubSegment();
        long subSegmentOffset = segmentOffset - last.getStartOffset();
        int writeLength = (int) Math.min(length, handle.getRollingPolicy().getMaxLength() - last.getLength());
        assert writeLength > 0 : "non-positive write length";
        return this.baseStorage.write(handle.getActiveSubSegmentHandle(), subSegmentOffset, data, writeLength, timeout)
                               .thenApply(v -> {
                                   last.increaseLength(writeLength);
                                   return writeLength;
                               });
    }

    private CompletableFuture<Void> createSubSegment(RollingSegmentHandle handle, Duration timeout) {
        // Create new active SubSegment, only after which serialize the handle update and update the handle.
        // We ignore if the SubSegment exists and is empty - that's most likely due to a previous failed attempt.
        long segmentLength = handle.length();
        SubSegment newSubSegment = SubSegment.forSegment(handle.getSegmentName(), segmentLength);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.baseStorage
                .create(newSubSegment.getName(), timer.getRemaining())
                .handle((v, ex) -> ignoreEmptyFile(ex, newSubSegment.getName(), timer.getRemaining()))
                .thenCompose(si -> serializeNewSubSegment(handle, newSubSegment, timer.getRemaining()))
                .thenCompose(v -> this.baseStorage.openWrite(newSubSegment.getName()))
                .thenAccept(activeHandle -> handle.addSubSegment(newSubSegment, activeHandle));
    }

    private CompletableFuture<Void> deleteSubSegments(RollingSegmentHandle handle, Predicate<SubSegment> canDelete, Duration timeout) {
        val deletionFutures = handle.subSegments().stream()
                                    .filter(s -> s.exists() && canDelete.test(s))
                                    .map(s -> deleteSubSegment(s, timeout))
                                    .collect(Collectors.toList());
        return FutureHelpers.allOf(deletionFutures);
    }

    private CompletableFuture<Void> deleteSubSegment(SubSegment subSegment, Duration timeout) {
        return this.baseStorage.openWrite(subSegment.getName())
                               .thenCompose(subHandle -> this.baseStorage.delete(subHandle, timeout))
                               .exceptionally(ex -> ignore(ex, StreamSegmentNotExistsException.class))
                               .thenRun(subSegment::markInexistent);
    }

    private CompletableFuture<Void> rollover(RollingSegmentHandle handle, Duration timeout) {
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot rollover using a read-only handle.");
        Preconditions.checkArgument(!handle.isSealed(), "Cannot rollover a Sealed Segment.");

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return sealActiveSubSegment(handle, timer)
                .thenCompose(v -> createSubSegment(handle, timer.getRemaining()));
    }

    private CompletableFuture<Void> sealActiveSubSegment(RollingSegmentHandle handle, TimeoutTimer timer) {
        if (handle.getActiveSubSegmentHandle() != null) {
            return this.baseStorage.seal(handle.getActiveSubSegmentHandle(), timer.getRemaining())
                                   .thenRun(() -> handle.setActiveSubSegmentHandle(null).lastSubSegment().markSealed());
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private boolean canTruncate(SubSegment subSegment, long truncationOffset) {
        // We should only truncate those SubSegments that are entirely before the truncationOffset. An empty SubSegment
        // that starts exactly at the truncationOffset should be spared (this means we truncate the entire Segment), as
        // we need that SubSegment to determine the actual length of the Segment.
        return subSegment.getStartOffset() < truncationOffset
                || subSegment.getLastOffset() <= truncationOffset;
    }

    private CompletableFuture<Void> refreshSubSegmentExistence(RollingSegmentHandle handle, Duration timeout) {
        // We check all SubSegments that we assume exist for actual existence (since once deleted, they can't come back).
        val futures = handle.subSegments().stream()
                            .filter(SubSegment::exists)
                            .map(s -> this.baseStorage.exists(s.getName(), timeout).thenAccept(e -> {
                                if (!e) {
                                    s.markInexistent();
                                }
                            }))
                            .collect(Collectors.toList());
        return FutureHelpers.allOf(futures);
    }

    //endregion

    //region Header Operations

    /**
     * Opens a RollingSegmentHandle for the given Segment.
     */
    private CompletableFuture<RollingSegmentHandle> open(String segmentName, boolean readOnly, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String headerSegment = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        return this.baseStorage.getStreamSegmentInfo(headerSegment, timer.getRemaining())
                .thenApply(si -> checkNonEmptyFile(si, segmentName))
                .thenCompose(headerInfo -> readHeader(headerInfo, readOnly, timer.getRemaining()))
                .thenCompose(handle -> openActiveSubSegment(handle, timer.getRemaining()));
    }

    /**
     * Reads the Header file for a particular Segment.
     */
    private CompletableFuture<RollingSegmentHandle> readHeader(SegmentProperties headerInfo, boolean readOnly, Duration timeout) {
        byte[] readBuffer = new byte[(int) headerInfo.getLength()];
        val openHeader = readOnly ? this.baseStorage.openRead(headerInfo.getName()) : this.baseStorage.openWrite(headerInfo.getName());
        return openHeader
                .thenCompose(hh -> this.baseStorage.read(hh, 0, readBuffer, 0, readBuffer.length, timeout))
                .thenApply(v -> {
                    RollingSegmentHandle handle = HandleSerializer.deserialize(readBuffer, openHeader.join());
                    if (headerInfo.isSealed()) {
                        handle.markSealed();
                    }

                    return handle;
                });
    }

    private CompletableFuture<Void> sealHeader(RollingSegmentHandle h, Duration timeout) {
        return this.baseStorage.seal(h.getHeaderHandle(), timeout)
                               .thenRun(h::markSealed);
    }

    private CompletableFuture<Void> serializeHandle(RollingSegmentHandle handle, Duration timeout) {
        ByteArraySegment handleData = HandleSerializer.serialize(handle);
        return this.baseStorage
                .write(handle.getHeaderHandle(), 0, handleData.getReader(), handleData.getLength(), timeout)
                .thenRun(() -> handle.setHeaderLength(handleData.getLength()));
    }

    private CompletableFuture<Void> serializeNewSubSegment(RollingSegmentHandle handle, SubSegment newSubSegment, Duration timeout) {
        return updateHandle(handle, HandleSerializer.serializeSubSegment(newSubSegment), timeout);
    }

    private CompletableFuture<Void> serializeBeginConcat(RollingSegmentHandle targetHandle, RollingSegmentHandle sourceHandle, Duration timeout) {
        byte[] updateData = HandleSerializer.serializeConcat(sourceHandle.subSegments().size(), targetHandle.length());
        return updateHandle(targetHandle, updateData, timeout);
    }

    private CompletableFuture<Void> updateHandle(RollingSegmentHandle handle, byte[] data, Duration timeout) {
        return this.baseStorage
                .write(handle.getHeaderHandle(), handle.getHeaderLength(), new ByteArrayInputStream(data), data.length, timeout)
                .thenRun(() -> handle.increaseHeaderLength(data.length));

    }

    //endregion

    //region Helpers

    private List<SubSegment> rebase(List<SubSegment> subSegments, long newStartOffset) {
        AtomicLong segmentOffset = new AtomicLong(newStartOffset);
        return subSegments.stream()
                          .map(s -> s.withNewOffset(segmentOffset.getAndAdd(s.getLength())))
                          .collect(Collectors.toList());
    }

    @SneakyThrows
    private void checkTruncatedSegment(Throwable ex, RollingSegmentHandle handle, SubSegment subSegment) {
        if (ex != null && (ExceptionHelpers.getRealException(ex) instanceof StreamSegmentNotExistsException) || !subSegment.exists()) {
            // We ran into a SubSegment that does not exist (either marked as such or due to a failed read).
            subSegment.markInexistent();
            String message = String.format("Cannot read from Segment '%s' because offsets %d-%d have been deleted.",
                    handle.getSegmentName(), subSegment.getStartOffset(), subSegment.getLastOffset());
            ex = new StreamSegmentTruncatedException(message, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    private <T extends Exception> Void ignore(Throwable ex, Class<T> type) {
        return ignore(ex, type, null);
    }

    @SneakyThrows
    private <T extends Exception, V> V ignore(Throwable ex, Class<T> type, V resultOnIgnored) {
        if (!ExceptionHelpers.getRealException(ex).getClass().isAssignableFrom(type)) {
            throw ex;
        }

        return resultOnIgnored;
    }

    private CompletableFuture<Void> ignoreEmptyFile(Throwable ex, String subSegmentName, Duration timeout) {
        if (ex == null) {
            // No exception.
            return CompletableFuture.completedFuture(null);
        }

        if (ExceptionHelpers.getRealException(ex) instanceof StreamSegmentExistsException) {
            // SubSegment exists, check if it's empty.
            return this.baseStorage
                    .getStreamSegmentInfo(subSegmentName, timeout)
                    .thenCompose(si -> si.getLength() == 0 ? CompletableFuture.completedFuture(null) : FutureHelpers.failedFuture(ex));
        } else {
            // Some other kind of exception - rethrow.
            return FutureHelpers.failedFuture(ex);
        }
    }

    @SneakyThrows(StreamSegmentNotExistsException.class)
    private SegmentProperties checkNonEmptyFile(SegmentProperties si, String segmentName) {
        if (si.getLength() == 0) {
            // We treat empty header files as inexistent segments.
            throw new StreamSegmentNotExistsException(segmentName);
        }

        return si;
    }

    private RollingSegmentHandle asWritableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        return asReadableHandle(handle);
    }

    private RollingSegmentHandle asReadableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle instanceof RollingSegmentHandle, "handle must be of type HDFSSegmentHandle.");
        return (RollingSegmentHandle) handle;
    }

    //endregion
}
