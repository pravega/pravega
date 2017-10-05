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
import com.google.common.collect.Iterators;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.TruncateableStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
        //TODO: implement
        return null;
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
        String headerFile = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        return recoverHeader(headerFile, timeout)
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
        CompletableFuture<?> result = FutureHelpers.exceptionallyCompose(
                this.baseStorage.create(headerFile, timer.getRemaining())
                                .handle((v, ex) -> ignoreEmptyFile(ex, headerFile, timer.getRemaining()))
                                .thenComposeAsync(ignored -> this.baseStorage.openWrite(headerFile), this.executor)
                                .thenComposeAsync(h -> {
                                    headerHandle.set(h);
                                    RollingSegmentHandle handle = new RollingSegmentHandle(segmentName, h, rollingPolicy, Collections.emptyList());
                                    return serializeHandle(handle, timer.getRemaining());
                                }, this.executor),
                ex -> {
                    // If we encountered an error while writing the handle file, delete it before returning the exception,
                    // otherwise we'll leave behind an empty file.
                    if (!(ExceptionHelpers.getRealException(ex) instanceof StreamSegmentExistsException)) {
                        return this.baseStorage.delete(headerHandle.get(), timer.getRemaining())
                                               .thenCompose(v2 -> FutureHelpers.failedFuture(ex));
                    } else {
                        // Some other kind of exception - rethrow.
                        return FutureHelpers.failedFuture(ex);
                    }
                });
        return result.thenApply(v -> StreamSegmentInformation.builder().name(segmentName).build());
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
                                .thenComposeAsync(v -> writeToActiveSubSegment(h, writeOffset, data, writeLength, timer.getRemaining()), this.executor);
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
                    .thenComposeAsync(v -> {
                        h.setActiveSubSegmentHandle(null);
                        h.lastSubSegment().markSealed();
                        return sealHeader(h, timer.getRemaining());
                    }, this.executor);
        } else {
            // No active file, only need to seal the Header.
            return sealHeader(h, timeout);
        }
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long targetOffset, String sourceSegment, Duration timeout) {
        val th = asWritableHandle(targetHandle);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return open(sourceSegment, false, timer.getRemaining())
                .thenComposeAsync(sh -> {
                    // Verify target Segment (offset and not sealed).
                    if (targetOffset != th.length()) {
                        return FutureHelpers.failedFuture(new BadOffsetException(th.getSegmentName(), th.length(), targetOffset));
                    }

                    if (th.isSealed()) {
                        return FutureHelpers.failedFuture(new StreamSegmentSealedException(th.getSegmentName()));
                    }

                    // We can only use a Segment as a concat source if it is Sealed.
                    Preconditions.checkState(sh.isSealed(), "Cannot concat segment '%s' into '%s' because it is not sealed.",
                            sourceSegment, th.getSegmentName());
                    if (sh.length() == 0) {
                        //Source is empty; do not bother with concatenation.
                        return delete(sh, timer.getRemaining());
                    }

                    // We can only use a Segment as a concat source if all of its SubSegments exist.
                    return refreshSubSegmentExistence(sh, timer.getRemaining())
                            .thenComposeAsync(v -> {
                                Preconditions.checkState(sh.subSegments().stream().allMatch(SubSegment::exists),
                                        "Cannot use segment '%s' as concat source because it is truncated.", sh.getSegmentName());

                                return concatSubSegments(th, sh, timer);
                            });
                }, this.executor);
    }

    private CompletableFuture<Void> concatSubSegments(RollingSegmentHandle target, RollingSegmentHandle source, TimeoutTimer timer) {
        // TODO consider using native concat if the source
        // Generate new SubSegment entries from the SubSegments of the Source Segment(but update their start offsets).
        AtomicLong segmentOffset = new AtomicLong(target.length());
        val newSubSegments = source.subSegments().stream()
                .map(s -> s.withNewOffset(segmentOffset.getAndAdd(s.getLength())))
                .collect(Collectors.toList());

        AtomicInteger serializationLength = new AtomicInteger(0);
        // TODO: findBugs complains about this not being closed. Write a new, friendlier InputStream.
        val writeStream = new SequenceInputStream(Iterators.asEnumeration(newSubSegments.stream()
                .map(s -> {
                    byte[] ser = HandleSerializer.serialize(s);
                    serializationLength.addAndGet(ser.length);
                    return new ByteArrayInputStream(ser);
                })
                .collect(Collectors.toList()).iterator()));

        long targetHeaderLength = target.getHeaderLength();

        // The Concat operation is a bit tricky. We need to atomically concatenate the data, which means that no matter
        // where we fail, the Source is either fully concat into the Target (and thus inaccessible) or it is not, in which
        // case the Target is not touched and the Source is still accessible. In order to do this with the primitives in the
        // Storage interface, we need to do a few things.
        // 1. Seal previous active segment in the target.
        // 2. Create a "merge" header file and serialize all of the updates there. This contains only the data that will be
        // appended to the target's header file, with the correct merge offsets.
        // 3. Delete source header file. The Source segment is still recoverable (since we have a merge file).
        // 4. Concat "merge" header file. The "merge" header will cease to exist and the source data is now part of the
        // target segment.
        // 5. Update in-memory handle.
        return sealActiveSubSegment(target, timer)
                .thenCompose(v -> serializeMergeHeader(writeStream, serializationLength.get(), source.getHeaderHandle().getSegmentName(), timer))
                .thenCompose(mergeHandle -> this.baseStorage.delete(source.getHeaderHandle(), timer.getRemaining())
                        .exceptionally(ex -> ignore(ex, StreamSegmentNotExistsException.class))
                        .thenCompose(v -> FutureHelpers.exceptionallyCompose(
                                this.baseStorage.concat(target.getHeaderHandle(), targetHeaderLength, mergeHandle.getSegmentName(), timer.getRemaining()),
                                ex -> this.baseStorage.delete(mergeHandle, timer.getRemaining())
                                        .thenCompose(v2 -> FutureHelpers.failedFuture(ex)))))
                .thenRun(() -> {
                    target.increaseHeaderLength(serializationLength.get());
                    target.addSubSegments(newSubSegments);
                });
    }

    private CompletableFuture<SegmentHandle> serializeMergeHeader(InputStream contents, int length, String baseSegmentName, TimeoutTimer timer) {
        String mergeName = StreamSegmentNameUtils.getMergeSegmentName(baseSegmentName);
        return FutureHelpers.exceptionallyCompose(this.baseStorage.create(mergeName, timer.getRemaining()),
                ex -> {
                    if (ExceptionHelpers.getRealException(ex) instanceof StreamSegmentExistsException) {
                        return this.baseStorage.openWrite(mergeName)
                                .thenCompose(h -> this.baseStorage.delete(h, timer.getRemaining()))
                                .thenCompose(v -> this.baseStorage.create(mergeName, timer.getRemaining()));
                    } else {
                        return FutureHelpers.failedFuture(ex);
                    }
                })
                .thenCompose(v -> this.baseStorage.openWrite(mergeName))
                .thenCompose(mergeHandle -> this.baseStorage.write(mergeHandle, 0, contents, length, timer.getRemaining())
                        .thenCompose(v -> this.baseStorage.seal(mergeHandle, timer.getRemaining()))
                        .thenApply(v -> mergeHandle));
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
                .thenCompose(v -> this.baseStorage.delete(h.getHeaderHandle(), timer.getRemaining()))
                .handle((v, ex1) -> {
                    // Regardless of whether we managed to delete the Header file, we need to delete the merge header file
                    // in case that was still sticking around.
                    if (ex1 != null && !(ExceptionHelpers.getRealException(ex1) instanceof StreamSegmentNotExistsException)) {
                        return FutureHelpers.<Void>failedFuture(ex1);
                    }

                    return this.baseStorage.openWrite(StreamSegmentNameUtils.getMergeSegmentName(h.getHeaderHandle().getSegmentName()))
                            .thenCompose(mh -> this.baseStorage.delete(mh, timer.getRemaining()))
                            .exceptionally(ex2 -> {
                                // If only one of these fails, then it's ok and expected, since most of the time only one
                                // of them exists. However if both fail, we must report the exception.
                                if (ex1 != null) {
                                    throw new CompletionException(Exceptions.coalesce(ex1, ex2));
                                }
                                return null;
                            });

                }).thenAccept(Callbacks::doNothing);
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
                    .thenComposeAsync(v -> deleteSubSegments(h, s -> canTruncate(s, truncationOffset), timeout), this.executor);
        } else {
            return deleteSubSegments(h, s -> canTruncate(s, truncationOffset), timeout);
        }
    }

    //endregion

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

    private CompletableFuture<Void> sealHeader(RollingSegmentHandle h, Duration timeout) {
        return this.baseStorage.seal(h.getHeaderHandle(), timeout)
                               .thenRun(h::markSealed);
    }

    private CompletableFuture<Void> rollover(RollingSegmentHandle handle, Duration timeout) {
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot rollover using a read-only handle.");
        Preconditions.checkArgument(!handle.isSealed(), "Cannot rollover a Sealed Segment.");

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return sealActiveSubSegment(handle, timer)
                .thenComposeAsync(v -> createSubSegment(handle, timer.getRemaining()), this.executor);
    }

    private CompletableFuture<Void> sealActiveSubSegment(RollingSegmentHandle handle, TimeoutTimer timer) {
        if (handle.getActiveSubSegmentHandle() != null) {
            return this.baseStorage.seal(handle.getActiveSubSegmentHandle(), timer.getRemaining())
                    .thenRun(() -> {
                        handle.setActiveSubSegmentHandle(null);
                        handle.lastSubSegment().markSealed();
                    });
        } else {
            return CompletableFuture.completedFuture(null);
        }
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
                .thenComposeAsync(si -> serializeHandleUpdate(handle, newSubSegment, timer.getRemaining()), this.executor)
                .thenComposeAsync(v -> this.baseStorage.openWrite(newSubSegment.getName()), this.executor)
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

    private boolean canTruncate(SubSegment subSegment, long truncationOffset) {
        // We should only truncate those SubSegments that are entirely before the truncationOffset. An empty SubSegment
        // that starts exactly at the truncationOffset should be spared (this means we truncate the entire Segment), as
        // we need that SubSegment to determine the actual length of the Segment.
        return subSegment.getStartOffset() < truncationOffset
                || subSegment.getLastOffset() <= truncationOffset;
    }

    /**
     * Opens a RollingSegmentHandle for the given Segment.
     */
    private CompletableFuture<RollingSegmentHandle> open(String segmentName, boolean readOnly, Duration timeout) {
        String headerSegment = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return recoverHeader(headerSegment, timer.getRemaining())
                .thenComposeAsync(si -> {
                    if (si.getLength() == 0) {
                        // We treat empty header files as inexistent segments.
                        return FutureHelpers.failedFuture(new StreamSegmentNotExistsException(segmentName));
                    } else {
                        return readHeader(si, readOnly, timer.getRemaining());
                    }
                }, this.executor)
                .thenComposeAsync(handle -> openActiveSubSegment(handle, timer.getRemaining()), this.executor);
    }

    private CompletableFuture<SegmentProperties> recoverHeader(String headerSegment, Duration timeout) {
        /*
        OpenHeader
        1. If not exists, recover from $merge file.
        2. H exists, MH exists/notexists: open H
        3. H not exists, MH exists: recover H from MH, open H
        4. H not exists, MH not exists, SegmentNotExists.

        Fix:
        * Exists & Open (done by this method).
        * Create
        * Delete (DONE)
         */
        return this.baseStorage.getStreamSegmentInfo(headerSegment, timeout);
    }

    /**
     * Reads the Header file for a particular Segment.
     */
    private CompletableFuture<RollingSegmentHandle> readHeader(SegmentProperties headerInfo, boolean readOnly, Duration timeout) {
        byte[] readBuffer = new byte[(int) headerInfo.getLength()];
        val openHeader = readOnly ? this.baseStorage.openRead(headerInfo.getName()) : this.baseStorage.openWrite(headerInfo.getName());
        return openHeader
                .thenComposeAsync(hh -> this.baseStorage.read(hh, 0, readBuffer, 0, readBuffer.length, timeout), this.executor)
                .thenApplyAsync(v -> {
                    RollingSegmentHandle handle = HandleSerializer.deserialize(readBuffer, openHeader.join());
                    if (headerInfo.isSealed()) {
                        handle.markSealed();
                    }

                    return handle;
                }, this.executor);
    }

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
                    .thenComposeAsync(si -> {
                        activeSubSegment.setLength(si.getLength());
                        if (si.isSealed()) {
                            // Last segment is Sealed, so we can't have a Write Handle for it.
                            activeSubSegment.markSealed();
                            return CompletableFuture.completedFuture(null);
                        } else {
                            // Open-Write the active SubSegment, and get its handle.
                            return this.baseStorage.openWrite(activeSubSegment.getName());
                        }
                    }, this.executor)
                    .thenApply(activeHandle -> {
                        handle.setActiveSubSegmentHandle(activeHandle);
                        return handle;
                    });
        } else {
            // No SubSegments - return as is.
            return CompletableFuture.completedFuture(handle);
        }
    }

    /**
     * Updates the given handle with the latest information from the header file in Storage.
     */
    private CompletableFuture<Void> reload(RollingSegmentHandle handle) {
        // TODO: implement
        return null;
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

    private CompletableFuture<Void> serializeHandle(RollingSegmentHandle handle, Duration timeout) {
        ByteArraySegment handleData = HandleSerializer.serialize(handle);
        return this.baseStorage
                .write(handle.getHeaderHandle(), 0, handleData.getReader(), handleData.getLength(), timeout)
                .thenRun(() -> handle.setHeaderLength(handleData.getLength()));
    }

    private CompletableFuture<Void> serializeHandleUpdate(RollingSegmentHandle handle, SubSegment newSubSegment, Duration timeout) {
        byte[] appendData = HandleSerializer.serialize(newSubSegment);
        return this.baseStorage
                .write(handle.getHeaderHandle(), handle.getHeaderLength(), new ByteArrayInputStream(appendData), appendData.length, timeout)
                .thenRun(() -> handle.increaseHeaderLength(appendData.length));
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

    private RollingSegmentHandle asWritableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        return asReadableHandle(handle);
    }

    private RollingSegmentHandle asReadableHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle instanceof RollingSegmentHandle, "handle must be of type HDFSSegmentHandle.");
        return (RollingSegmentHandle) handle;
    }
}
