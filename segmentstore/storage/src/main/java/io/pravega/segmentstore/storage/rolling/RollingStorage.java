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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CollectionHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.contracts.StreamingException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

/**
 * A layer on top of a general SyncStorage implementation that allows rolling Segments on a size-based policy and truncating
 * them at various offsets.
 *
 * Every Segment that is created using this Storage is made up of a Header and zero or more SubSegments
 * * The Header contains the Segment's Rolling Policy, as well as an ordered list of Offset-to-SubSegment pointers for
 * all the SubSegments in the Segment.
 * * The SubSegments contain data that their Segment is made of. A SubSegment starting at offset N with length L contains
 * data for offsets [N,N+L) of the Segment.
 * * A Segment is considered to exist if it has a non-empty Header and if its last SubSegment exists. If it does not have
 * any SubSegments (freshly created), it is considered to exist.
 * * A Segment is considered to be Sealed if its Header is sealed.
 */
public class RollingStorage implements SyncStorage {
    //region Members

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final SyncStorage baseStorage;
    private final SegmentRollingPolicy defaultRollingPolicy;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RollingStorage class.
     *
     * @param baseStorage          A SyncStorage that will be used to execute operations.
     * @param defaultRollingPolicy A SegmentRollingPolicy to apply to every StreamSegment that does not have its own policy
     *                             defined.
     */
    public RollingStorage(SyncStorage baseStorage, SegmentRollingPolicy defaultRollingPolicy) {
        this.baseStorage = Preconditions.checkNotNull(baseStorage, "baseStorage");
        this.defaultRollingPolicy = Preconditions.checkNotNull(defaultRollingPolicy, "defaultRollingPolicy");
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
    public SegmentHandle openRead(String segmentName) throws StreamSegmentException {
        val headerInfo = getHeaderInfo(segmentName);
        val headerHandle = this.baseStorage.openRead(headerInfo.getName());
        val handle = readHeader(headerInfo, headerHandle);
        updateLengths(handle);
        return handle;
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        val h = asReadableHandle(handle);
        ensureNotDeleted(h);
        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }

        if (h.isReadOnly() && !h.isSealed() && offset + length > h.length()) {
            // We have a non-sealed read-only handle. It's possible that the SubSegments may have been modified since the
            // last time we refreshed it, and we received a request for a read beyond our last known offset. Reload the
            // handle before attempting the read.
            val newHandle = (RollingSegmentHandle) openRead(handle.getSegmentName());
            h.refresh(newHandle);
        }

        Preconditions.checkArgument(offset + length <= h.length(), "Offset %s + length %s is beyond the last offset %s of the segment.",
                offset, length, h.length());

        // Read in a loop, from each SubSegment, until we can't read anymore.
        // If at any point we encounter a StreamSegmentNotExistsException, fail immediately with StreamSegmentTruncatedException (+inner).
        val subSegments = h.subSegments();
        int currentIndex = CollectionHelpers.binarySearch(subSegments, s -> offset < s.getStartOffset() ? -1 : (offset >= s.getLastOffset() ? 1 : 0));
        assert currentIndex >= 0 : "unable to locate first SubSegment index.";

        try {
            int bytesRead = 0;
            while (bytesRead < length && currentIndex < subSegments.size()) {
                // Verify if this is a known truncated SubSegment; if so, bail out quickly.
                SubSegment current = subSegments.get(currentIndex);
                checkTruncatedSegment(null, h, current);
                long readOffset = offset + bytesRead - current.getStartOffset();
                int readLength = (int) Math.min(length - bytesRead, current.getLength() - readOffset);
                assert readOffset >= 0 && readLength >= 0 : "negative readOffset or readLength";

                // Read from the actual SubSegment into the given buffer.
                try {
                    val sh = this.baseStorage.openRead(current.getName());
                    int count = this.baseStorage.read(sh, readOffset, buffer, bufferOffset + bytesRead, readLength);
                    bytesRead += count;
                    if (readOffset + count >= current.getLength()) {
                        currentIndex++;
                    }
                } catch (StreamSegmentNotExistsException ex) {
                    checkTruncatedSegment(ex, h, current);
                }
            }

            return bytesRead;
        } catch (StreamSegmentTruncatedException ex) {
            // It's possible that the Segment has been truncated or deleted altogether using another handle. We need to
            // refresh the handle and throw the appropriate exception.
            val newHandle = (RollingSegmentHandle) openRead(handle.getSegmentName());
            h.refresh(newHandle);
            if (h.isDeleted()) {
                throw new StreamSegmentNotExistsException(handle.getSegmentName(), ex);
            } else {
                throw ex;
            }
        }
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String segmentName) throws StreamSegmentException {
        val handle = (RollingSegmentHandle) openRead(segmentName);
        return StreamSegmentInformation
                .builder()
                .name(handle.getSegmentName())
                .sealed(handle.isSealed())
                .length(handle.length())
                .build();
    }

    @Override
    @SneakyThrows(StreamSegmentException.class)
    public boolean exists(String segmentName) {
        try {
            // Try to open-read the segment, this checks both the header file and the existence of the last SubSegment.
            openRead(segmentName);
            return true;
        } catch (StreamSegmentNotExistsException ex) {
            return false;
        }
    }

    //endregion

    //region Storage Implementation

    @Override
    public SegmentProperties create(String streamSegmentName) throws StreamSegmentException {
        return create(streamSegmentName, this.defaultRollingPolicy);
    }

    /**
     * Creates a new StreamSegment with given SegmentRollingPolicy.
     *
     * @param segmentName   The full name of the StreamSegment.
     * @param rollingPolicy The Rolling Policy to apply to this StreamSegment.
     * @return A SegmentProperties describing the newly created Segment.
     * @throws StreamSegmentException If an exception occurred.
     */
    public SegmentProperties create(String segmentName, SegmentRollingPolicy rollingPolicy) throws StreamSegmentException {
        Preconditions.checkNotNull(rollingPolicy, "rollingPolicy");
        String headerSubSegment = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);

        // Create the header file, and then serialize the contents to it.
        // If the header file already exists, then it's OK if it's empty (probably a remnant from a previously failed
        // attempt); in that case we ignore it and let the creation proceed.
        SegmentHandle headerHandle = null;
        try {
            try {
                this.baseStorage.create(headerSubSegment);
            } catch (StreamSegmentExistsException ex) {
                checkIfEmptyAndNotSealed(ex, headerSubSegment);
            }

            headerHandle = this.baseStorage.openWrite(headerSubSegment);
            serializeHandle(new RollingSegmentHandle(headerHandle, rollingPolicy, Collections.emptyList()));
        } catch (StreamSegmentExistsException ex) {
            throw ex;
        } catch (Exception ex) {
            if (!ExceptionHelpers.mustRethrow(ex) && headerHandle != null) {
                // If we encountered an error while writing the handle file, delete it before returning the exception,
                // otherwise we'll leave behind an empty file.
                try {
                    this.baseStorage.delete(headerHandle);
                } catch (Exception ex2) {
                    ex.addSuppressed(ex2);
                }
            }

            throw ex;
        }

        return StreamSegmentInformation.builder().name(segmentName).build();
    }

    @Override
    public SegmentHandle openWrite(String segmentName) throws StreamSegmentException {
        val headerInfo = getHeaderInfo(segmentName);
        val headerHandle = this.baseStorage.openWrite(headerInfo.getName());
        val handle = readHeader(headerInfo, headerHandle);
        updateLengths(handle);

        // Finally, open the Active SubSegment for writing.
        SubSegment last = handle.lastSubSegment();
        if (last != null && !last.isSealed()) {
            val activeHandle = this.baseStorage.openWrite(last.getName());
            handle.setActiveSubSegmentHandle(activeHandle);
        }

        return handle;
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        val h = asWritableHandle(handle);
        ensureNotDeleted(h);
        ensureNotSealed(h);
        ensureOffset(h, offset);

        // We run this in a loop because we may have to split the write over multiple SubSegments in order to avoid exceeding
        // any SubSegment's maximum length.
        int bytesWritten = 0;
        while (bytesWritten < length) {
            if (h.getActiveSubSegmentHandle() == null || h.lastSubSegment().getLength() >= h.getRollingPolicy().getMaxLength()) {
                rollover(h);
            }

            SubSegment last = h.lastSubSegment();
            int writeLength = (int) Math.min(length - bytesWritten, h.getRollingPolicy().getMaxLength() - last.getLength());
            assert writeLength > 0 : "non-positive write length";
            long subSegmentOffset = offset + bytesWritten - last.getStartOffset();
            this.baseStorage.write(h.getActiveSubSegmentHandle(), subSegmentOffset, data, writeLength);
            last.increaseLength(writeLength);
            bytesWritten += writeLength;
        }
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        val h = asWritableHandle(handle);
        ensureNotDeleted(h);

        if (h.getActiveSubSegmentHandle() != null) {
            // We have an active Segment; make sure it's Sealed too.

            this.baseStorage.seal(h.getActiveSubSegmentHandle());
            h.setActiveSubSegmentHandle(null);
            h.lastSubSegment().markSealed();
        }

        this.baseStorage.seal(h.getHeaderHandle());
        h.markSealed();
    }

    @Override
    public void concat(SegmentHandle targetHandle, long targetOffset, String sourceSegment) throws StreamSegmentException {
        val target = asWritableHandle(targetHandle);

        ensureOffset(target, targetOffset);
        ensureNotDeleted(target);
        ensureNotSealed(target);

        // We can only use a Segment as a concat source if it is Sealed.
        RollingSegmentHandle source = (RollingSegmentHandle) openWrite(sourceSegment);
        Preconditions.checkState(source.isSealed(), "Cannot concat segment '%s' into '%s' because it is not sealed.",
                sourceSegment, target.getSegmentName());
        if (source.length() == 0) {
            // Source is empty; do not bother with concatenation.
            delete(source);
            return;
        }

        // We can only use a Segment as a concat source if all of its SubSegments exist.
        refreshSubSegmentExistence(source);
        Preconditions.checkState(source.subSegments().stream().allMatch(SubSegment::exists),
                "Cannot use segment '%s' as concat source because it is truncated.", source.getSegmentName());

        SubSegment lastSource = source.lastSubSegment();
        SubSegment lastTarget = target.lastSubSegment();
        if (lastSource != null && lastSource.getStartOffset() == 0
                && lastTarget != null && !lastTarget.isSealed()
                && lastTarget.getLength() + lastSource.getLength() <= target.getRollingPolicy().getMaxLength()) {
            // The Source is made up of a single SubSegment that can fit entirely into the Target's Active SubSegment.
            // Concat it directly without touching the header file; this helps prevent having a lot of very small SubSegments
            // around if the application has a lot of small transactions.
            this.baseStorage.concat(target.getActiveSubSegmentHandle(), target.lastSubSegment().getLength(), lastSource.getName());
            target.lastSubSegment().increaseLength(lastSource.getLength());
            try {
                this.baseStorage.delete(source.getHeaderHandle());
            } catch (StreamSegmentNotExistsException ex) {
                // It's ok if it's not there anymore.
            }
        } else {
            // Generate new SubSegment entries from the SubSegments of the Source Segment(but update their start offsets).
            List<SubSegment> newSubSegments = rebase(source.subSegments(), target.length());
            sealActiveSubSegment(target);
            serializeBeginConcat(target, source);
            this.baseStorage.concat(target.getHeaderHandle(), target.getHeaderLength(), source.getHeaderHandle().getSegmentName());
            target.increaseHeaderLength(source.getHeaderLength());
            target.addSubSegments(newSubSegments);
        }
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        // We need to seal the whole Segment to prevent anyone else from creating new SubSegments while we're deleting
        // them, after which we delete all SubSegments and finally the header file.
        val h = asReadableHandle(handle);
        if (!h.isSealed()) {
            val writeHandle = h.isReadOnly() ? (RollingSegmentHandle) openWrite(handle.getSegmentName()) : h;
            seal(writeHandle);
        }

        deleteSubSegments(h, s -> true);
        try {
            this.baseStorage.delete(h.getHeaderHandle());
            h.markDeleted();
        } catch (StreamSegmentNotExistsException ex) {
            h.markDeleted();
            throw ex;
        }
    }

    //endregion

    //region TruncateableStorage Implementation

    @Override
    public void truncate(SegmentHandle handle, long truncationOffset) throws StreamSegmentException {
        // Delete all SubSegments which are entirely before the truncation offset.
        val h = asWritableHandle(handle);
        ensureNotDeleted(h);
        Preconditions.checkArgument(truncationOffset >= 0 && truncationOffset <= h.length(),
                "truncationOffset must be non-negative and at most the length of the Segment.");
        val last = h.lastSubSegment();
        if (last != null && canTruncate(last, truncationOffset)) {
            // If we were asked to truncate the entire Segment, then rollover at this point so we can delete all existing
            // data.
            rollover(h);
        }

        deleteSubSegments(h, s -> canTruncate(s, truncationOffset));
    }

    @Override
    public boolean supportsTruncation() {
        return true;
    }

    //endregion

    //region SubSegment Operations

    private void rollover(RollingSegmentHandle handle) throws StreamSegmentException {
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot rollover using a read-only handle.");
        Preconditions.checkArgument(!handle.isSealed(), "Cannot rollover a Sealed Segment.");
        sealActiveSubSegment(handle);
        createSubSegment(handle);
    }

    private void sealActiveSubSegment(RollingSegmentHandle handle) throws StreamSegmentException {
        if (handle.getActiveSubSegmentHandle() != null) {
            this.baseStorage.seal(handle.getActiveSubSegmentHandle());
            handle.setActiveSubSegmentHandle(null);
            handle.lastSubSegment().markSealed();
        }
    }

    private void createSubSegment(RollingSegmentHandle handle) throws StreamSegmentException {
        // Create new active SubSegment, only after which serialize the handle update and update the handle.
        // We ignore if the SubSegment exists and is empty - that's most likely due to a previous failed attempt.
        long segmentLength = handle.length();
        SubSegment newSubSegment = SubSegment.forSegment(handle.getSegmentName(), segmentLength);
        try {
            this.baseStorage.create(newSubSegment.getName());
        } catch (StreamSegmentExistsException ex) {
            checkIfEmptyAndNotSealed(ex, newSubSegment.getName());
        }

        serializeNewSubSegment(handle, newSubSegment);
        val activeHandle = this.baseStorage.openWrite(newSubSegment.getName());
        handle.addSubSegment(newSubSegment, activeHandle);
    }

    private void deleteSubSegments(RollingSegmentHandle handle, Predicate<SubSegment> canDelete) throws StreamSegmentException {
        for (SubSegment s : handle.subSegments()) {
            if (s.exists() && canDelete.test(s)) {
                try {
                    val subHandle = this.baseStorage.openWrite(s.getName());
                    this.baseStorage.delete(subHandle);
                    s.markInexistent();
                } catch (StreamSegmentNotExistsException ex) {
                    // Ignore; It's OK if it doesn't exist; just make sure the handle is updated.
                    s.markInexistent();
                }
            }
        }
    }

    private boolean canTruncate(SubSegment subSegment, long truncationOffset) {
        // We should only truncate those SubSegments that are entirely before the truncationOffset. An empty SubSegment
        // that starts exactly at the truncationOffset should be spared (this means we truncate the entire Segment), as
        // we need that SubSegment to determine the actual length of the Segment.
        return subSegment.getStartOffset() < truncationOffset
                && subSegment.getLastOffset() <= truncationOffset;
    }

    private void refreshSubSegmentExistence(RollingSegmentHandle handle) {
        // We check all SubSegments that we assume exist for actual existence (since once deleted, they can't come back).
        for (SubSegment s : handle.subSegments()) {
            if (s.exists() && !this.baseStorage.exists(s.getName())) {
                s.markInexistent();
            }
        }
    }

    //endregion

    //region Header Operations

    private SegmentProperties getHeaderInfo(String segmentName) throws StreamSegmentException {
        String headerSegment = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        val headerInfo = this.baseStorage.getStreamSegmentInfo(headerSegment);
        if (headerInfo.getLength() == 0) {
            // We treat empty header files as inexistent segments.
            throw new StreamSegmentNotExistsException(segmentName);
        }

        return headerInfo;
    }

    private RollingSegmentHandle readHeader(SegmentProperties headerInfo, SegmentHandle headerHandle) throws StreamSegmentException {
        byte[] readBuffer = new byte[(int) headerInfo.getLength()];
        this.baseStorage.read(headerHandle, 0, readBuffer, 0, readBuffer.length);
        RollingSegmentHandle handle = HandleSerializer.deserialize(readBuffer, headerHandle);
        if (headerInfo.isSealed()) {
            handle.markSealed();
        }
        return handle;
    }

    private void updateLengths(RollingSegmentHandle handle) throws StreamSegmentException {
        // For all but the last SubSegment we can infer the lengths by doing some simple arithmetic.
        SubSegment previous = null;
        for (SubSegment s : handle.subSegments()) {
            if (previous != null) {
                previous.setLength(s.getStartOffset() - previous.getStartOffset());
                previous.markSealed();
            }

            previous = s;
        }

        // For the last one, we need to actually check the file.
        SubSegment activeSubSegment = handle.lastSubSegment();
        if (activeSubSegment != null) {
            val si = this.baseStorage.getStreamSegmentInfo(activeSubSegment.getName());
            activeSubSegment.setLength(si.getLength());
            if (si.isSealed()) {
                // Last segment is Sealed, so we can't have a Write Handle for it.
                activeSubSegment.markSealed();
            }
        }
    }

    private void serializeHandle(RollingSegmentHandle handle) throws StreamSegmentException {
        ByteArraySegment handleData = HandleSerializer.serialize(handle);
        try {
            this.baseStorage.write(handle.getHeaderHandle(), 0, handleData.getReader(), handleData.getLength());
            handle.setHeaderLength(handleData.getLength());
        } catch (BadOffsetException ex) {
            // If we get BadOffsetException when writing the Handle, it means it was modified externally.
            throw new StorageNotPrimaryException(handle.getSegmentName(), ex);
        }
    }

    private void serializeNewSubSegment(RollingSegmentHandle handle, SubSegment newSubSegment) throws StreamSegmentException {
        updateHandle(handle, HandleSerializer.serializeSubSegment(newSubSegment));
    }

    private void serializeBeginConcat(RollingSegmentHandle targetHandle, RollingSegmentHandle sourceHandle) throws StreamSegmentException {
        byte[] updateData = HandleSerializer.serializeConcat(sourceHandle.subSegments().size(), targetHandle.length());
        updateHandle(targetHandle, updateData);
    }

    private void updateHandle(RollingSegmentHandle handle, byte[] data) throws StreamSegmentException {
        try {
            this.baseStorage.write(handle.getHeaderHandle(), handle.getHeaderLength(), new ByteArrayInputStream(data), data.length);
            handle.increaseHeaderLength(data.length);
        } catch (BadOffsetException ex) {
            // If we get BadOffsetException when writing the Handle, it means it was modified externally.
            throw new StorageNotPrimaryException(handle.getSegmentName(), ex);
        }
    }

    //endregion

    //region Helpers

    private List<SubSegment> rebase(List<SubSegment> subSegments, long newStartOffset) {
        AtomicLong segmentOffset = new AtomicLong(newStartOffset);
        return subSegments.stream()
                .map(s -> s.withNewOffset(segmentOffset.getAndAdd(s.getLength())))
                .collect(Collectors.toList());
    }

    @SneakyThrows(StreamingException.class)
    private void checkTruncatedSegment(StreamingException ex, RollingSegmentHandle handle, SubSegment subSegment) throws StreamSegmentTruncatedException {
        if (ex != null && (ExceptionHelpers.getRealException(ex) instanceof StreamSegmentNotExistsException) || !subSegment.exists()) {
            // We ran into a SubSegment that does not exist (either marked as such or due to a failed read).
            subSegment.markInexistent();
            String message = String.format("Offsets %d-%d have been deleted.", subSegment.getStartOffset(), subSegment.getLastOffset());
            ex = new StreamSegmentTruncatedException(handle.getSegmentName(), message, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    private void checkIfEmptyAndNotSealed(StreamSegmentExistsException ex, String subSegmentName) throws StreamSegmentException {
        // SubSegment exists, check if it's empty and not sealed.
        try {
            val si = this.baseStorage.getStreamSegmentInfo(subSegmentName);
            if (si.getLength() > 0 || si.isSealed()) {
                throw ex;
            }
        } catch (StreamSegmentNotExistsException notExists) {
            // nothing to do.
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

    private void ensureNotDeleted(RollingSegmentHandle handle) throws StreamSegmentNotExistsException {
        if (handle.isDeleted()) {
            throw new StreamSegmentNotExistsException(handle.getSegmentName());
        }
    }

    private void ensureNotSealed(RollingSegmentHandle handle) throws StreamSegmentSealedException {
        if (handle.isSealed()) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }
    }

    private void ensureOffset(RollingSegmentHandle handle, long offset) throws BadOffsetException {
        if (offset != handle.length()) {
            throw new BadOffsetException(handle.getSegmentName(), handle.length(), offset);
        }
    }

    //endregion
}
