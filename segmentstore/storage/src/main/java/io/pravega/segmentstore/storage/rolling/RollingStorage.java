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
package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.io.BoundedInputStream;
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
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.shared.NameUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * A layer on top of a general SyncStorage implementation that allows rolling Segments on a size-based policy and truncating
 * them at various offsets.
 *
 * Every Segment that is created using this Storage is made up of a Header and zero or more SegmentChunks.
 * * The Header contains the Segment's Rolling Policy, as well as an ordered list of Offset-to-SegmentChunk pointers for
 * all the SegmentChunks in the Segment.
 * * The SegmentChunks contain data that their Segment is made of. A SegmentChunk starting at offset N with length L contains
 * data for offsets [N,N+L) of the Segment.
 * * A Segment is considered to exist if it has a non-empty Header and if its last SegmentChunk exists. If it does not have
 * any SegmentChunks (freshly created), it is considered to exist.
 * * A Segment is considered to be Sealed if its Header is sealed.
 *
 * A note about compatibility:
 * * The RollingStorage wrapper is fully compatible with data and Segments that were created before RollingStorage was
 * applied. That means that it can access and modify existing Segments that were created without a Header, but all new
 * Segments will have a Header. As such, there is no need to do any sort of migration when starting to use this class.
 * * Should the RollingStorage need to be discontinued without having to do a migration:
 * ** The create() method should be overridden (in a derived class) to create new Segments natively (without a Header).
 * ** The concat() method should be overridden (in a derived class) to not convert Segments without Header into Segments
 * with Header.
 * ** Existing Segments (made up of Header and multi-SegmentChunks) can still be accessed by means of this class.
 */
@Slf4j
public class RollingStorage implements SyncStorage {
    //region Members

    private final SyncStorage baseStorage;
    private final SyncStorage headerStorage;
    private final SegmentRollingPolicy defaultRollingPolicy;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RollingStorage class with a default SegmentRollingPolicy set to NoRolling.
     *
     * @param baseStorage          A SyncStorage that will be used to execute operations.
     */
    public RollingStorage(SyncStorage baseStorage) {
        this(baseStorage, SegmentRollingPolicy.NO_ROLLING);
    }

    /**
     * Creates a new instance of the RollingStorage class.
     *
     * @param baseStorage          A SyncStorage that will be used to execute operations.
     * @param defaultRollingPolicy A SegmentRollingPolicy to apply to every StreamSegment that does not have its own policy
     *                             defined.
     */
    public RollingStorage(SyncStorage baseStorage, SegmentRollingPolicy defaultRollingPolicy) {
        this.baseStorage = Preconditions.checkNotNull(baseStorage, "baseStorage");
        this.headerStorage = this.baseStorage.withReplaceSupport();
        if (!this.headerStorage.supportsReplace()) {
            log.info("Base Storage does not support replace. Will not be able to trim header chunks after truncation.");
        }
        this.defaultRollingPolicy = Preconditions.checkNotNull(defaultRollingPolicy, "defaultRollingPolicy");
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.baseStorage.close();
            log.info("Closed");
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
        long traceId = LoggerHelpers.traceEnter(log, "openRead", segmentName);
        val handle = openHandle(segmentName, true);
        LoggerHelpers.traceLeave(log, "openRead", traceId, handle);
        return handle;
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        val h = getHandle(handle);
        long traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);
        ensureNotDeleted(h);
        Exceptions.checkArrayRange(bufferOffset, length, buffer.length, "bufferOffset", "length");

        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }

        if (!h.isSealed() && offset + length > h.length()) {
            // We have a non-sealed handle (read-only or read-write). It's possible that the SegmentChunks may have been
            // modified since the last time we refreshed it, and we received a request for a read beyond our last known offset.
            // This could happen if the Segment was modified using a different handle or a previous write did succeed but was
            // reported as having failed. Reload the handle before attempting the read so that we have the most up-to-date info.
            val newHandle = (RollingSegmentHandle) openRead(handle.getSegmentName());
            h.refresh(newHandle);
            log.debug("Handle refreshed: {}.", h);
        }

        Preconditions.checkArgument(offset + length <= h.length(), "Offset %s + length %s is beyond the last offset %s of the segment.",
                offset, length, h.length());

        // Read in a loop, from each SegmentChunk, until we can't read anymore.
        // If at any point we encounter a StreamSegmentNotExistsException, fail immediately with StreamSegmentTruncatedException (+inner).
        val chunks = h.chunks();
        int currentIndex = CollectionHelpers.binarySearch(chunks, s -> offset < s.getStartOffset() ? -1 : (offset >= s.getLastOffset() ? 1 : 0));
        assert currentIndex >= 0 : "unable to locate first SegmentChunk index.";

        try {
            int bytesRead = 0;
            while (bytesRead < length && currentIndex < chunks.size()) {
                // Verify if this is a known truncated SegmentChunk; if so, bail out quickly.
                SegmentChunk current = chunks.get(currentIndex);
                checkTruncatedSegment(null, h, current);
                if (current.getLength() == 0) {
                    // Empty SegmentChunk; don't bother trying to read from it.
                    continue;
                }

                long readOffset = offset + bytesRead - current.getStartOffset();
                int readLength = (int) Math.min(length - bytesRead, current.getLength() - readOffset);
                assert readOffset >= 0 && readLength >= 0 : "negative readOffset or readLength";

                // Read from the actual SegmentChunk into the given buffer.
                try {
                    val sh = this.baseStorage.openRead(current.getName());
                    int count = this.baseStorage.read(sh, readOffset, buffer, bufferOffset + bytesRead, readLength);
                    bytesRead += count;
                    if (readOffset + count >= current.getLength()) {
                        currentIndex++;
                    }
                } catch (StreamSegmentNotExistsException ex) {
                    log.debug("SegmentChunk '{}' does not exist anymore ({}).", current, h);
                    checkTruncatedSegment(ex, h, current);
                }
            }

            LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, bytesRead);
            return bytesRead;
        } catch (StreamSegmentTruncatedException ex) {
            // It's possible that the Segment has been truncated or deleted altogether using another handle. We need to
            // refresh the handle and throw the appropriate exception.
            val newHandle = (RollingSegmentHandle) openRead(handle.getSegmentName());
            h.refresh(newHandle);
            if (h.isDeleted()) {
                log.debug("Segment '{}' has been deleted. Cannot read anymore.", h);
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
            // Try to open-read the segment, this checks both the header file and the existence of the last SegmentChunk.
            openRead(segmentName);
            return true;
        } catch (StreamSegmentNotExistsException ex) {
            return false;
        }
    }

    //endregion

    //region SyncStorage Implementation

    @Override
    public SegmentHandle create(String streamSegmentName) throws StreamSegmentException {
        return create(streamSegmentName, this.defaultRollingPolicy);
    }

    @Override
    public SegmentHandle create(String segmentName, SegmentRollingPolicy rollingPolicy) throws StreamSegmentException {
        Preconditions.checkNotNull(rollingPolicy, "rollingPolicy");
        String headerName = NameUtils.getHeaderSegmentName(segmentName);
        long traceId = LoggerHelpers.traceEnter(log, "create", segmentName, rollingPolicy);

        // First, check if the segment exists but with no header (it might have been created prior to applying
        // RollingStorage to this baseStorage).
        if (this.baseStorage.exists(segmentName)) {
            throw new StreamSegmentExistsException(segmentName);
        }

        // Create the header file, and then serialize the contents to it.
        // If the header file already exists, then it's OK if it's empty (probably a remnant from a previously failed
        // attempt); in that case we ignore it and let the creation proceed.
        SegmentHandle headerHandle = null;
        RollingSegmentHandle retValue;
        try {
            try {
                headerHandle = this.headerStorage.create(headerName);
            } catch (StreamSegmentExistsException ex) {
                checkIfEmptyAndNotSealed(ex, headerName, this.headerStorage);
                headerHandle = this.headerStorage.openWrite(headerName);
                log.debug("Empty Segment Header found for '{}'; treating as inexistent.", segmentName);
            }

            retValue = new RollingSegmentHandle(headerHandle, rollingPolicy, new ArrayList<>());
            serializeHandle(retValue);
        } catch (StreamSegmentExistsException ex) {
            throw ex;
        } catch (Exception ex) {
            if (!Exceptions.mustRethrow(ex) && headerHandle != null) {
                // If we encountered an error while writing the handle file, delete it before returning the exception,
                // otherwise we'll leave behind an empty file.
                try {
                    log.warn("Could not write Header Segment for '{}', rolling back.", segmentName, ex);
                    this.headerStorage.delete(headerHandle);
                } catch (Exception ex2) {
                    ex.addSuppressed(ex2);
                }
            }

            throw ex;
        }

        LoggerHelpers.traceLeave(log, "create", traceId, segmentName);
        return retValue;
    }

    @Override
    public SegmentHandle openWrite(String segmentName) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", segmentName);
        val handle = openHandle(segmentName, false);

        // Finally, open the Active SegmentChunk for writing.
        SegmentChunk last = handle.lastChunk();
        if (last != null && !last.isSealed()) {
            val activeHandle = this.baseStorage.openWrite(last.getName());
            handle.setActiveChunkHandle(activeHandle);
        }

        LoggerHelpers.traceLeave(log, "openWrite", traceId, handle);
        return handle;
    }

    @Override
    @SneakyThrows(IOException.class)
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        val h = getHandle(handle);
        ensureNotDeleted(h);
        ensureNotSealed(h);
        ensureWritable(h);
        ensureOffset(h, offset);
        long traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);

        // We run this in a loop because we may have to split the write over multiple SegmentChunks in order to avoid exceeding
        // any SegmentChunk's maximum length.
        int bytesWritten = 0;
        while (bytesWritten < length) {
            if (h.getActiveChunkHandle() == null || h.lastChunk().getLength() >= h.getRollingPolicy().getMaxLength()) {
                rollover(h);
            }

            SegmentChunk last = h.lastChunk();
            int writeLength = (int) Math.min(length - bytesWritten, h.getRollingPolicy().getMaxLength() - last.getLength());
            assert writeLength > 0 : "non-positive write length";
            long chunkOffset = offset + bytesWritten - last.getStartOffset();

            // Use a BoundedInputStream to ensure that the underlying storage does not try to read more (or less) data
            // than we instructed it to. Invoking BoundedInputStream.close() will throw an IOException if baseStorage.write()
            // has not read all the bytes it was supposed to.
            try (BoundedInputStream bis = new BoundedInputStream(data, writeLength)) {
                this.baseStorage.write(h.getActiveChunkHandle(), chunkOffset, bis, writeLength);
            }
            last.increaseLength(writeLength);
            bytesWritten += writeLength;
        }

        LoggerHelpers.traceLeave(log, "write", traceId, handle, offset, bytesWritten);
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        val h = getHandle(handle);
        ensureNotDeleted(h);
        if (h.isReadOnly() && h.isSealed()) {
            // Nothing to do.
            log.debug("Segment already sealed: '{}'.", h.getSegmentName());
            return;
        }

        long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
        sealActiveChunk(h);
        SegmentHandle headerHandle = h.getHeaderHandle();
        if (headerHandle != null) {
            this.headerStorage.seal(headerHandle);
        }

        h.markSealed();
        log.debug("Sealed Header for '{}'.", h.getSegmentName());
        LoggerHelpers.traceLeave(log, "seal", traceId, handle);
    }

    @Override
    public void unseal(SegmentHandle handle) {
        throw new UnsupportedOperationException("RollingStorage does not support unseal().");
    }

    @Override
    public void concat(SegmentHandle targetHandle, long targetOffset, String sourceSegment) throws StreamSegmentException {
        val target = getHandle(targetHandle);
        ensureOffset(target, targetOffset);
        ensureNotDeleted(target);
        ensureNotSealed(target);
        ensureWritable(target);
        long traceId = LoggerHelpers.traceEnter(log, "concat", target, targetOffset, sourceSegment);

        // We can only use a Segment as a concat source if it is Sealed.
        RollingSegmentHandle source = (RollingSegmentHandle) openWrite(sourceSegment);
        Preconditions.checkState(source.isSealed(), "Cannot concat segment '%s' into '%s' because it is not sealed.",
                sourceSegment, target.getSegmentName());
        if (source.length() == 0) {
            // Source is empty; do not bother with concatenation.
            log.debug("Concat source '{}' is empty. Deleting instead of concatenating.", source);
            delete(source);
            return;
        }

        // We can only use a Segment as a concat source if it hasn't been truncated.
        refreshChunkExistence(source);
        Preconditions.checkState(source.chunks().stream().allMatch(SegmentChunk::exists) && source.chunks().get(0).getStartOffset() == 0,
                "Cannot use Segment '%s' as concat source because it is truncated.", source.getSegmentName());

        if (shouldConcatNatively(source, target)) {
            // The Source either does not have a Header or is made up of a single SegmentChunk that can fit entirely into
            // the Target's Active SegmentChunk. Concat it directly without touching the header file; this helps prevent
            // having a lot of very small SegmentChunks around if we end up doing a lot of concatenations.
            log.debug("Concat '{}' into '{}' using native method.", source, target);
            SegmentChunk lastTarget = target.lastChunk();
            if (lastTarget == null || lastTarget.isSealed()) {
                // Make sure the last SegmentChunk of the target is not sealed, otherwise we can't concat into it.
                rollover(target);
            }

            SegmentChunk lastSource = source.lastChunk();
            this.baseStorage.concat(target.getActiveChunkHandle(), target.lastChunk().getLength(), lastSource.getName());
            target.lastChunk().increaseLength(lastSource.getLength());
            if (source.getHeaderHandle() != null) {
                try {
                    this.headerStorage.delete(source.getHeaderHandle());
                } catch (StreamSegmentNotExistsException ex) {
                    // It's ok if it's not there anymore.
                    log.warn("Attempted to delete concat source Header '{}' but it doesn't exist.", source.getHeaderHandle().getSegmentName(), ex);
                }
            }
        } else {
            // Generate new SegmentChunk entries from the SegmentChunks of the Source Segment(but update their start offsets).
            log.debug("Concat '{}' into '{}' using header merge method.", source, target);

            if (target.getHeaderHandle() == null) {
                // We need to concat into a Segment that does not have a Header (yet). Create one before continuing.
                createHeader(target);
            }

            List<SegmentChunk> newSegmentChunks = rebase(source.chunks(), target.length());
            sealActiveChunk(target);
            serializeBeginConcat(target, source);
            this.headerStorage.concat(target.getHeaderHandle(), target.getHeaderLength(), source.getHeaderHandle().getSegmentName());
            target.increaseHeaderLength(source.getHeaderLength());
            target.addChunks(newSegmentChunks);

            // After we do a header merge, it's possible that the (new) last chunk may still have space to write to.
            // Unseal it now so that future writes/concats will not unnecessarily create chunks. Note that this will not
            // unseal the segment (even though it's unsealed) - that is determined by the Header file seal status.
            unsealLastChunkIfNecessary(target);
        }

        LoggerHelpers.traceLeave(log, "concat", traceId, target, targetOffset, sourceSegment);
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        val h = getHandle(handle);
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle);

        SegmentHandle headerHandle = h.getHeaderHandle();
        if (headerHandle == null) {
            // Directly delete the only SegmentChunk, and bubble up any exceptions if it doesn't exist.
            val subHandle = this.baseStorage.openWrite(h.lastChunk().getName());
            try {
                this.baseStorage.delete(subHandle);
                h.lastChunk().markInexistent();
                h.markDeleted();
            } catch (StreamSegmentNotExistsException ex) {
                h.lastChunk().markInexistent();
                h.markDeleted();
                throw ex;
            }
        } else {
            // We need to seal the whole Segment to prevent anyone else from creating new SegmentChunks while we're deleting
            // them, after which we delete all SegmentChunks and finally the header file.
            if (!h.isSealed()) {
                val writeHandle = h.isReadOnly() ? (RollingSegmentHandle) openWrite(handle.getSegmentName()) : h;
                seal(writeHandle);
            }

            deleteChunks(h, s -> true);
            try {
                this.headerStorage.delete(headerHandle);
                h.markDeleted();
            } catch (StreamSegmentNotExistsException ex) {
                h.markDeleted();
                throw ex;
            }
        }

        LoggerHelpers.traceLeave(log, "delete", traceId, handle);
    }

    @Override
    public void truncate(SegmentHandle handle, long truncationOffset) throws StreamSegmentException {
        // Delete all SegmentChunks which are entirely before the truncation offset.
        RollingSegmentHandle h = getHandle(handle);
        ensureNotDeleted(h);

        // The only acceptable case where we allow a read-only handle is if the Segment is sealed, since openWrite() will
        // only return a read-only handle in that case.
        Preconditions.checkArgument(h.isSealed() || !h.isReadOnly(), "Can only truncate with a read-only handle if the Segment is Sealed.");
        if (h.getHeaderHandle() == null) {
            // No header means the Segment is made up of a single SegmentChunk. We can't do anything.
            return;
        }

        long traceId = LoggerHelpers.traceEnter(log, "truncate", h, truncationOffset);
        Preconditions.checkArgument(truncationOffset >= 0 && truncationOffset <= h.length(),
                "truncationOffset must be non-negative and at most the length of the Segment.");
        val last = h.lastChunk();
        boolean chunksDeleted;
        if (last != null && canTruncate(last, truncationOffset) && !h.isSealed()) {
            // If we were asked to truncate the entire (non-sealed) Segment, then rollover at this point so we can delete
            // all existing data.
            rollover(h);

            // We are free to delete all chunks.
            chunksDeleted = deleteChunks(h, s -> canTruncate(s, truncationOffset));
        } else {
            // Either we were asked not to truncate the whole segment, or we were, and the Segment is sealed. If the latter,
            // then the Header is also sealed, we could not have done a quick rollover; as such we have no option but to
            // preserve the last chunk so that we can recalculate the length of the Segment if we need it again.
            chunksDeleted = deleteChunks(h, s -> canTruncate(s, truncationOffset) && s.getLastOffset() < h.length());
        }

        // Try to truncate the handle if we can.
        if (chunksDeleted && this.headerStorage.supportsReplace()) {
            truncateHandle(h);
        }

        LoggerHelpers.traceLeave(log, "truncate", traceId, h, truncationOffset);
    }

    @Override
    public boolean supportsTruncation() {
        return true;
    }

    @Override
    public Iterator<SegmentProperties> listSegments() throws IOException {
        return new RollingStorageSegmentIterator(this, this.baseStorage.listSegments(),
                props -> NameUtils.isHeaderSegment(props.getName()));
    }

    //endregion

    //region SegmentChunk Operations

    private void rollover(RollingSegmentHandle handle) throws StreamSegmentException {
        Preconditions.checkArgument(handle.getHeaderHandle() != null, "Cannot rollover a Segment with no header.");
        Preconditions.checkArgument(!handle.isReadOnly(), "Cannot rollover using a read-only handle.");
        Preconditions.checkArgument(!handle.isSealed(), "Cannot rollover a Sealed Segment.");
        log.debug("Rolling over '{}'.", handle);
        sealActiveChunk(handle);
        try {
            createChunk(handle);
        } catch (StreamSegmentExistsException ex) {
            // It may be possible that a concurrent rollover request using a different handle (either from this instance
            // or another) has already created the new chunk. Refresh the handle and try again. This is usually the case
            // with concurrent Storage instances trying to modify the same segment at the same time.
            int chunkCount = handle.chunks().size();
            handle.refresh(openHandle(handle.getSegmentName(), false));
            if (chunkCount == handle.chunks().size()) {
                // Nothing changed; re-throw the exception.
                throw ex;
            } else {
                // We've just refreshed the handle and picked up the latest chunk. Move on.
                log.warn("Aborted rollover due to concurrent rollover detected ('{}').", handle);
            }
        }
    }

    private void sealActiveChunk(RollingSegmentHandle handle) throws StreamSegmentException {
        SegmentHandle activeChunk = handle.getActiveChunkHandle();
        SegmentChunk last = handle.lastChunk();
        if (activeChunk != null && !last.isSealed()) {
            this.baseStorage.seal(activeChunk);
            handle.setActiveChunkHandle(null);
            last.markSealed();
            log.debug("Sealed active SegmentChunk '{}' for '{}'.", activeChunk.getSegmentName(), handle.getSegmentName());
        }
    }

    private void unsealLastChunkIfNecessary(RollingSegmentHandle handle) throws StreamSegmentException {
        SegmentChunk last = handle.lastChunk();
        if (last == null || !last.isSealed()) {
            // Nothing to do.
            return;
        }

        SegmentHandle activeChunk = handle.getActiveChunkHandle();
        boolean needsHandleUpdate = activeChunk == null;
        if (needsHandleUpdate) {
            // We didn't have a pointer to the active chunk's Handle because the chunk was sealed before open-write.
            activeChunk = this.baseStorage.openWrite(last.getName());
        }

        try {
            this.baseStorage.unseal(activeChunk);
        } catch (UnsupportedOperationException e) {
            log.warn("Unable to unseal SegmentChunk '{}' since base storage does not support unsealing.", last);
            return;
        }

        last.markUnsealed();
        if (needsHandleUpdate) {
            activeChunk = this.baseStorage.openWrite(last.getName());
            handle.setActiveChunkHandle(activeChunk);
        }

        log.debug("Unsealed active SegmentChunk '{}' for '{}'.", activeChunk.getSegmentName(), handle.getSegmentName());
    }

    private void createChunk(RollingSegmentHandle handle) throws StreamSegmentException {
        // Create new active SegmentChunk, only after which serialize the handle update and update the handle.
        // We ignore if the SegmentChunk exists and is empty - that's most likely due to a previous failed attempt.
        long segmentLength = handle.length();
        SegmentChunk newSegmentChunk = SegmentChunk.forSegment(handle.getSegmentName(), segmentLength);
        try {
            this.baseStorage.create(newSegmentChunk.getName());
        } catch (StreamSegmentExistsException ex) {
            checkIfEmptyAndNotSealed(ex, newSegmentChunk.getName());
        }

        serializeNewChunk(handle, newSegmentChunk);
        val activeHandle = this.baseStorage.openWrite(newSegmentChunk.getName());
        handle.addChunk(newSegmentChunk, activeHandle);
        log.debug("Created new SegmentChunk '{}' for '{}'.", newSegmentChunk, handle);
    }

    private boolean deleteChunks(RollingSegmentHandle handle, Predicate<SegmentChunk> canDelete) throws StreamSegmentException {
        boolean anyDeleted = false;
        for (SegmentChunk s : handle.chunks()) {
            if (s.exists() && canDelete.test(s)) {
                anyDeleted = true;
                try {
                    val subHandle = this.baseStorage.openWrite(s.getName());
                    this.baseStorage.delete(subHandle);
                    s.markInexistent();
                    log.debug("Deleted SegmentChunk '{}' for '{}'.", s, handle);
                } catch (StreamSegmentNotExistsException ex) {
                    // Ignore; It's OK if it doesn't exist; just make sure the handle is updated.
                    s.markInexistent();
                }
            }
        }
        return anyDeleted;
    }

    private boolean canTruncate(SegmentChunk segmentChunk, long truncationOffset) {
        // We should only truncate those SegmentChunks that are entirely before the truncationOffset. An empty SegmentChunk
        // that starts exactly at the truncationOffset should be spared (this means we truncate the entire Segment), as
        // we need that SegmentChunk to determine the actual length of the Segment.
        return segmentChunk.getStartOffset() < truncationOffset
                && segmentChunk.getLastOffset() <= truncationOffset;
    }

    private void refreshChunkExistence(RollingSegmentHandle handle) {
        // We check all SegmentChunks that we assume exist for actual existence (since once deleted, they can't come back).
        for (SegmentChunk s : handle.chunks()) {
            if (s.exists() && !this.baseStorage.exists(s.getName())) {
                s.markInexistent();
            }
        }
    }

    //endregion

    //region Header Operations

    private void createHeader(RollingSegmentHandle handle) throws StreamSegmentException {
        Preconditions.checkArgument(handle.getHeaderHandle() == null, "handle already has a header.");

        // Create a new Header SegmentChunk.
        String headerName = NameUtils.getHeaderSegmentName(handle.getSegmentName());
        this.headerStorage.create(headerName);
        val headerHandle = this.headerStorage.openWrite(headerName);

        // Create a new Handle and serialize it, after which update the original handle.
        val newHandle = new RollingSegmentHandle(headerHandle, handle.getRollingPolicy(), handle.chunks());
        serializeHandle(newHandle);
        handle.refresh(newHandle);
    }

    private boolean shouldConcatNatively(RollingSegmentHandle source, RollingSegmentHandle target) {
        if (source.getHeaderHandle() == null) {
            // Source does not have a Header, hence we cannot do Header concat.
            return true;
        }

        SegmentChunk lastSource = source.lastChunk();
        SegmentChunk lastTarget = target.lastChunk();
        return lastSource != null && lastSource.getStartOffset() == 0
                && lastTarget != null && !lastTarget.isSealed()
                && lastTarget.getLength() + lastSource.getLength() <= target.getRollingPolicy().getMaxLength();
    }

    private RollingSegmentHandle openHandle(String segmentName, boolean readOnly) throws StreamSegmentException {
        // Load up the handle from Storage.
        RollingSegmentHandle handle;
        try {
            // Attempt to open using Header.
            val headerInfo = getHeaderInfo(segmentName);
            val headerHandle = readOnly
                    ? this.headerStorage.openRead(headerInfo.getName())
                    : this.headerStorage.openWrite(headerInfo.getName());
            handle = readHeader(headerInfo, headerHandle);
        } catch (StreamSegmentNotExistsException ex) {
            // Header does not exist. Attempt to open Segment directly.
            val segmentHandle = readOnly ? this.baseStorage.openRead(segmentName) : this.baseStorage.openWrite(segmentName);
            handle = new RollingSegmentHandle(segmentHandle);
        }

        // Update each SegmentChunk's Length (based on offset difference) and mark them as Sealed.
        SegmentChunk last = null;
        for (SegmentChunk s : handle.chunks()) {
            if (last != null) {
                last.setLength(s.getStartOffset() - last.getStartOffset());
                last.markSealed();
            }

            last = s;
        }

        // For the last one, we need to actually check the file and update its info.
        if (last != null) {
            val si = this.baseStorage.getStreamSegmentInfo(last.getName());
            last.setLength(si.getLength());
            if (si.isSealed()) {
                last.markSealed();
                if (handle.getHeaderHandle() == null) {
                    handle.markSealed();
                }
            }
        }

        return handle;
    }

    private SegmentProperties getHeaderInfo(String segmentName) throws StreamSegmentException {
        String headerSegment = NameUtils.getHeaderSegmentName(segmentName);
        val headerInfo = this.headerStorage.getStreamSegmentInfo(headerSegment);
        if (headerInfo.getLength() == 0) {
            // We treat empty header files as inexistent segments.
            throw new StreamSegmentNotExistsException(segmentName);
        }

        return headerInfo;
    }

    private RollingSegmentHandle readHeader(SegmentProperties headerInfo, SegmentHandle headerHandle) throws StreamSegmentException {
        byte[] readBuffer = new byte[(int) headerInfo.getLength()];
        this.headerStorage.read(headerHandle, 0, readBuffer, 0, readBuffer.length);
        RollingSegmentHandle handle = HandleSerializer.deserialize(readBuffer, headerHandle);
        if (headerInfo.isSealed()) {
            handle.markSealed();
        }

        return handle;
    }

    private void serializeHandle(RollingSegmentHandle handle) throws StreamSegmentException {
        ByteArraySegment handleData = HandleSerializer.serialize(handle);
        try {
            this.headerStorage.write(handle.getHeaderHandle(), 0, handleData.getReader(), handleData.getLength());
            handle.setHeaderLength(handleData.getLength());
            log.debug("Header for '{}' fully serialized to '{}'.", handle.getSegmentName(), handle.getHeaderHandle().getSegmentName());
        } catch (BadOffsetException ex) {
            // If we get BadOffsetException when writing the Handle, it means it was modified externally.
            throw new StorageNotPrimaryException(handle.getSegmentName(), ex);
        }
    }

    private void truncateHandle(RollingSegmentHandle handle) throws StreamSegmentException {
        handle.excludeInexistentChunks();
        ByteArraySegment handleData = HandleSerializer.serialize(handle);
        this.headerStorage.replace(handle.getHeaderHandle(), handleData);
        handle.setHeaderLength(handleData.getLength());
        log.debug("Header for '{}' fully serialized (replaced) to '{}'.", handle.getSegmentName(), handle.getHeaderHandle().getSegmentName());
    }

    private void serializeNewChunk(RollingSegmentHandle handle, SegmentChunk newSegmentChunk) throws StreamSegmentException {
        updateHandle(handle, HandleSerializer.serializeChunk(newSegmentChunk));
    }

    private void serializeBeginConcat(RollingSegmentHandle targetHandle, RollingSegmentHandle sourceHandle) throws StreamSegmentException {
        byte[] updateData = HandleSerializer.serializeConcat(sourceHandle.chunks().size(), targetHandle.length());
        updateHandle(targetHandle, updateData);
    }

    private void updateHandle(RollingSegmentHandle handle, byte[] data) throws StreamSegmentException {
        try {
            this.headerStorage.write(handle.getHeaderHandle(), handle.getHeaderLength(), new ByteArrayInputStream(data), data.length);
            handle.increaseHeaderLength(data.length);
            log.debug("Header for '{}' updated with {} bytes for a length of {}.", handle.getSegmentName(), data.length, handle.getHeaderLength());
        } catch (BadOffsetException ex) {
            // If we get BadOffsetException when writing the Handle, it means it was modified externally.
            throw new StorageNotPrimaryException(handle.getSegmentName(), ex);
        }
    }

    //endregion

    //region Helpers

    private List<SegmentChunk> rebase(List<SegmentChunk> segmentChunks, long newStartOffset) {
        AtomicLong segmentOffset = new AtomicLong(newStartOffset);
        return segmentChunks.stream()
                .map(s -> s.withNewOffset(segmentOffset.getAndAdd(s.getLength())))
                .collect(Collectors.toList());
    }

    @SneakyThrows(StreamingException.class)
    private void checkTruncatedSegment(StreamingException ex, RollingSegmentHandle handle, SegmentChunk segmentChunk) {
        if (ex != null && (Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException) || !segmentChunk.exists()) {
            // We ran into a SegmentChunk that does not exist (either marked as such or due to a failed read).
            segmentChunk.markInexistent();
            String message = String.format("Offsets %d-%d have been deleted.", segmentChunk.getStartOffset(), segmentChunk.getLastOffset());
            ex = new StreamSegmentTruncatedException(handle.getSegmentName(), message, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    private void checkIfEmptyAndNotSealed(StreamSegmentExistsException ex, String chunkName) throws StreamSegmentException {
        checkIfEmptyAndNotSealed(ex, chunkName, this.baseStorage);
    }

    private void checkIfEmptyAndNotSealed(StreamSegmentExistsException ex, String chunkName, SyncStorage storage) throws StreamSegmentException {
        // SegmentChunk exists, check if it's empty and not sealed.
        try {
            val si = storage.getStreamSegmentInfo(chunkName);
            if (si.getLength() > 0 || si.isSealed()) {
                throw ex;
            }
        } catch (StreamSegmentNotExistsException notExists) {
            // nothing to do.
        }
    }

    private RollingSegmentHandle getHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle instanceof RollingSegmentHandle, "handle must be of type RollingSegmentHandle.");
        return (RollingSegmentHandle) handle;
    }

    private void ensureWritable(RollingSegmentHandle handle) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only (%s).", handle.getSegmentName());
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

    private void ensureOffset(RollingSegmentHandle handle, long offset) throws StreamSegmentException {
        if (offset != handle.length()) {
            // Force-refresh the handle to make sure it is still in sync with reality. Make sure we open a read handle
            // so that we don't force any sort of fencing during this process.
            val refreshedHandle = openHandle(handle.getSegmentName(), true);
            handle.refresh(refreshedHandle);
            log.debug("Handle refreshed: {}.", handle);
            if (offset != handle.length()) {
                // Still in disagreement; throw exception.
                throw new BadOffsetException(handle.getSegmentName(), handle.length(), offset);
            }
        }
    }

    /**
     * Iterator for segments in Rolling storage.
     */
    private static class RollingStorageSegmentIterator implements Iterator<SegmentProperties> {
        private final RollingStorage instance;
        private final Iterator<SegmentProperties> results;

        RollingStorageSegmentIterator(RollingStorage instance, Iterator<SegmentProperties> results, java.util.function.Predicate<SegmentProperties> patternMatchPredicate) {
            this.instance = instance;
            this.results = StreamSupport.stream(Spliterators.spliteratorUnknownSize(results, 0), false)
                    .filter(patternMatchPredicate)
                    .map(this::toSegmentProperties)
                    .iterator();
        }

        public SegmentProperties toSegmentProperties(SegmentProperties segmentProperties) {
            try {
                String segmentName = NameUtils.getSegmentNameFromHeader(segmentProperties.getName());
                val handle = instance.openHandle(segmentName, true);
                return StreamSegmentInformation.builder()
                        .name(segmentName)
                        .length(handle.length())
                        .sealed(handle.isSealed()).build();
            } catch (StreamSegmentException e) {
                log.error("Exception occurred while transforming the object into SegmentProperties.");
                return null;
            }
        }

        /**
         * Method to check the presence of next element in the iterator.
         * @return true if the next element is there, else false.
         */
        @Override
        public boolean hasNext() {
            return results.hasNext();
        }

        /**
         * Method to return the next element in the iterator.
         * @return A newly created StreamSegmentInformation class.
         * @throws NoSuchElementException in case of an unexpected failure.
         */
        @Override
        public SegmentProperties next() throws NoSuchElementException {
            return results.next();
        }
    }
    //endregion
}
