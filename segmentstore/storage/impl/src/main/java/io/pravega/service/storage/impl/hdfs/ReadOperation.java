/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage.impl.hdfs;

import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.util.CollectionHelpers;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * FileSystemOperation that Reads from a Segment.
 */
@Slf4j
public class ReadOperation extends FileSystemOperation<HDFSSegmentHandle> implements Callable<Integer> {
    private static final int MAX_ATTEMPT_COUNT = 3;
    private final long offset;
    private final byte[] buffer;
    private final int bufferOffset;
    private final int length;

    /**
     * Creates a new instance of the ReadOperation class.
     *
     * @param handle       A Read or ReadWrite handle for the Segment to read from.
     * @param offset       The offset in the Segment to begin reading at.
     * @param buffer       A buffer to load read data into.
     * @param bufferOffset An offset in the buffer to start loading the data at.
     * @param length       The number of bytes to read.
     * @param context      Context for the operation.
     */
    ReadOperation(HDFSSegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, OperationContext context) {
        super(handle, context);
        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
            throw new ArrayIndexOutOfBoundsException(String.format(
                    "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                    offset, bufferOffset, length, buffer.length));
        }

        this.offset = offset;
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
        this.length = length;
    }

    @Override
    public Integer call() throws IOException {
        HDFSSegmentHandle handle = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "read", handle, this.offset, this.length);
        Timer timer = new Timer();

        // Make sure arguments are valid. Refresh the handle if needed (and allowed).
        validateOffsetAndRefresh(handle);

        int attemptCount = 0;
        AtomicInteger totalBytesRead = new AtomicInteger();
        boolean needsRefresh = true;
        while (needsRefresh && attemptCount < MAX_ATTEMPT_COUNT) {
            attemptCount++;
            try {
                // Read data.
                read(handle, totalBytesRead);
                needsRefresh = false;
            } catch (FileNotFoundException fnf) {
                if (!handle.isReadOnly() || attemptCount >= MAX_ATTEMPT_COUNT) {
                    // FileNotFound is only expected in read-only handles, since another read-write handle may have compacted
                    // the files together. If this is indeed a read-write handle, there is no point in retrying.
                    // Also throw this if we tried enough times.
                    throw fnf;
                }

                log.info("Unable to read from file '{}' (attempt {}/{}). Refreshing and retrying.", fnf.getMessage(), attemptCount, MAX_ATTEMPT_COUNT);
                refreshHandle(handle);
            }
        }

        Metrics.READ_LATENCY.reportSuccessEvent(timer.getElapsed());
        Metrics.READ_BYTES.add(totalBytesRead.get());
        LoggerHelpers.traceLeave(log, "read", traceId, handle, this.offset, totalBytesRead);
        return totalBytesRead.get();
    }

    private void read(HDFSSegmentHandle handle, AtomicInteger totalBytesRead) throws IOException {
        val handleFiles = handle.getFiles();
        int currentFileIndex = CollectionHelpers.binarySearch(handleFiles, this::compareToStartOffset);
        assert currentFileIndex >= 0 : "unable to locate first file index.";
        while (totalBytesRead.get() < this.length && currentFileIndex < handleFiles.size()) {
            FileDescriptor currentFile = handleFiles.get(currentFileIndex);
            long fileOffset = this.offset + totalBytesRead.get() - currentFile.getOffset();
            int fileReadLength = (int) Math.min(this.length - totalBytesRead.get(), currentFile.getLength() - fileOffset);
            assert fileOffset >= 0 && fileReadLength >= 0 : "negative file read offset or length";

            try (FSDataInputStream stream = this.context.fileSystem.open(currentFile.getPath())) {
                stream.readFully(fileOffset, this.buffer, this.bufferOffset + totalBytesRead.get(), fileReadLength);
                totalBytesRead.addAndGet(fileReadLength);
            } catch (EOFException ex) {
                throw new IOException(
                        String.format("Internal error while reading segment file. Attempted to read file '%s' at offset %d, length %d.",
                                currentFile, fileOffset, fileReadLength),
                        ex);
            }

            currentFileIndex++;
        }
    }

    private void validateOffsetAndRefresh(HDFSSegmentHandle handle) throws IOException {
        long lastFileOffset = handle.getLastFile().getLastOffset();
        boolean refreshed = false;
        while (this.offset + this.length > lastFileOffset) {
            if (!refreshed && handle.isReadOnly()) {
                //Read-only handles are not updated internally; they require a refresh.
                refreshHandle(handle);
                lastFileOffset = handle.getLastFile().getLastOffset();
                refreshed = true;
            } else {
                // We either refreshed or we have a read-write handle, which is always updated internally, but not by us.
                throw new IllegalArgumentException(
                        String.format("Offset %d + length %d is beyond the last offset %d of the segment (using read-write handle).",
                                this.offset, this.length, lastFileOffset));
            }
        }
    }

    private void refreshHandle(HDFSSegmentHandle handle) throws IOException {
        val systemFiles = findAll(handle.getSegmentName(), true);
        handle.replaceFiles(systemFiles);
    }

    private int compareToStartOffset(FileDescriptor fi) {
        if (this.offset < fi.getOffset()) {
            return -1;
        } else if (this.offset >= fi.getLastOffset()) {
            return 1;
        } else {
            return 0;
        }
    }
}
