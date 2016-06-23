package com.emc.logservice.server.reading;

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryContents;

import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;

/**
 * Read Result Entry for data that is readily available for reading (in memory).
 */
public class MemoryReadResultEntry extends ReadResultEntry {
    private final CompletableFuture<ReadResultEntryContents> dataStream;

    /**
     * Creates a new instance of the MemoryReadResultEntry class.
     *
     * @param entry The ByteArrayReadIndexEntry to create the Result Entry from.
     * @throws IndexOutOfBoundsException If entryOffset, length or both are invalid.
     */
    public MemoryReadResultEntry(ByteArrayReadIndexEntry entry, int entryOffset, int length) {
        super(entry.getStreamSegmentOffset() + entryOffset, length);
        Exceptions.checkArgument(entryOffset >= 0, "entryOffset", "EntryOffset must be non-negative.");
        Exceptions.checkArgument(length > 0, "length", "Length must be a positive integer.");
        Exceptions.checkArgument(entryOffset + length <= entry.getLength(), "entryOffset + length", "EntryOffset + Length must be less than the size of the entry data.");

        // Data Stream is readily available.
        this.dataStream = CompletableFuture.completedFuture(new ReadResultEntryContents(new ByteArrayInputStream(entry.getData(), entryOffset, length), length));
    }

    @Override
    public CompletableFuture<ReadResultEntryContents> getContent() {
        return this.dataStream;
    }
}
