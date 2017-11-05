/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeperstorage;

import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import org.apache.curator.framework.api.transaction.CuratorOp;

/*
* Storage Log represents a single segment. A segment is rolled over whenever a ownership change is observed.
*
**/
class LogStorage {

    private final LogStorageManager manager;

    @Getter
    private final String name;

    @Getter
    private final boolean readOnlyHandle;

    private final ConcurrentHashMap<Integer, LedgerData> dataMap;

    @Getter
    private int updateVersion;

    @Getter
    private long containerEpoch;

    @GuardedBy("this")
    @Getter
    private boolean sealed;

    @GuardedBy("this")
    @Getter
    private long length;

    @Getter
    private ImmutableDate lastModified;

    LogStorage(LogStorageManager logStorageManager, String streamSegmentName, int updateVersion, long containerEpoch, int sealed, boolean readOnly) {
        this.manager = logStorageManager;
        this.dataMap = new ConcurrentHashMap<>();
        this.name = streamSegmentName;
        this.readOnlyHandle = readOnly;
        if ( sealed == 1) {
            this.sealed = true;
        } else {
            this.sealed = false;
        }
        this.containerEpoch = containerEpoch;
        this.updateVersion = updateVersion;
    }

    /**
     * Returns the BK ledger which has the given offset and is writable.
     * @param offset offset from which writes start.
     * @return The metadata of the ledger.
     */
    CompletableFuture<LedgerData> getLedgerDataForWriteAt(long offset) {
        CompletableFuture<LedgerData> retVal = new CompletableFuture<>();
        if (offset != length) {
            retVal.completeExceptionally(new BadOffsetException(this.getName(), length, offset));
            return retVal;
        }
        LedgerData ledgerData = this.getLastLedgerData();
        if (ledgerData != null && !ledgerData.getLedgerHandle().isClosed()) {
            retVal.complete(ledgerData);
        } else {
            // If there is no ledger, create a new one.
            retVal = manager.createLedgerAt(this.name, (int) offset).thenApply(data -> {
                this.dataMap.put((int) offset, data);
                return data;
            });
        }
        return retVal;
    }

    /**
     * Add a new BK ledger and metadata at a given offset.
     * @param offset The starting offset represented by the ledger.
     * @param ledgerData metadata of the ledger.
     */
    synchronized void addToList(int offset, LedgerData ledgerData) {

        // If we are replacing an existing ledger, adjust the length

        LedgerData older = this.dataMap.put(offset, ledgerData);
        if (older != null) {
            this.length -= older.getLedgerHandle().getLength();
        }
        this.length += ledgerData.getLedgerHandle().getLength();
    }

    /**
     * Increase length of the LogStorage as a side effect of the write operation.
     * This is just a cache operation. The length is not persisted.
     *
     * @param size size of data written.
     */
    synchronized void increaseLengthBy(int size) {
        this.length += size;
    }

    synchronized CompletableFuture<Void> deleteAllLedgers() {
        return CompletableFuture.allOf(
                this.dataMap.entrySet().stream().map(entry -> manager.deleteLedger(entry.getValue().getLedgerHandle())).toArray(CompletableFuture[]::new));
    }

    LedgerData getLastLedgerData() {
        if (this.dataMap.isEmpty()) {
            return null;
        } else {
            return this.dataMap.entrySet().stream().max(Comparator.comparingInt(Map.Entry::getKey)).get().getValue();
        }
    }

    synchronized void markSealed() {
        this.sealed = true;
    }

    /**
     * Creates a list of curator transaction for merging source LogStorage in to this.
     * @param source Name of the source ledger.
     * @return list of curator operations.
     */
    synchronized List<CuratorOp> addLedgerDataFrom(LogStorage source) {
        List<CuratorOp> retVal = source.dataMap.entrySet().stream().map(entry -> {
            int newKey = (int) (entry.getKey() + this.length);
            this.dataMap.put(newKey, entry.getValue());
            return manager.createAddOp(this.name, newKey, entry.getValue());
        }).collect(Collectors.toList());

        this.length += source.length;
        return retVal;
    }

    synchronized LedgerData getLedgerDataForReadAt(long offset) {
        if (offset >= length) {
            throw new CompletionException( new BadOffsetException(this.getName(), length, offset));
        }
        Optional<Map.Entry<Integer, LedgerData>> found = dataMap.entrySet().stream().filter(entry -> (entry.getKey() <= offset) && (offset < (entry.getKey() + entry.getValue().getLength()))).findFirst();
        if (found.isPresent()) {
            return found.get().getValue();
        } else {
            throw new CompletionException(new BadOffsetException(this.getName(), length, offset));
        }
    }

    public static LogStorage deserialize(LogStorageManager manager, String segmentName, byte[] bytes, int version, boolean readOnly) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long epoc = bb.getLong();
        int sealed = bb.getInt();

        return new LogStorage(manager, segmentName, version, epoc, sealed, readOnly);
    }

    void setContainerEpoch(long containerEpoch) {
        this.containerEpoch = containerEpoch;
    }

    public byte[] serialize() {
        int size = Long.SIZE + Integer.SIZE;
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.putLong(this.containerEpoch);
        if (this.sealed) {
            bb.putInt(1);
        } else {
            bb.putInt(0);
        }

        return bb.array();
    }

    synchronized void setUpdateVersion(int version) {
        this.updateVersion = version;
    }
}
