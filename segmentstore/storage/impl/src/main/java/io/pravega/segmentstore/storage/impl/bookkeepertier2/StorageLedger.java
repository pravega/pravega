/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeepertier2;

import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.curator.framework.api.transaction.CuratorOp;

/*
* Storage Ledger represent a single segment. A segment is rolled over whenever a ownership change is observed.
*
**/
public class StorageLedger {

    private final StorageLedgerManager manager;

    @Getter
    private final String name;

    private ConcurrentHashMap<Integer, LedgerData> dataMap;

    @Getter
    private boolean sealed;

    @Getter
    private int length;

    @Getter
    private ImmutableDate lastModified;

    public StorageLedger(StorageLedgerManager storageLedgerManager, String streamSegmentName) {
        manager = storageLedgerManager;
        dataMap = new ConcurrentHashMap<>();
        this.name = streamSegmentName;
    }

    public CompletableFuture<LedgerData> getLedgerDataForWriteAt(long offset) {
        CompletableFuture<LedgerData> retVal = new CompletableFuture<>();
        if (offset != length) {
            retVal.completeExceptionally(new BadOffsetException(this.getName(), length, offset));
        }
        Optional<Map.Entry<Integer, LedgerData>> found = dataMap.entrySet().stream().filter(entry -> (entry.getKey() <= offset) && (offset <= (entry.getKey() + entry.getValue().getLength()))).findFirst();
        if (found.isPresent()) {
            retVal.complete(found.get().getValue());
        } else {
            return manager.createLedgerAt(this.name, (int) offset).thenApply(data -> {
                this.dataMap.put((int) offset, data);
                return data;
        });
        }
        return retVal;
    }

    public void addToList(int offset, LedgerData ledgerData) {
        this.dataMap.put(offset, ledgerData);
    }

    public synchronized void increaseLengthBy(int size) {
        this.length += size;
    }

    public synchronized CompletableFuture<Void> deleteAllLedgers() {
        return CompletableFuture.allOf(
                this.dataMap.entrySet().stream().map(entry -> manager.deleteLedger(entry.getValue().getLh())).toArray(CompletableFuture[]::new));
    }

    public LedgerData getLastLedgerData() {
        return this.dataMap.entrySet().stream().max((entry1, entry2) ->  entry1.getKey() - entry2.getKey()).get().getValue();
    }

    public void setSealed() {
        sealed = true;
    }

    public List<CuratorOp> addLedgerDataFrom(StorageLedger source) {
        List<CuratorOp> retVal = source.dataMap.entrySet().stream().map(entry -> {
            int newKey = entry.getKey() + this.length;
            this.dataMap.put(newKey, entry.getValue());
            return manager.createAddOp(this.name, newKey, entry.getValue());
        }).collect(Collectors.toList());

        this.length  += source.length;
        return retVal;
    }

    public CompletableFuture<LedgerData> getLedgerDataForReadAt(long offset) {
        CompletableFuture<LedgerData> retVal = new CompletableFuture<>();
        if (offset >= length) {
            retVal.completeExceptionally(new BadOffsetException(this.getName(), length, offset));
        }
        Optional<Map.Entry<Integer, LedgerData>> found = dataMap.entrySet().stream().filter(entry -> (entry.getKey() <= offset) && (offset < (entry.getKey() + entry.getValue().getLength()))).findFirst();
        if (found.isPresent()) {
            retVal.complete(found.get().getValue());
        } else {
            retVal.completeExceptionally(new BadOffsetException(this.getName(), length, offset));
        }
        return retVal;
    }
}
