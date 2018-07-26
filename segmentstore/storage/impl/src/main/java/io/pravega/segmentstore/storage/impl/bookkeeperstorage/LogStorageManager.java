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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * This class represents the manager for StorageLedgers.
 * It is responsible for creating and deleting storage ledger objects in bookkeeper.
 * This objects interacts with Zookeeper through Async curator framework to manage the metadata.
 */
@Slf4j
class LogStorageManager {
    private static final BookKeeper.DigestType LEDGER_DIGEST_TYPE = BookKeeper.DigestType.MAC;

    private final BookKeeperStorageConfig config;
    private final CuratorFramework zkClient;

    private final ConcurrentHashMap<String, LogStorage> ledgers;
    private BookKeeper bookkeeper;
    private AtomicLong containerEpoch = new AtomicLong();

    LogStorageManager(BookKeeperStorageConfig config, CuratorFramework zkClient) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(zkClient, "zkClient");

        this.config = config;
        this.zkClient = zkClient;
        ledgers = new ConcurrentHashMap<>();
    }

    //region Storage API

    /**
     * Creates a LogStorage at location streamSegmentName.
     *
     * @param streamSegmentName Full name of the stream segment
     * @return A new instance of LogStorage if one does not already exist
     * @Param bookkeeper the BK instance
     * @Param zkClient
     */
    public LedgerData create(String streamSegmentName) throws StreamSegmentException {
        log.info("Creating segment {}", streamSegmentName);
        LogStorage logStorage = new LogStorage(streamSegmentName, 0, this.containerEpoch.get(), false);

        /* Create the node for the segment in the ZK. */
        try {
            zkClient.create()
                    .forPath(getZkPath(streamSegmentName), logStorage.serialize());
            synchronized (this) {
                ledgers.put(streamSegmentName, logStorage);
            }
            LedgerData data = getLedgerDataForWriteAt(logStorage, 0);
            return data;
        } catch (Exception exc) {
            throw translateZKException(streamSegmentName, exc);
        }
    }


    /**
     * Fences an segment which already exists.
     * Steps involved:
     * 1. Read the latest ledger from ZK
     * 2. Try to open it for write
     * 3. If it is fenced, create a new one.
     *
     * @param streamSegmentName name of the segment to be fenced.
     */
    public LogStorage fence(String streamSegmentName) throws StreamSegmentException {

        log.info("Sealing ledger for segment {}", streamSegmentName);
        /** Get the LogStorage metadata. */
        LogStorage ledger = getOrRetrieveStorageLedger(streamSegmentName);

        /** check whether fencing is required. */
        if (ledger.getContainerEpoch() == containerEpoch.get()) {
            return ledger;
        } else if (ledger.getContainerEpoch() > containerEpoch.get()) {
            throw new CompletionException(new StorageNotPrimaryException(streamSegmentName));
        } else {
           return fenceWithUpdate(ledger, streamSegmentName);
        }
    }

    private LogStorage fenceWithUpdate(LogStorage ledger, String streamSegmentName) throws StreamSegmentException {
        boolean tryAgain = true;
        ledger.setContainerEpoch(containerEpoch.get());
        Stat stat = null;

        while (tryAgain) {
            try {
                stat = zkClient.setData()
                               .withVersion(ledger.getUpdateVersion())
                               .forPath(getZkPath(streamSegmentName), ledger.serialize());
                tryAgain = false;
            } catch (Exception exc) {
                if (Exceptions.unwrap(exc) instanceof KeeperException.BadVersionException) {
                    //Need to retry as data we had was out of sync
                    tryAgain = true;
                } else {
                    throw translateZKException(streamSegmentName, exc);
                }
            }
        }
        ledger.setUpdateVersion(stat.getVersion());
        /** Fence out all the ledgers and create a new one at the end for appends. */
        log.info("Fencing all the ledgers for {}", streamSegmentName);
        return fenceLedgersAndCreateOneAtEnd(streamSegmentName, ledger);
    }

    /**
     * Initializes the BookKeeper and curator objects.
     *
     * @param containerEpoch the epoc to be used for the fencing and create calls.
     */
    public void initialize(long containerEpoch) {
        this.containerEpoch.set(containerEpoch);
        int entryTimeout = (int) Math.ceil(this.config.getBkWriteTimeoutMillis() / 1000.0);
        int readTimeout = (int) Math.ceil(this.config.getBkReadTimeoutMillis() / 1000.0);
        ClientConfiguration config = new ClientConfiguration()
                .setZkServers(this.config.getZkAddress())
                .setClientTcpNoDelay(true)
                .setAddEntryTimeout(entryTimeout)
                .setReadEntryTimeout(readTimeout)
                .setClientConnectTimeoutMillis((int) this.config.getZkConnectionTimeout().toMillis())
                .setZkTimeout((int) this.config.getZkConnectionTimeout().toMillis());
        if (this.config.getBkLedgerPath().isEmpty()) {
            config.setZkLedgersRootPath("/" + zkClient.getNamespace() + "/bookkeeper/ledgers");
        } else {
            config.setZkLedgersRootPath(this.config.getBkLedgerPath());
        }
        try {
            bookkeeper = new BookKeeper(config);
        } catch (Exception e) {
            throw new CompletionException(e);
        }
    }

    /**
     * API to detect the existance of a stream segment.
     * @param streamSegmentName name of the segment.
     * @return A CompletableFuture which holds the boolean value.
     */
    public boolean exists(String streamSegmentName) {
        try {
            return zkClient.checkExists().forPath(getZkPath(streamSegmentName)) != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * API to read data.
     * @param segmentName name of the segment.
     * @param offset  offset into the segment.
     * @param buffer  Buffer to read the data in.
     * @param bufferOffset starting offset inside the buffer.
     * @param length Size of the data to be read.
     * @return  A CompletableFuture which contains the actual read size once the read is complete.
     */
    public int read(String segmentName, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        int currentLength = length;
        long currentOffset = offset;
        int currentBufferOffset = bufferOffset;

        try {
            LogStorage ledger = getOrRetrieveStorageLedger(segmentName);
            Preconditions.checkArgument(offset + length <= ledger.getLength(), segmentName);
            /** Loop till the data is read completely. */
            while (currentLength != 0) {
                /* Get the BK ledger which contains the offset. */
                /** Read data from the BK ledger. */
                int dataRead = 0;
                dataRead = readDataFromLedger(ledger.getLedgerDataForReadAt(currentOffset),
                        currentOffset,
                        buffer, currentBufferOffset, currentLength);

                /** Update the remaining lengths and offsets. */
                Preconditions.checkState(dataRead != 0, "No data read");
                currentLength -= dataRead;
                currentOffset += dataRead;
                currentBufferOffset += dataRead;
            }
        } catch (Exception e) {
            throw translateZKException(segmentName, e);
        }
        return length;
    }

    /**
     * API to write data.
     * @param segmentName name of the segment.
     * @param offset Offset in the segment at which the write starts.
     * @param data  Data to be written.
     * @param length size of the data to be written.
     * @return A CompletableFuture which completes once the write is complete.
     */
    public int write(String segmentName, long offset, InputStream data, int length) throws StreamSegmentException {
        log.info("Writing {} at offset {} for segment {}", length, offset, segmentName);

        LogStorage ledger = getOrRetrieveStorageLedger(segmentName);
        if (ledger.getLength() != offset) {
            throw new CompletionException(new BadOffsetException(segmentName, ledger.getLength(), offset));
        }
        if (ledger.isSealed()) {
            throw new CompletionException(new StreamSegmentSealedException(segmentName));
        }
        /* Get the last ledger where data can be appended. */
        LedgerData ledgerData = null;
        ledgerData = getORCreateLHForOffset(ledger, offset);
        writeDataAt(ledgerData.getLedgerHandle(), offset, data, length, segmentName);
        /* Update lengths in the cache. The lengths are not persisted. */
        ledgerData.increaseLengthBy(length);
        ledgerData.setLastAddConfirmed(ledgerData.getLastAddConfirmed() + 1);
        synchronized (this) {
            ledgers.get(segmentName).increaseLengthBy(length);
        }
        return length;
    }

    /**
     * API to seal a given segment.
     * @param segmentName name of the segment.
     * @return A CompletableFuture which completes once the seal operation is complete.
     */
    public void seal(String segmentName) throws StreamSegmentException {
        LogStorage ledger = this.getOrRetrieveStorageLedger(segmentName);
        /** Check whether this segmentstore is the current owner. */
        if (ledger.getContainerEpoch() > this.containerEpoch.get()) {
            throw new CompletionException(new StorageNotPrimaryException(segmentName));
        }
        ledger.markSealed();
        /** Update the details in ZK.*/
        try {
            Stat stat = zkClient.setData()
                                .withVersion(ledger.getUpdateVersion())
                                .forPath(getZkPath(segmentName), ledger.serialize());
            ledger.incrementUpdateVersion();
            /** Seal the last ledger. This is the only ledger which can be written to. */
            log.debug("Sealing segment {}", segmentName);
            this.sealLedger(ledger.getLastLedgerData());
        } catch (Exception exc) {
            throw translateZKException(segmentName, exc);
        }
    }

    /**
     * Concatenates the sourceSegment in to the target segment at offset.
     *
     * The concatenation involves updating the metadata in a single ZK transaction.
     * The operations are:
     * 1. Add the ledgers of the source segment to the target metadata at their new offset.
     * 2. Remove them from the source metadata.
     * 3. Update the target version to ensure CAS.
     *
     * @param segmentName the target segment.
     * @param sourceSegment The segment to be merged to target.
     * @param offset offset at which the merge happens.
     * @return A completable future which completes once the concat operation is complete.
     */
    public void concat(String segmentName, String sourceSegment, long offset) throws StreamSegmentException {
        List<CuratorOp> curatorOps;
        try {
        curatorOps = this.getZKOperationsForConcat(segmentName, sourceSegment, offset);
            List<CuratorTransactionResult> results = zkClient.transaction()
                                                             .forOperations(curatorOps);
            LogStorage logStorage = getOrRetrieveStorageLedger(segmentName);
            logStorage.incrementUpdateVersion();
            /* Fence all the ledgers and add one at the end. */
            fenceLedgersAndCreateOneAtEnd(segmentName, logStorage);
            /** Delete metadata and cache for the source once the operation is complete. */
            this.zkClient.delete().deletingChildrenIfNeeded().forPath(getZkPath(sourceSegment));
            ledgers.remove(sourceSegment);
        } catch (Exception exc) {
            /** In case of exception, drop the corrupt cache. */
            ledgers.remove(segmentName);
            throw translateZKException(segmentName, exc);
        }
    }

    /**
     * Deletes a segment along with all the data and metadata.
     * @param segmentName name of the segment to be deleted.
     * @return A CompletableFuture which completes once the delete is complete.
     */
    public void delete(String segmentName) throws StreamSegmentException {
        LogStorage ledger = this.getOrRetrieveStorageLedger(segmentName);
        if (ledger.getContainerEpoch() > this.containerEpoch.get()) {
            throw new CompletionException(new StorageNotPrimaryException(segmentName));
        }
        try {
            this.zkClient.delete()
                         .deletingChildrenIfNeeded()
                         .forPath(getZkPath(segmentName));
        } catch (Exception e) {
           log.warn("Exception while deleting a segment {}", segmentName, e);

        }
        ledgers.remove(segmentName);
        deleteAllLedgers(ledger);
    }

    void deleteAllLedgers(LogStorage ledger) {
        synchronized (ledger) {
            ledger.getDataMap().entrySet().stream().forEach(entry -> deleteLedger(entry.getValue().getLedgerHandle()));
        }
    }

    //endregion


    public LedgerData createLedgerAt(String streamSegmentName, int offset) {

        log.info("Creating ledger for {} at {}", streamSegmentName, offset);
        try {
            LedgerHandle ledgerHandle = bookkeeper.createLedger(config.getBkEnsembleSize(),
                    config.getBkWriteQuorumSize(), LEDGER_DIGEST_TYPE, config.getBKPassword());
            LedgerData lh = new LedgerData(ledgerHandle, offset, 0, this.containerEpoch.get());
            zkClient.create().forPath(getZkPath(streamSegmentName) + "/" + offset, lh.serialize());
            return lh;
        } catch (Exception e) {
            throw new CompletionException(e);
        }
    }

    void deleteLedger(LedgerHandle lh) {
        Exceptions.handleInterrupted(() -> {
            try {
                bookkeeper.deleteLedger(lh.getId());
            } catch (BKException e) {
                log.warn("Delete ledger failed. Continuing as we gave it our best shot");
            }
        });
    }

    public CuratorOp createAddOp(String segmentName, int newKey, LedgerData value) throws StreamSegmentException {
        try {
            return zkClient.transactionOp().create().forPath(getZkPath(segmentName) + "/" + newKey, value.serialize());
        } catch (Exception e) {
            throw translateZKException(segmentName, e);
        }
    }

    /**
     * Returns details about a specific storageledger.
     * If the ledger does not exist in the cache, we try to
     *
     * @param streamSegmentName name of the stream segment
     * @return Properties of the given segment.
     */
    public SegmentProperties getOrRetrieveStorageLedgerDetails(String streamSegmentName) throws StreamSegmentException {
        LogStorage ledger = getOrRetrieveStorageLedger(streamSegmentName);
        return StreamSegmentInformation.builder()
                                       .name(streamSegmentName)
                                       .length(ledger.getLength())
                                       .sealed(ledger.isSealed())
                                       .lastModified(ledger.getLastModified())
                                       .build();
    }


    //region private helper methods to interact with BK and ZK metadata

    private Throwable translateBKException(BKException e, String segmentName) {
        if (e instanceof BKException.BKLedgerClosedException
                || e instanceof BKException.BKIllegalOpException
                || e instanceof BKException.BKLedgerFencedException) {
            return new StorageNotPrimaryException(segmentName);
        }
        return e;
    }

    private LedgerData getORCreateLHForOffset(LogStorage ledger, long offset) throws BadOffsetException {
        return getLedgerDataForWriteAt(ledger, offset);
    }

    /**
     * Returns the BK ledger which has the given offset and is writable.
     *
     * @param ledger
     * @param offset offset from which writes start.
     * @return The metadata of the ledger.
     */
    LedgerData getLedgerDataForWriteAt(LogStorage ledger, long offset) throws BadOffsetException {
        synchronized (ledger) {
            if (offset != ledger.getLength()) {
                throw new BadOffsetException(ledger.getName(), ledger.getLength(), offset);
            }
            LedgerData ledgerData = ledger.getLastLedgerData();
            if (ledgerData != null && !ledgerData.getLedgerHandle().isClosed()) {
                return ledgerData;
            } else {
                // If there is no ledger, create a new one.
                LedgerData data = createLedgerAt(ledger.getName(), (int) offset);
                ledger.getDataMap().put((int) offset, data);
                return data;
            }
        }
    }

    private void sealLedger(LedgerData lastLedgerData) {
        try {
            lastLedgerData.getLedgerHandle().close();
        } catch (Exception e) {
            log.warn("Exception {} while closing the last ledger", e);
        }
    }

    private LedgerData deserializeLedgerData(Integer startOffset, byte[] bytes, Stat stat) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        LedgerHandle lh = null;
        long ledgerId = bb.getLong();
        log.info("Opening a ledger with ledger id {}", ledgerId);

        try {
                lh = bookkeeper.openLedger(ledgerId, LEDGER_DIGEST_TYPE, config.getBKPassword());
        } catch (Exception e) {
            log.warn("Exception {} while opening ledger id {}", e, ledgerId);
            throw new CompletionException(e);
        }
        LedgerData ld = new LedgerData(lh, startOffset, stat.getVersion(), containerEpoch.get());
        ld.setLength((int) lh.getLength());
        ld.setReadonly(lh.isClosed());
        ld.setLastAddConfirmed(lh.getLastAddConfirmed());
        return ld;
    }

    private LogStorage getOrRetrieveStorageLedger(String streamSegmentName) throws StreamSegmentException {

        synchronized (this) {
            if (ledgers.containsKey(streamSegmentName)) {
                return ledgers.get(streamSegmentName);
                }
            }
        return retrieveStorageLedgerMetadata(streamSegmentName);
    }

    private LogStorage retrieveStorageLedgerMetadata(String streamSegmentName) throws StreamSegmentException {

        Stat stat = new Stat();
        byte[] bytes = null;
        try {
            bytes = zkClient.getData().storingStatIn(stat).forPath(getZkPath(streamSegmentName));
            LogStorage storageLog = LogStorage.deserialize(streamSegmentName, bytes, stat.getVersion());
            synchronized (this) {
                LogStorage olderValue = ledgers.putIfAbsent(streamSegmentName, storageLog);
            }
            return getBKLedgerDetails(streamSegmentName, storageLog);
        } catch (Exception exc) {
            throw translateZKException(streamSegmentName, exc);
        }
    }

    private LogStorage getBKLedgerDetails(String streamSegmentName, LogStorage logStorage) throws Exception {
        List<String> children = zkClient.getChildren().forPath(getZkPath(streamSegmentName));
        children
                .stream()
                .forEach(child -> {
                    int offset = Integer.valueOf(child);
                    Stat stat = new Stat();
                    byte[] bytes = new byte[0];
                    try {
                        bytes = zkClient.getData()
                                               .storingStatIn(stat)
                                               .forPath(getZkPath(streamSegmentName) + "/" + child);
                    LedgerData ledgerData = deserializeLedgerData(Integer.valueOf(child), bytes, stat);
                    ledgerData.setLastAddConfirmed(ledgerData.getLedgerHandle().getLastAddConfirmed());
                    ledgerData.setLength((int) ledgerData.getLedgerHandle().getLength());
                    logStorage.addToList(offset, ledgerData);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return logStorage;
    }

    private int readDataFromLedger(LedgerData ledgerData, long offset, byte[] buffer, int bufferOffset, int length) throws BKException, IOException {

        long currentOffset = offset - ledgerData.getStartOffset();
        int lengthRemaining = length;
        int currentBufferOffset = bufferOffset;
        boolean readingDone = false;
        long firstEntryId = ledgerData.getNearestEntryIDToOffset(currentOffset);
        int entriesInOneRound = config.getBkReadAheadCount();

        while (!readingDone) {

            long lastEntryId;
            if (ledgerData.getLastAddConfirmed() - firstEntryId < entriesInOneRound) {
                //Current ledger has less than one batch size. Read what ever is available.
                lastEntryId = ledgerData.getLastAddConfirmed();
            } else {
                //Read max configured entries.
                lastEntryId = firstEntryId + entriesInOneRound;
            }
            long finalFirstEntryId = firstEntryId;
            AtomicReference<Enumeration<LedgerEntry>> enumeration = new AtomicReference<>();
            Exceptions.handleInterrupted(() -> {
                enumeration.set(ledgerData.getLedgerHandle().readEntries(finalFirstEntryId, lastEntryId));
            });
            while (enumeration.get().hasMoreElements()) {
                LedgerEntry entry = enumeration.get().nextElement();

                InputStream stream = entry.getEntryInputStream();
                if (stream.available() <= currentOffset) {
                    currentOffset -= stream.available();
                } else {
                    long startInEntry = currentOffset;

                    long skipped = stream.skip(startInEntry);

                    int dataRemainingInEntry = (int) (entry.getLength() - startInEntry);

                    int dataRead = 0;
                    dataRead = stream.read(buffer, currentBufferOffset,
                            dataRemainingInEntry > lengthRemaining ?
                                    lengthRemaining : dataRemainingInEntry);
                    if (dataRead == -1) {
                        throw new EOFException();
                    }
                    lengthRemaining -= dataRead;
                    if (lengthRemaining == 0) {
                        ledgerData.setLastReadOffset(offset + length - ledgerData.getStartOffset(), entry.getEntryId());
                        readingDone = true;
                    }
                    currentBufferOffset += dataRead;
                    currentOffset = 0L;
                }
                if (entry.getEntryId() == ledgerData.getLastAddConfirmed()) {
                    //All the possible reads are done from this ledger. Break out.
                    readingDone = true;
                }
            }
            //We have looped through the complete batch. Move on to the next batch.
            firstEntryId = entriesInOneRound + 1;
        }
        return length - lengthRemaining;
    }

    private LogStorage fenceLedgersAndCreateOneAtEnd(String streamSegmentName, LogStorage ledger) throws StreamSegmentException {
        LogStorage logStorage = fenceAllTheLedgers(streamSegmentName, ledger);
        log.info("Made all the ledgers readonly. Adding a new ledger at {} for {}",
                logStorage.getLength(), streamSegmentName);
        LedgerData ledgerData = createLedgerAt(streamSegmentName, (int) logStorage.getLength());
        logStorage.addToList((int) logStorage.getLength(), ledgerData);
        return logStorage;
    }

    private LogStorage fenceAllTheLedgers(String streamSegmentName, LogStorage ledger) throws StreamSegmentException {
        List<String> children = null;
        try {
            children = zkClient.getChildren().forPath(getZkPath(streamSegmentName));
        } catch (Exception e) {
            throw translateZKException(streamSegmentName, e);
        }
        if (children.size() == 0) {
            LedgerData ledgerData = createLedgerAt(streamSegmentName, 0);
            ledger.addToList(0, ledgerData);
        } else {
            CompletableFuture<LedgerData>[] futures = null;
            children.stream().forEach(child -> {
                try {
                int offset = Integer.valueOf(child);
                LedgerData ledgerData = null;
                    ledgerData = fenceLedgerAt(streamSegmentName, offset);
                if (ledgerData.getLedgerHandle().getLength() == 0) {
                    tryDeleteLedger(ledgerData.getLedgerHandle().getId());
                    zkClient.delete().deletingChildrenIfNeeded().forPath(getZkPath(streamSegmentName) + "/" + child);
                }
                ledger.addToList(offset, ledgerData);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
            return ledger;
    }

    private void tryDeleteLedger(long ledgerId) {
        bookkeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
            if (rc != 0) {
                log.warn("Deletion of ledger {} failed with errorCode {}", ledgerId, rc);
            }
        }, null);
    }

    private LedgerData fenceLedgerAt(String streamSegmentName, int firstOffset) throws Exception {
        Stat stat = new Stat();
        byte[] data = zkClient.getData()
                              .storingStatIn(stat)
                              .forPath(getZkPath(streamSegmentName) + "/" + firstOffset);

        LedgerData ledgerData = deserializeLedgerData(firstOffset, data, stat);
        ledgerData.setLength((int) ledgerData.getLedgerHandle().getLength());
        ledgerData.setLastAddConfirmed(ledgerData.getLedgerHandle().getLastAddConfirmed());
        log.info("Fencing ledger {}", streamSegmentName);
        ledgerData.getLedgerHandle().close();
        return ledgerData;
    }

    private void writeDataAt(LedgerHandle lh, long offset, InputStream data, int length, String segmentName) {
        log.info("Writing {} at offset {} for ledger {}", length, offset, lh.getId());
        byte[] bytes = new byte[length];
        try {
            StreamHelpers.readAll(data, bytes, 0, length);
        } catch (IOException e) {
            throw new CompletionException(e);
        }

        Exceptions.handleInterrupted(() -> {
            try {
                lh.addEntry(bytes, 0, length);
            } catch (BKException e) {
                throw new CompletionException(translateBKException(e, segmentName));
            }
        });
    }

    private List<CuratorOp> getZKOperationsForConcat(String segmentName, String sourceSegment, long offset) throws Exception {
        LogStorage target = this.getOrRetrieveStorageLedger(segmentName);
        LogStorage source = this.getOrRetrieveStorageLedger(sourceSegment);
        Preconditions.checkState(source.isSealed(), "source must be sealed");
        if (source.getContainerEpoch() != this.containerEpoch.get()) {
            throw new CompletionException(new StorageNotPrimaryException(target.getName()));
        }
        List<CuratorOp> operations = new ArrayList<>();
        LedgerData lastLedger = target.getLastLedgerData();
        if (lastLedger != null && lastLedger.getLedgerHandle().getLength() == 0) {
            operations.add(createLedgerDeleteOp(lastLedger, target));
        }
        List<CuratorOp> targetOps = addLedgerDataFrom(source, target);
        operations.addAll(targetOps);
        // Update the segment also to ensure fencing has not happened
        operations.add(createLedgerUpdateOp(target));
        return operations;
    }

    /**
     * Creates a list of curator transaction for merging source LogStorage in to this.
     * @param source Name of the source ledger.
     * @return list of curator operations.
     */
    List<CuratorOp> addLedgerDataFrom(LogStorage source, LogStorage target) {
        List<CuratorOp> retVal = null;
        synchronized (source) {
            retVal = source.getDataMap().entrySet().stream().map(entry -> {
                int newKey = (int) (entry.getKey() + target.getLength());
                LedgerData value = entry.getValue();
                value.setStartOffset(newKey);
                target.getDataMap().put(newKey, value);
                try {
                    return createAddOp(target.getName(), newKey, entry.getValue());
                } catch (StreamSegmentException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        }

        // Increase the target segment length
        target.increaseLengthBy((int) source.getLength());
        return retVal;
    }

    private CuratorOp createLedgerDeleteOp(LedgerData lastLedger, LogStorage target) throws Exception {
        return zkClient.transactionOp().delete()
                       .forPath(getZkPath(target.getName()) + "/" + lastLedger.getStartOffset());
    }

    private CuratorOp createLedgerUpdateOp(LogStorage target) throws Exception {
        return zkClient.transactionOp().setData().withVersion(target.getUpdateVersion())
                       .forPath(getZkPath(target.getName()), target.serialize());
    }

    //endregion

    //region ZK private helper methods

    private StreamSegmentException translateZKException(String streamSegmentName, Throwable exc) {
        if (exc instanceof KeeperException.NodeExistsException) {
            return new StreamSegmentExistsException(streamSegmentName);
        } else if (exc instanceof KeeperException.NoNodeException) {
            return new StreamSegmentNotExistsException(streamSegmentName);
        } else if (exc instanceof KeeperException.BadVersionException) {
            return new StorageNotPrimaryException(streamSegmentName);
        } else {
            throw new CompletionException(exc);
        }
    }

    private String getZkPath(String streamSegmentName) {
        if (streamSegmentName.startsWith("/")) {
            return streamSegmentName;
        } else {
            return "/" + streamSegmentName;
        }
    }

    //endregion
}