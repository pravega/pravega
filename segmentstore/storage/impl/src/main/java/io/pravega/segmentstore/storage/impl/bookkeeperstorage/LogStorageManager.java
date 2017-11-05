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
import com.google.common.collect.Sets;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.DeleteOption;
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
    private final ExecutorService executor;
    private final AsyncCuratorFramework zkClient;

    @GuardedBy("this")
    private final ConcurrentHashMap<String, LogStorage> ledgers;
    private BookKeeper bookkeeper;
    private long containerEpoch;

    LogStorageManager(BookKeeperStorageConfig config, CuratorFramework zkClient, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(zkClient, "zkClient");

        this.config = config;
        this.executor = executor;
        this.zkClient = AsyncCuratorFramework.wrap(zkClient);
        ledgers = new ConcurrentHashMap<>();
    }

    //region Storage API

    /**
     * Creates a LogStorage at location streamSegmentName.
     *
     * @param streamSegmentName Full name of the stream segment
     * @param timeout           timeout value for the operation
     * @return A new instance of LogStorage if one does not already exist
     * @Param bookkeeper the BK instance
     * @Param zkClient
     */
    public CompletableFuture<LedgerData> create(String streamSegmentName, Duration timeout) {
        LogStorage logStorage = new LogStorage(this, streamSegmentName, 0, this.containerEpoch, 0, false);

        /* Create the node for the segment in the ZK. */
        return zkClient.create()
                       .forPath(getZkPath(streamSegmentName), logStorage.serialize())
                       .toCompletableFuture()
                       .exceptionally(exc -> {
                           translateZKException(streamSegmentName, exc);
                           return null;
                       })
                       /** Add the ledger to the local cache.*/
                       .thenApply(name -> {
                           synchronized (this) {
                               ledgers.put(streamSegmentName, logStorage);
                           }
                           return streamSegmentName;
                       })
                       // Create the first ledger and add it to the local cache
                       .thenCompose(t -> {
                           CompletableFuture<LedgerData> retval = createLedgerAt(streamSegmentName, 0);
                           return retval.thenApply(data -> {
                               synchronized (this) {
                                   this.ledgers.get(streamSegmentName).addToList(0, data);
                               }
                               return data;
                           });
                       });
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
    public CompletableFuture<?> fence(String streamSegmentName) {
        AtomicReference<Boolean> tryAgain = new AtomicReference<>(true);
        AtomicReference<Boolean> needFencing = new AtomicReference<>(false);

        /** Get the LogStorage metadata. */
        return getOrRetrieveStorageLedger(streamSegmentName, false)
                /** check whether fencing is required. */
                .thenCompose(ledger -> {
                    if (ledger.getContainerEpoch() == containerEpoch) {
                        return CompletableFuture.completedFuture(ledger);
                    } else if (ledger.getContainerEpoch() > containerEpoch) {
                        throw new CompletionException(new StorageNotPrimaryException(streamSegmentName));
                    } else {
                        /** If fencing is required, update the metadata. */
                        needFencing.set(true);
                        ledger.setContainerEpoch(containerEpoch);
                        return zkClient.setData()
                                       .withVersion(ledger.getUpdateVersion())
                                       .forPath(getZkPath(streamSegmentName), ledger.serialize())
                                       .exceptionally(exc -> {
                                           if (Exceptions.unwrap(exc) instanceof KeeperException.BadVersionException) {
                                               //Need to retry as data we had was out of sync
                                               tryAgain.set(true);
                                           } else {
                                               throw new CompletionException(exc);
                                           }
                                           return null;
                                       }).thenApply(stat -> {
                                        ledger.setUpdateVersion(stat.getVersion());
                                        return ledger;
                                });
                    }
                })
                /** Fence out all the ledgers and create a new one at the end for appends. */
                .thenCompose(ledger -> {
                    if (needFencing.get()) {
                        log.info("Fencing all the ledgers for {}", streamSegmentName);
                        return fenceLedgersAndCreateOneAtEnd(streamSegmentName, ledger);
                    } else {
                        return CompletableFuture.completedFuture(ledger);
                    }
                });
    }

    /**
     * Initializes the bookkeeper and curator objects.
     *
     * @param containerEpoch the epoc to be used for the fencing and create calls.
     */
    public void initialize(long containerEpoch) {
        this.containerEpoch = containerEpoch;
        int entryTimeout = (int) Math.ceil(this.config.getBkWriteTimeoutMillis() / 1000.0);
        ClientConfiguration config = new ClientConfiguration()
                .setZkServers(this.config.getZkAddress())
                .setClientTcpNoDelay(true)
                .setAddEntryTimeout(entryTimeout)
                .setClientConnectTimeoutMillis((int) this.config.getZkConnectionTimeout().toMillis())
                .setZkTimeout((int) this.config.getZkConnectionTimeout().toMillis());
        if (this.config.getBkLedgerPath().isEmpty()) {
            config.setZkLedgersRootPath("/" + zkClient.unwrap().getNamespace() + "/bookkeeper/ledgers");
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
     * @param timeout The duration limit.
     * @return A CompletableFuture which holds the boolean value.
     */
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return zkClient.checkExists().forPath(getZkPath(streamSegmentName)).toCompletableFuture().thenApply(stat -> stat != null);
    }

    /**
     * API to read data.
     * @param segmentName name of the segment.
     * @param offset  offset into the segment.
     * @param buffer  Buffer to read the data in.
     * @param bufferOffset starting offset inside the buffer.
     * @param length Size of the data to be read.
     * @param timeout duration.
     * @return  A CompletableFuture which contains the actual read size once the read is complete.
     */
    public CompletableFuture<Integer> read(String segmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        final AtomicReference<Integer> currentLength = new AtomicReference<>(length);
        final AtomicReference<Long> currentOffset = new AtomicReference<>(offset);
        final AtomicReference<Integer> currentBufferOffset = new AtomicReference<>(bufferOffset);

        return getOrRetrieveStorageLedger(segmentName, true)
                .thenCompose(ledger -> {
                    Preconditions.checkArgument(offset + length <= ledger.getLength(), segmentName);
                    /** Loop till the data is read completely. */
                    return Futures.loop(
                            () -> currentLength.get() != 0,
                            /* Get the BK ledger which contains the offset. */
                            () ->
                                /** Read data from the BK ledger. */
                                readDataFromLedger(ledger.getLedgerDataForReadAt(currentOffset.get()),
                                        currentOffset.get(),
                                        buffer, currentBufferOffset.get(), currentLength.get())
                                /** Update the remaining lengths and offsets. */
                                .thenAccept(dataRead -> {
                                      Preconditions.checkState(dataRead != 0, "No data read");
                                      currentLength.accumulateAndGet(-dataRead, (integer, integer2) -> integer + integer2);
                                      currentOffset.accumulateAndGet((long) dataRead, (integer, integer2) -> integer + integer2);
                                      currentBufferOffset.accumulateAndGet(dataRead, (integer, integer2) -> integer + integer2);
                                }),
                    executor);
                })
                .thenApply(v -> length);
    }

    /**
     * API to write data.
     * @param segmentName name of the segment.
     * @param offset Offset in the segment at which the write starts.
     * @param data  Data to be written.
     * @param length size of the data to be written.
     * @return A CompletableFuture which completes once the write is complete.
     */
    public CompletableFuture<Integer> write(String segmentName, long offset, InputStream data, int length) {
        log.info("Writing {} at offset {} for segment {}", length, offset, segmentName);

        return getOrRetrieveStorageLedger(segmentName, false)
                .thenCompose((LogStorage ledger) -> {
                    if (ledger.getLength() != offset) {
                        throw new CompletionException(new BadOffsetException(segmentName, ledger.getLength(), offset));
                    }
                    if (ledger.isSealed()) {
                        throw new CompletionException(new StreamSegmentSealedException(segmentName));
                    }
                    /* Get the last ledger where data can be appended. */
                    return getORCreateLHForOffset(ledger, offset);
                }).thenCompose(ledgerData -> writeDataAt(ledgerData.getLedgerHandle(), offset, data, length, segmentName)
                        .thenApply(v -> {
                            /* Update lengths in the cache. The lengths are not persisted. */
                            ledgerData.increaseLengthBy(length);
                            ledgerData.setLastAddConfirmed(ledgerData.getLastAddConfirmed() + 1);
                            synchronized (this) {
                                ledgers.get(segmentName).increaseLengthBy(length);
                            }
                            return length;
                        }));
    }

    /**
     * API to seal a given segment.
     * @param segmentName name of the segment.
     * @return A CompletableFuture which completes once the seal operation is complete.
     */
    public CompletableFuture<Void> seal(String segmentName) {
        return this.getOrRetrieveStorageLedger(segmentName, false)
                   .thenCompose(ledger -> {
                       /** Check whether this segmentstore is the current owner. */
                       if (ledger.getContainerEpoch() > this.containerEpoch) {
                           throw new CompletionException(new StorageNotPrimaryException(segmentName));
                       }
                       ledger.markSealed();
                       /** Update the details in ZK.*/
                       return zkClient.setData()
                                      .withVersion(ledger.getUpdateVersion())
                                      .forPath(getZkPath(segmentName), ledger.serialize())
                                      .toCompletableFuture()
                                      .exceptionally(exc -> {
                                          translateZKException(segmentName, exc);
                                          return null;
                                      })
                                      .thenApply(stat -> {
                                          ledger.setUpdateVersion(ledger.getUpdateVersion() + 1);
                                          return ledger;
                                      });
                   })
                   /** Seal the last ledger. This is the only ledger which can be written to. */
                   .thenCompose(ledger -> this.sealLedger(ledger.getLastLedgerData()));
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
     * @param timeout duration for the operation.
     * @return A completable future which completes once the concat operation is complete.
     */
    public CompletableFuture<Void> concat(String segmentName, String sourceSegment, long offset, Duration timeout) {
        return this.getZKOperationsForConcat(segmentName, sourceSegment, offset)
                   .thenCompose(curatorOps -> zkClient.transaction()
                                                      .forOperations(curatorOps).toCompletableFuture()
                                                      .exceptionally(exc -> {
                                                          /** In case of exception, drop the corrupt cache. */
                                                          ledgers.remove(segmentName);
                                                          translateZKException(segmentName, exc);
                                                          return null;
                                                      })
                                                      .thenCompose(results -> getOrRetrieveStorageLedger(segmentName, false)
                                                              .thenCompose(logStorage -> {
                                                                  logStorage.setUpdateVersion(logStorage.getUpdateVersion() + 1);
                                                                  /* Fence all the ledgers and add one at the end. */
                                                                  return fenceLedgersAndCreateOneAtEnd(segmentName, logStorage);
                                                              })
                                                              .thenApply(logStorage -> null)))
                   /** Delete metadata and cache for the source once the operation is complete. */
                   .thenCompose(v -> this.zkClient.delete()
                                                  .withOptions(Sets.newHashSet(
                                                          Arrays.asList(DeleteOption.guaranteed,
                                                                  DeleteOption.deletingChildrenIfNeeded)))
                                                  .forPath(getZkPath(sourceSegment))
                                                  .toCompletableFuture()
                                                  .thenAccept(ret -> {
                                                      ledgers.remove(sourceSegment);
                                                  }));
    }

    /**
     * Deletes a segment along with all the data and metadata.
     * @param segmentName name of the segment to be deleted.
     * @return A CompletableFuture which completes once the delete is complete.
     */
    public CompletableFuture<Void> delete(String segmentName) {
        return this.getOrRetrieveStorageLedger(segmentName, false)
                   .thenCompose(ledger -> {
                       if (ledger.getContainerEpoch() > this.containerEpoch) {
                           throw new CompletionException(new StorageNotPrimaryException(segmentName));
                       }
                       return this.zkClient.delete()
                                           .withOptions(Sets.newHashSet(
                                                   Arrays.asList(DeleteOption.guaranteed,
                                                           DeleteOption.deletingChildrenIfNeeded)))
                                           .forPath(getZkPath(segmentName))
                                           .toCompletableFuture()
                                           .thenCompose(v -> {
                                               ledgers.remove(segmentName);
                                               return ledger.deleteAllLedgers();
                                           });
                   });
    }

    //endregion


    public CompletableFuture<LedgerData> createLedgerAt(String streamSegmentName, int offset) {
        CompletableFuture<LedgerData> retVal = new CompletableFuture<>();

        bookkeeper.asyncCreateLedger(config.getBkEnsembleSize(),
                config.getBkWriteQuorumSize(), LEDGER_DIGEST_TYPE, config.getBKPassword(),
                (errorCode, ledgerHandle, future) -> {
                    if (errorCode == 0) {
                        LedgerData lh = new LedgerData(ledgerHandle, offset, 0, this.containerEpoch);
                        zkClient.create().forPath(getZkPath(streamSegmentName) + "/" + offset, lh.serialize())
                                .toCompletableFuture()
                                .exceptionally(exc -> {
                                    retVal.completeExceptionally(exc);
                                    return null;
                                })
                                .thenApply(name -> {
                                    return retVal.complete(lh);
                                });
                    } else {
                        retVal.completeExceptionally(BKException.create(errorCode));
                    }
                }, retVal);
        return retVal;
    }

    public CompletableFuture<Void> deleteLedger(LedgerHandle lh) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        bookkeeper.asyncDeleteLedger(lh.getId(), (errorCode, o) -> {
            if (errorCode == 0) {
                future.complete(null);
            } else {
                log.warn("Delete ledger failed. Continuing as we gave it our best shot");
                future.complete(null);
            }
        }, future);
        return future;
    }

    public CuratorOp createAddOp(String segmentName, int newKey, LedgerData value) {
        return zkClient.transactionOp().create().forPath(getZkPath(segmentName) + "/" + newKey, value.serialize());
    }

    /**
     * Returns details about a specific storageledger.
     * If the ledger does not exist in the cache, we try to
     *
     * @param streamSegmentName name of the stream segment
     * @param readOnly          whether a readonly copy of BK is expected or a copy for write.
     * @return
     */
    public CompletableFuture<SegmentProperties> getOrRetrieveStorageLedgerDetails(String streamSegmentName, boolean readOnly) {

        return getOrRetrieveStorageLedger(streamSegmentName, readOnly)
                .thenApply(ledger -> StreamSegmentInformation.builder()
                                                             .name(streamSegmentName)
                                                             .length(ledger.getLength())
                                                             .sealed(ledger.isSealed())
                                                             .lastModified(ledger.getLastModified())
                                                             .build());
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

    private CompletableFuture<LedgerData> getORCreateLHForOffset(LogStorage ledger, long offset) {
        return ledger.getLedgerDataForWriteAt(offset);
    }

    private CompletableFuture<Void> sealLedger(LedgerData lastLedgerData) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                lastLedgerData.getLedgerHandle().close();
                future.complete(null);
            } catch (Exception e) {
                log.warn("Exception {} while closing the last ledger", e);
            }
        });
        return future;
    }

    private LedgerData deserializeLedgerData(Integer startOffset, byte[] bytes, boolean readOnly, Stat stat) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        LedgerHandle lh = null;
        long ledgerId = bb.getLong();
        log.info("Opening a ledger with ledger id {}", ledgerId);

        try {
            if (readOnly) {
                lh = bookkeeper.openLedgerNoRecovery(ledgerId, LEDGER_DIGEST_TYPE, config.getBKPassword());
            } else {
                lh = bookkeeper.openLedger(ledgerId, LEDGER_DIGEST_TYPE, config.getBKPassword());
            }
        } catch (Exception e) {
            log.warn("Exception {} while opening ledger id {}", e, ledgerId);
            throw new CompletionException(e);
        }
        LedgerData ld = new LedgerData(lh, startOffset, stat.getVersion(), containerEpoch);
        ld.setLength((int) lh.getLength());
        ld.setReadonly(lh.isClosed());
        ld.setLastAddConfirmed(lh.getLastAddConfirmed());
        return ld;
    }

    private CompletableFuture<LogStorage> getOrRetrieveStorageLedger(String streamSegmentName, boolean readOnly) {
        CompletableFuture<LogStorage> retVal = new CompletableFuture<>();

        synchronized (this) {
            if (ledgers.containsKey(streamSegmentName)) {
                LogStorage ledger = ledgers.get(streamSegmentName);
                if (!readOnly && ledger.isReadOnlyHandle()) {
                    //Check whether the value is read-write, if it is not, flush it so that we create a read-write token
                    ledgers.remove(streamSegmentName);
                } else {
                    // If the caller expects a readonly handle, either a readonly or read-write cached value will work.
                    retVal.complete(ledger);
                    return retVal;
                }
            }
        }
        return retrieveStorageLedgerMetadata(streamSegmentName, readOnly);
    }

    private CompletableFuture<LogStorage> retrieveStorageLedgerMetadata(String streamSegmentName, boolean readOnly) {

        Stat stat = new Stat();
        return zkClient.getData().storingStatIn(stat).forPath(getZkPath(streamSegmentName)).toCompletableFuture()
                       .exceptionally(exc -> {
                           translateZKException(streamSegmentName, exc);
                           return null;
                       })
                       .thenApply(bytes -> {
                           LogStorage retval = LogStorage.deserialize(this, streamSegmentName, bytes, stat.getVersion(), readOnly);
                           synchronized (this) {
                                  ledgers.putIfAbsent(streamSegmentName, retval);
                               }
                               return retval;
                       })
                       .thenCompose(storageLog -> {
                           if (readOnly) {
                               return getBKLedgerDetails(streamSegmentName, storageLog);
                           } else {
                               return CompletableFuture.completedFuture(storageLog);
                           }
                       });
    }

    private CompletableFuture<LogStorage> getBKLedgerDetails(String streamSegmentName, LogStorage logStorage) {
        return zkClient.getChildren().forPath(getZkPath(streamSegmentName))
                       .toCompletableFuture()
                       .thenCompose(children -> {
                           CompletableFuture<LedgerData>[] futures = null;
                           futures = children
                                   .stream()
                                   .map(child -> {
                                       int offset = Integer.valueOf(child);
                                       Stat stat = new Stat();
                                       return zkClient.getData()
                                                      .storingStatIn(stat)
                                                      .forPath(getZkPath(streamSegmentName) + "/" + child)
                                                      .thenApply(bytes -> {
                                                          LedgerData ledgerData = deserializeLedgerData(Integer.valueOf(child),
                                                                  bytes, true, stat);
                                                          ledgerData.setLastAddConfirmed(ledgerData.getLedgerHandle().getLastAddConfirmed());
                                                          ledgerData.setLength((int) ledgerData.getLedgerHandle().getLength());
                                                          logStorage.addToList(offset, ledgerData);
                                                          return ledgerData;
                                                      });
                                   })
                                   .toArray(CompletableFuture[]::new);
                           return CompletableFuture.allOf(futures).thenApply(v -> logStorage);
                       });
    }

    private CompletableFuture<Integer> readDataFromLedger(LedgerData ledgerData, long offset, byte[] buffer, int bufferOffset, int length) {

        final AtomicReference<Long> currentOffset = new AtomicReference<>(offset - ledgerData.getStartOffset());
        final AtomicReference<Integer> lengthRemaining = new AtomicReference<>(length);
        final AtomicReference<Integer> currentBufferOffset = new AtomicReference<>(bufferOffset);
        final AtomicReference<Boolean> readingDone = new AtomicReference<>(false);
        final AtomicReference<Long> firstEntryId = new AtomicReference<>((long) ledgerData.getNearestEntryIDToOffset(currentOffset.get()));
        int entriesInOneRound = config.getBkReadEntriesInOneGo();

        return Futures.loop(() -> !readingDone.get(),
                () -> {
                    CompletableFuture<Void> future = new CompletableFuture<>();

                    long lastEntryId;
                    if (ledgerData.getLastAddConfirmed() - firstEntryId.get() < entriesInOneRound) {
                        lastEntryId = ledgerData.getLastAddConfirmed();
                    } else {
                        lastEntryId = firstEntryId.get() + entriesInOneRound;
                    }
                    ledgerData.getLedgerHandle().asyncReadEntries(firstEntryId.get(), lastEntryId,
                            (errorCode, ledgerHandle, enumeration, o) -> {
                                if (errorCode != 0) {
                                    throw new CompletionException(BKException.create(errorCode));
                                }

                                while (enumeration.hasMoreElements()) {
                                    LedgerEntry entry = enumeration.nextElement();

                                    try {
                                        InputStream stream = entry.getEntryInputStream();
                                        if (stream.available() <= currentOffset.get()) {
                                            currentOffset.set(currentOffset.get() - stream.available());
                                        } else {
                                            long startInEntry = currentOffset.get();

                                            long skipped = stream.skip(startInEntry);

                                            int dataRemainingInEntry = (int) (entry.getLength() - startInEntry);

                                            int dataRead = 0;
                                            dataRead = stream.read(buffer, currentBufferOffset.get(),
                                                    dataRemainingInEntry > lengthRemaining.get() ?
                                                            lengthRemaining.get() : dataRemainingInEntry);
                                            if (dataRead == -1) {
                                                log.warn("Data read returned -1" + skipped);
                                                future.completeExceptionally(new CompletionException(new IOException()));
                                                return;
                                            }
                                            lengthRemaining.set(lengthRemaining.get() - dataRead);
                                            if (lengthRemaining.get() == 0) {
                                                ledgerData.saveLastReadOffset(offset + length - ledgerData.getStartOffset(), entry.getEntryId());
                                                future.complete(null);
                                                readingDone.set(true);
                                                return;
                                            }
                                            currentBufferOffset.set(currentBufferOffset.get() + dataRead);
                                            currentOffset.set((long) 0);
                                        }
                                    } catch (IOException e) {
                                        future.completeExceptionally(new CompletionException(e));
                                        return;
                                    }
                                    if (entry.getEntryId() == ledgerHandle.getLastAddConfirmed()) {
                                        future.complete(null);
                                        readingDone.set(true);
                                    }
                                }
                                firstEntryId.set(firstEntryId.get() + entriesInOneRound + 1);
                                future.complete(null);
                            }, future);
                    return future;
                },
                executor)
                            .thenApply(v -> length - lengthRemaining.get());
    }

    private CompletableFuture<LogStorage> fenceLedgersAndCreateOneAtEnd(String streamSegmentName, LogStorage ledger) {
        return fenceAllTheLedgers(streamSegmentName, ledger)
                .thenCompose(logStorage -> {
                    log.info("Made all the ledgers readonly. Adding a new ledger at {} for {}",
                            logStorage.getLength(), streamSegmentName);
                    return createLedgerAt(streamSegmentName, (int) logStorage.getLength())
                            .thenApply(ledgerData -> {
                                logStorage.addToList((int) logStorage.getLength(), ledgerData);
                                return logStorage;
                            });
                });
    }

    private CompletableFuture<LogStorage> fenceAllTheLedgers(String streamSegmentName, LogStorage ledger) {
        return zkClient.getChildren().forPath(getZkPath(streamSegmentName))
                       .toCompletableFuture()
                       .exceptionally(exc -> {
                           translateZKException(streamSegmentName, exc);
                           return null;
                       })
                       .thenCompose(
                               children -> {
                                   if (children.size() == 0) {
                                       return createLedgerAt(streamSegmentName, 0).thenApply(ledgerData -> {
                                           ledger.addToList(0, ledgerData);
                                           return ledger;
                                       });
                                   } else {
                                       CompletableFuture<LedgerData>[] futures = null;
                                       futures = children
                                               .stream()
                                               .map(child -> {
                                                   int offset = Integer.valueOf(child);
                                                   return fenceLedgerAt(streamSegmentName, offset)
                                                           .thenCompose(ledgerData -> {
                                                               if (ledgerData.getLedgerHandle().getLength() == 0) {
                                                                   tryDeleteLedger(ledgerData.getLedgerHandle().getId());
                                                                   return zkClient.delete().forPath(getZkPath(streamSegmentName) + "/" + child).toCompletableFuture()
                                                                                  .thenApply(v -> null);
                                                               }
                                                               ledger.addToList(offset, ledgerData);
                                                               return CompletableFuture.completedFuture(ledgerData);
                                                           });
                                               })
                                               .toArray(CompletableFuture[]::new);
                                       return CompletableFuture.allOf(futures).thenApply(v -> ledger);
                                   }
                               });
    }

    private void tryDeleteLedger(long ledgerId) {
        bookkeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
            if (rc != 0) {
                log.warn("Deletion of ledger {} failed with errorCode {}", ledgerId, rc);
            }
        }, null);
    }

    private CompletableFuture<LedgerData> fenceLedgerAt(String streamSegmentName, int firstOffset) {
        Stat stat = new Stat();
        return zkClient.getData()
                       .storingStatIn(stat)
                       .forPath(getZkPath(streamSegmentName) + "/" + firstOffset)
                       .toCompletableFuture()
                       .thenApply(data -> {
                           LedgerData ledgerData = deserializeLedgerData(firstOffset, data, false, stat);
                           ledgerData.setLength((int) ledgerData.getLedgerHandle().getLength());
                           ledgerData.setLastAddConfirmed(ledgerData.getLedgerHandle().getLastAddConfirmed());
                           try {
                               ledgerData.getLedgerHandle().close();
                           } catch (Exception e) {
                               throw new CompletionException(e);
                           }
                           return ledgerData;
                       });
    }


    private CompletableFuture<Void> writeDataAt(LedgerHandle lh, long offset, InputStream data, int length, String segmentName) {
        log.info("Writing {} at offset {} for ledger {}", length, offset, lh.getId());
        byte[] bytes = new byte[length];
        CompletableFuture<Void> retVal = new CompletableFuture<>();

        try {
            StreamHelpers.readAll(data, bytes, 0, length);
        } catch (IOException e) {
            retVal.completeExceptionally(e);
            return retVal;
        }

        lh.asyncAddEntry(bytes, (errorCode, ledgerHandle, l, future) -> {
            if (errorCode == 0) {
                retVal.complete(null);
            } else {
                log.warn("asyncAddEntry failed for ledger with ledger id {}, LAC {}, LAP {} ", lh.getId(), lh.getLastAddConfirmed(), lh.getLastAddPushed());
                retVal.completeExceptionally(translateBKException(BKException.create(errorCode), segmentName));
            }
        }, retVal);

        return retVal;
    }

    private CompletableFuture<List<CuratorOp>> getZKOperationsForConcat(String segmentName, String sourceSegment, long offset) {
        return this.getOrRetrieveStorageLedger(segmentName, false)
                   .exceptionally(exc -> {
                               translateZKException(segmentName, exc);
                               return null;
                           }
                   )
                   .thenCombine(this.getOrRetrieveStorageLedger(sourceSegment, false),
                           (target, source) -> {
                               Preconditions.checkState(source.isSealed(), "source must be sealed");
                               if (source.getContainerEpoch() != this.containerEpoch) {
                                   throw new CompletionException(new StorageNotPrimaryException(target.getName()));
                               }
                               List<CuratorOp> operations = new ArrayList<>();
                               LedgerData lastLedger = target.getLastLedgerData();
                               if (lastLedger != null && lastLedger.getLedgerHandle().getLength() == 0) {
                                   operations.add(createLedgerDeleteOp(lastLedger, target));
                               }
                               List<CuratorOp> targetOps = target.addLedgerDataFrom(source);
                               operations.addAll(targetOps);
                               // Update the segment also to ensure fencing has not happened
                               operations.add(createLedgerUpdateOp(target));
                               return operations;
                           });
    }

    private CuratorOp createLedgerDeleteOp(LedgerData lastLedger, LogStorage target) {
        return zkClient.transactionOp().delete()
                       .forPath(getZkPath(target.getName()) + "/" + lastLedger.getStartOffset());
    }

    private CuratorOp createLedgerUpdateOp(LogStorage target) {
        return zkClient.transactionOp().setData().withVersion(target.getUpdateVersion())
                       .forPath(getZkPath(target.getName()), target.serialize());
    }

    //endregion

    //region ZK private helper methods

    private void translateZKException(String streamSegmentName, Throwable exc) {
        if (exc instanceof KeeperException.NodeExistsException) {
            throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
        } else if (exc instanceof KeeperException.NoNodeException) {
            throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
        } else if (exc instanceof KeeperException.BadVersionException) {
            throw new CompletionException(new StorageNotPrimaryException(streamSegmentName));
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
