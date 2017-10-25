/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeepertier2;

import io.pravega.common.concurrent.FutureHelpers;
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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
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
 * It is responsible for creating and deleteing storage ledger objects.
 */
@Slf4j
public class StorageLedgerManager {
    private static final BookKeeper.DigestType LEDGER_DIGEST_TYPE = BookKeeper.DigestType.MAC;

    private final BookkeeperStorageConfig config;
    private final ExecutorService executor;
    private final AsyncCuratorFramework zkClient;

    private ConcurrentHashMap<String, StorageLedger> ledgers;
    private BookKeeper bookkeeper;
    private long containerEpoc;

    public StorageLedgerManager(BookkeeperStorageConfig config, CuratorFramework zkClient, ExecutorService executor) {
        this.config = config;
        this.executor = executor;
        this.zkClient = AsyncCuratorFramework.wrap(zkClient);
        ledgers = new ConcurrentHashMap<>();
    }

    /**
     * Creates a StorageLedger at location streamSegmentName.
     *
     * @param streamSegmentName Full name of the stream segment
     * @param timeout           timeout value for the operation
     * @return A new instance of StorageLedger if one does not already exist
     * @Param bookkeeper the BK instance
     * @Param zkClient
     */
    public CompletableFuture<LedgerData> create(String streamSegmentName, Duration timeout) {
        StorageLedger storageLedger = new StorageLedger(this, streamSegmentName, 0, this.containerEpoc, false);
        return zkClient.create()
                       .forPath(getZkPath(streamSegmentName), storageLedger.serialize())
                       .toCompletableFuture()
                       .exceptionally(exc -> {
                           translateZKException(streamSegmentName, exc);
                           return null;
                       })
                       .thenApply(name -> {
                           ledgers.put(streamSegmentName, storageLedger);
                           return streamSegmentName;
                       })
                       .thenCompose(t -> {
                           CompletableFuture<LedgerData> retval = createLedgerAt(streamSegmentName, 0);
                           return retval.thenApply(data -> {
                               this.ledgers.get(streamSegmentName).addToList(0, data);
                               return data;
                           });
                       });
    }

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

        return getOrRetrieveStorageLedger(streamSegmentName, false)
                .thenCompose(ledger -> {
                    CompletableFuture<StorageLedger> retVal = new CompletableFuture<StorageLedger>();
                    if (ledger.getContainerEpoc() == containerEpoc) {
                        retVal.complete(ledger);
                        return retVal;
                    } else if (ledger.getContainerEpoc() > containerEpoc) {
                        throw new CompletionException(new StorageNotPrimaryException(streamSegmentName));
                    } else {
                        needFencing.set(true);
                        ledger.setContainerEpoc(containerEpoc);
                        return zkClient.setData()
                                       .withVersion(ledger.getUpdateVersion())
                                       .forPath(getZkPath(streamSegmentName), ledger.serialize())
                                       .exceptionally(exc -> {
                                           if (exc instanceof KeeperException.BadVersionException) {
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
                }).thenCompose(ledger -> {
                    if (needFencing.get()) {
                        log.info("Fencing all the ledgers for {}", streamSegmentName);
                        return fenceLedgersAndCreateOneAtEnd(streamSegmentName, ledger);
                    } else {
                        CompletableFuture<StorageLedger> future = new CompletableFuture<>();
                        future.complete(ledger);
                        return future;
                    }
                });
    }

    private CompletableFuture<StorageLedger> fenceLedgersAndCreateOneAtEnd(String streamSegmentName, StorageLedger ledger) {
        return fenceAllTheLedgers(streamSegmentName, ledger)
                .thenCompose(storageLedger -> {
                    log.info("Made all the ledgers readonly. Adding a new ledger at {} for {}",
                            storageLedger.getLength(), streamSegmentName);
                    return createLedgerAt(streamSegmentName, storageLedger.getLength())
                            .thenApply(ledgerData -> {
                                storageLedger.addToList(storageLedger.getLength(), ledgerData);
                                return storageLedger;
                            });
                });
    }

    private CompletableFuture<StorageLedger> fenceAllTheLedgers(String streamSegmentName, StorageLedger ledger) {
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
                                       CompletableFuture<LedgerData>[] futures = children.stream()
                                                                                         .map(child -> {
                                                                                             int offset = Integer.valueOf(child);
                                                                                             return fenceLedgerAt(streamSegmentName, offset)
                                                                                                     .thenCompose(ledgerData -> {
                                                                                                         if (ledgerData.getLh().getLength() == 0) {
                                                                                                             tryDeleteLedger(ledgerData.getLh().getId());
                                                                                                             return zkClient.delete().forPath(getZkPath(streamSegmentName) + "/" + child).toCompletableFuture()
                                                                                                                            .thenApply(v -> null);
                                                                                                         }
                                                                                                         ledger.addToList(offset, ledgerData);
                                                                                                         CompletableFuture<LedgerData> retval = new CompletableFuture<LedgerData>();
                                                                                                         retval.complete(ledgerData);
                                                                                                         return retval;
                                                                                                     });
                                                                                         })
                                                                                         .toArray(CompletableFuture[]::new);
                                       return CompletableFuture.allOf(futures).thenApply(v -> {
                                           return ledger;
                                       });
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
                           ledgerData.setLength((int) ledgerData.getLh().getLength());
                           ledgerData.setLastAddConfirmed(ledgerData.getLh().getLastAddConfirmed());
                           try {
                               ledgerData.getLh().close();
                           } catch (Exception e) {
                               throw new CompletionException(e);
                           }
                           return ledgerData;
                       });
    }

    public CompletableFuture<LedgerData> createLedgerAt(String streamSegmentName, int offset) {
        CompletableFuture<LedgerData> retVal = new CompletableFuture<>();

        bookkeeper.asyncCreateLedger(config.getBkEnsembleSize(),
                config.getBkWriteQuorumSize(), LEDGER_DIGEST_TYPE, config.getBKPassword(),
                (errorCode, ledgerHandle, future) -> {
                    if (errorCode == 0) {
                        LedgerData lh = new LedgerData(ledgerHandle, offset, 0, this.containerEpoc);
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

    /**
     * Initializes the bookkeeper and curator objects.
     *
     * @param containerEpoch the epoc to be used for the fencing and create calls.
     */
    public void initialize(long containerEpoch) {
        this.containerEpoc = containerEpoch;
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return zkClient.checkExists().forPath(getZkPath(streamSegmentName)).toCompletableFuture().thenApply(stat -> stat != null);
    }

    public CompletableFuture<Integer> read(String segmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        final AtomicReference<Integer> currentLength = new AtomicReference<>(length);
        final AtomicReference<Long> currentOffset = new AtomicReference<>(offset);
        final AtomicReference<Integer> currentBufferOffset = new AtomicReference<>(bufferOffset);

        return getOrRetrieveStorageLedger(segmentName, true)
                .thenCompose(ledger -> {
                    if (offset + length > ledger.getLength()) {
                        throw new CompletionException(new IllegalArgumentException(segmentName));
                    }
                    return FutureHelpers.loop(
                            () -> {
                                return currentLength.get() != 0;
                            },
                            () -> ledger.getLedgerDataForReadAt(currentOffset.get())
                                        .thenCompose(ledgerData -> readDataFromLedger(ledgerData, currentOffset.get(),
                                                buffer, currentBufferOffset.get(), currentLength.get()))
                                        .thenApply(dataRead -> {
                                            currentLength.set(currentLength.get() - dataRead);
                                            currentOffset.set(currentOffset.get() + dataRead);
                                            currentBufferOffset.set(currentBufferOffset.get() + dataRead);
                                            return null;
                                        }),
                            executor);
                })
                .thenApply(v -> length);
    }

    private CompletableFuture<Integer> readDataFromLedger(LedgerData ledgerData, long offset, byte[] buffer, int bufferOffset, int length) {
        /* TODO: This is brute force. Put some hurisitcs. Some possibilities:
         * 1. Store already read data for re-reading
         * 2. Store written data in last-accessed format.
         * 3. Store the current read pointer. Reads will always be sequential
        **/
        final AtomicReference<Long> currentOffset = new AtomicReference<>(offset - ledgerData.getStartOffset());
        final AtomicReference<Integer> lengthRemaining = new AtomicReference<>(length);
        final AtomicReference<Integer> currentBufferOffset = new AtomicReference<>(bufferOffset);
        final AtomicReference<Boolean> readingDone = new AtomicReference<>(false);
        final AtomicReference<Long> firstEntryId = new AtomicReference<>((long) 0);
        int nearestEntryId = ledgerData.getNearestEntryIdToOffset(currentOffset.get());
        int entriesInOneRound = 70;

        return FutureHelpers.loop(() -> !readingDone.get(),
                () -> {
                    CompletableFuture<Void> future = new CompletableFuture<>();

                    long lastEntryId;
                    if (ledgerData.getLastAddConfirmed() - firstEntryId.get() < entriesInOneRound) {
                        lastEntryId = ledgerData.getLastAddConfirmed();
                    } else {
                        lastEntryId = firstEntryId.get() + entriesInOneRound;
                    }
                    ledgerData.getLh().asyncReadEntries(firstEntryId.get(), lastEntryId,
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
                                                             .name(streamSegmentName).length(ledger.getLength())
                                                             .sealed(ledger.isSealed())
                                                             .lastModified(ledger.getLastModified())
                                                             .build());
    }

    private CompletableFuture<StorageLedger> getOrRetrieveStorageLedger(String streamSegmentName, boolean readOnly) {
        CompletableFuture<StorageLedger> retVal = new CompletableFuture<>();

        synchronized (this) {
            if (ledgers.containsKey(streamSegmentName)) {
                StorageLedger ledger = ledgers.get(streamSegmentName);
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

    private CompletableFuture<StorageLedger> retrieveStorageLedgerMetadata(String streamSegmentName, boolean readOnly) {

        Stat stat = new Stat();
        return zkClient.getData().storingStatIn(stat).forPath(getZkPath(streamSegmentName)).toCompletableFuture()
                       .exceptionally(exc -> {
                           translateZKException(streamSegmentName, exc);
                           return null;
                       })
                       .thenApply(bytes -> {
                           StorageLedger retval = StorageLedger.deserialize(this, streamSegmentName, bytes, stat.getVersion());
                           ledgers.put(streamSegmentName, retval);
                           return retval;
                       })
                       .thenCompose(storageLedger -> {
                           if (readOnly) {
                               return getBKLEdgerDetails(streamSegmentName, storageLedger);
                           } else {
                               CompletableFuture<StorageLedger> retVal = new CompletableFuture<>();
                               retVal.complete(storageLedger);
                               return retVal;
                           }
                       });
    }

    private CompletableFuture<StorageLedger> getBKLEdgerDetails(String streamSegmentName, StorageLedger storageLedger) {
        return zkClient.getChildren().forPath(getZkPath(streamSegmentName))
                       .toCompletableFuture()
                       .thenCompose(children -> {
                           CompletableFuture<LedgerData>[] futures = children.stream()
                                                                             .map(child -> {
                                                                                 int offset = Integer.valueOf(child);
                                                                                 Stat stat = new Stat();
                                                                                 return zkClient.getData()
                                                                                                .storingStatIn(stat)
                                                                                                .forPath(getZkPath(streamSegmentName) + "/" + child)
                                                                                                .thenApply(bytes -> {
                                                                                                    LedgerData ledgerData = deserializeLedgerData(Integer.valueOf(child),
                                                                                                            bytes, true, stat);
                                                                                                    ledgerData.setLastAddConfirmed(ledgerData.getLh().getLastAddConfirmed());
                                                                                                    ledgerData.setLength((int) ledgerData.getLh().getLength());
                                                                                                    storageLedger.addToList(offset, ledgerData);
                                                                                                    return ledgerData;
                                                                                                });
                                                                             })
                                                                             .toArray(CompletableFuture[]::new);
                           return CompletableFuture.allOf(futures).thenApply(v -> storageLedger);
                       });
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
        LedgerData ld = new LedgerData(lh, startOffset, stat.getVersion(), containerEpoc);
        ld.setLength((int) lh.getLength());
        ld.setReadonly(lh.isClosed());
        ld.setLastAddConfirmed(lh.getLastAddConfirmed());
        return ld;
    }

    public CompletableFuture<Void> write(String segmentName, long offset, InputStream data, int length) {
        log.info("Writing {} at offset {} for segment {}", length, offset, segmentName);

        return getOrRetrieveStorageLedger(segmentName, false)
                .thenCompose((StorageLedger ledger) -> {
                    if (ledger.getLength() != offset) {
                        throw new CompletionException(new BadOffsetException(segmentName, ledger.getLength(), offset));
                    }
                    if (ledger.isSealed()) {
                        throw new CompletionException(new StreamSegmentSealedException(segmentName));
                    }
                    return getORCreateLHForOffset(ledger, offset);
                }).thenCompose(ledgerData -> {
                    CompletableFuture<Void> retVal = writeDataAt(ledgerData.getLh(), offset, data, length, segmentName)
                            .thenApply(v -> {
                                ledgerData.increaseLengthBy(length);
                                ledgerData.setLastAddConfirmed(ledgerData.getLastAddConfirmed() + 1);
                                ledgers.get(segmentName).increaseLengthBy(length);
                                return v;
                            });
                    return retVal;
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

    private Throwable translateBKException(BKException e, String segmentName) {
        if (e instanceof BKException.BKLedgerClosedException
                || e instanceof BKException.BKIllegalOpException
                || e instanceof BKException.BKLedgerFencedException) {
            return new StorageNotPrimaryException(segmentName);
        }
        return e;
    }

    private CompletableFuture<LedgerData> getORCreateLHForOffset(StorageLedger ledger, long offset) {
        return ledger.getLedgerDataForWriteAt(offset);
    }

    public CompletableFuture<Void> seal(String segmentName) {
        return this.getOrRetrieveStorageLedger(segmentName, false)
                   .thenCompose(ledger -> {
                       if (ledger.getContainerEpoc() > this.containerEpoc) {
                           throw new CompletionException(new StorageNotPrimaryException(segmentName));
                       }
                       ledger.setSealed();
                       return zkClient.setData()
                                      .withVersion(ledger.getUpdateVersion())
                                      .forPath(getZkPath(segmentName))
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
                   .thenCompose(ledger -> this.sealLedger(ledger.getLastLedgerData()));
    }

    private CompletableFuture<Void> sealLedger(LedgerData lastLedgerData) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                lastLedgerData.getLh().close();
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public CompletableFuture<Void> concat(String segmentName, String sourceSegment, long offset, Duration timeout) {
        return this.getZKOperationsForConcat(segmentName, sourceSegment, offset)
                   .thenCompose(curatorOps -> zkClient.transaction()
                                                      .forOperations(curatorOps).toCompletableFuture()
                                                      .exceptionally(exc -> {
                                                          ledgers.remove(segmentName);
                                                          translateZKException(segmentName, exc);
                                                          return null;
                                                      })
                                                      .thenCompose(results -> {
                                                          return getOrRetrieveStorageLedger(segmentName, false)
                                                                  .thenCompose(storageLedger -> {
                                                                      storageLedger.setUpdateVersion(storageLedger.getUpdateVersion() + 1);
                                                                      return fenceLedgersAndCreateOneAtEnd(segmentName, storageLedger);
                                                                  })
                                                                  .thenApply(storageLedger -> null);
                                                      }))
                   .thenCompose(v -> this.zkClient.delete()
                                                  .withOptions(new HashSet<>(
                                                          Arrays.asList(DeleteOption.guaranteed,
                                                                  DeleteOption.deletingChildrenIfNeeded)))
                                                  .forPath(getZkPath(sourceSegment))
                                                  .toCompletableFuture()
                                                  .thenApply(ret -> {
                                                      ledgers.remove(sourceSegment);
                                                      return null;
                                                  }));
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
                               if (!source.isSealed()) {
                                   throw new IllegalStateException("source must be sealed");
                               }
                               if (source.getContainerEpoc() != this.containerEpoc) {
                                   throw new CompletionException(new StorageNotPrimaryException(target.getName()));
                               }
                               List<CuratorOp> operations = new ArrayList<>();
                               LedgerData lastLedger = target.getLastLedgerData();
                               if (lastLedger.getLh().getLength() == 0) {
                                   operations.add(createLedgerDeleteOp(lastLedger, target));
                               }
                               List<CuratorOp> retVal = target.addLedgerDataFrom(source);
                               operations.addAll(retVal);
                               // Update the segment also to ensure fencing has not happened
                               operations.add(createLedgerUpdateOp(target));
                               return operations;
                           });
    }

    private CuratorOp createLedgerDeleteOp(LedgerData lastLedger, StorageLedger target) {
        return zkClient.transactionOp().delete()
                       .forPath(getZkPath(target.getName()) + "/" + lastLedger.getStartOffset());
    }

    private CuratorOp createLedgerUpdateOp(StorageLedger target) {
        return zkClient.transactionOp().setData().withVersion(target.getUpdateVersion())
                       .forPath(getZkPath(target.getName()), target.serialize());
    }

    public CompletableFuture<Void> delete(String segmentName) {
        return this.getOrRetrieveStorageLedger(segmentName, false)
                   .thenCompose(ledger -> {
                       if (ledger.getContainerEpoc() > this.containerEpoc) {
                           throw new CompletionException(new StorageNotPrimaryException(segmentName));
                       }
                       return this.zkClient.delete()
                                           .withOptions(new HashSet<>(
                                                   Arrays.asList(DeleteOption.guaranteed,
                                                           DeleteOption.deletingChildrenIfNeeded)))
                                           .forPath(getZkPath(segmentName))
                                           .toCompletableFuture()
                                           .thenApply(v -> {
                                               ledgers.remove(segmentName);
                                               return ledger;
                                           });
                   })
                   .thenCompose(ledger -> ledger.deleteAllLedgers());
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

    public synchronized void dropCachedValue(String streamSegmentName) {
        if (ledgers.containsKey(streamSegmentName)) {
            ledgers.remove(streamSegmentName);
        }
    }
}
