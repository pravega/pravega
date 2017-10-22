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

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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

    public StorageLedgerManager(BookkeeperStorageConfig config, CuratorFramework zkClient, ExecutorService executor) {
        this.config = config;
        this.executor = executor;
        this.zkClient = AsyncCuratorFramework.wrap(zkClient);
        ledgers = new ConcurrentHashMap<>();

    }

    /**
     * Creates a StorageLedger at location streamSegmentName
     *
     * @param streamSegmentName Full name of the stream segment
     * @param timeout           timeout value for the operation
     * @return A new instance of StorageLedger if one does not already exist
     * @Param bookkeeper the BK instance
     * @Param zkClient
     */
    public CompletableFuture<LedgerData> create(String streamSegmentName, Duration timeout) {
        return zkClient.create()
                       .forPath(getZkPath(streamSegmentName))
                       .toCompletableFuture()
                       .exceptionally(exc ->  {
                           translateZKException(streamSegmentName, exc);
                           return null;
                       })
                       .thenApply( name -> {
                           ledgers.put(streamSegmentName, new StorageLedger(this, streamSegmentName));
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
        return zkClient.getChildren().forPath(getZkPath(streamSegmentName))
                       .toCompletableFuture()
                       .exceptionally(exc -> {
                           translateZKException(streamSegmentName, exc);
                           return null;
                       })
                       .thenApply(
                               children -> {
                                   if (children.size() == 0) {
                                       return createLedgerAt(streamSegmentName, 0);
                                   } else {
                                       Optional<String> lastLedger = children.stream().max((x, y) -> (Integer.valueOf(x) < Integer.valueOf(y)) ? -1 : 1);
                                       return fenceAndCreateNewLedger(streamSegmentName, Integer.valueOf(lastLedger.get()));
                                   }
                               });
    }

    private CompletableFuture<?> fenceAndCreateNewLedger(String streamSegmentName, Integer lastLedgerOffset) {
        return fenceLedgerAt(streamSegmentName, lastLedgerOffset).thenApply(
                newOffset -> createLedgerAt(streamSegmentName, newOffset));
    }

    private CompletableFuture<Integer> fenceLedgerAt(String streamSegmentName, int firstOffset) {
        return zkClient.getData().forPath(streamSegmentName + "/" + firstOffset)
                       .toCompletableFuture()
                       .thenApply(data -> {
                           LedgerData ledgerData = deserializeLedgerData(firstOffset, data);
                           try {
                               ledgerData.getLh().close();
                           } catch (Exception e) {
                               throw new CompletionException(e);
                           }
                           return ledgerData.getLength();
                          });
    }

    public CompletableFuture<LedgerData> createLedgerAt(String streamSegmentName, int offset) {
        CompletableFuture<LedgerData> retVal = new CompletableFuture<>();

        bookkeeper.asyncCreateLedger(config.getBkEnsembleSize(),
                config.getBkWriteQuorumSize(), LEDGER_DIGEST_TYPE, config.getBKPassword(),
                (errorCode, ledgerHandle, future) -> {
                    if (errorCode == 0) {
                        LedgerData lh = new LedgerData(ledgerHandle, offset);
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
     * Initializes the bookkeeper and curator objects
     */
    public void initialize() {

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

        return getOrRetrieveStorageLedger(segmentName)
                .thenCompose(ledger -> {
                                if (offset + length > ledger.getLength()) {
                                    throw new CompletionException(new IllegalArgumentException(segmentName));
                                }
                                return FutureHelpers.loop(
                                        () -> {
                                            return currentLength.get() != 0;
                                        },
                                        () -> ledger.getLedgerDataForReadAt(currentOffset.get())
                                        .thenCompose(ledgerData ->  readDataFromLedger(ledgerData, currentOffset.get(),
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
       /*TODO: This is brute force. Put some hurisitcs. Some possibilities:
       * 1. Store already read data for re-reading
       * 2. Store written data in last-accessed format.
       * 3. Store the current read pointer. Reads will always be sequential*/
       final AtomicReference<Long> currentOffset = new AtomicReference<>(offset - ledgerData.getStartOffset());
       final AtomicReference<Integer> lengthRemaining = new AtomicReference<>(length);
       final AtomicReference<Integer> currentBufferOffset = new AtomicReference<>(bufferOffset);
       final AtomicReference<Boolean> readingDone = new AtomicReference<>(false);
       final AtomicReference<Long> firstEntryId = new AtomicReference<>((long) 0);
       int entriesInOneRound = 7;

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

                                      if (entry.getLength() <= currentOffset.get()) {
                                          currentOffset.set(currentOffset.get() - entry.getLength());
                                      } else {
                                          long startInEntry = currentOffset.get();
                                          try {
                                              InputStream stream = entry.getEntryInputStream();
                                              long skipped = stream.skip(startInEntry);

                                              int dataRemainingInEntry = (int) (entry.getLength() - startInEntry);

                                              int dataRead = 0;
                                                  dataRead = stream.read(buffer, currentBufferOffset.get(),
                                                          dataRemainingInEntry > lengthRemaining.get() ?
                                                                  lengthRemaining.get() : dataRemainingInEntry);
                                                  if (dataRead == -1) {
                                                      log.warn("Data read returned -1" + skipped);
                                                  }
                                              lengthRemaining.set(lengthRemaining.get() - dataRead);
                                              if (lengthRemaining.get() == 0) {
                                                  future.complete(null);
                                                  readingDone.set(true);
                                                  return;
                                              }
                                              currentBufferOffset.set(currentBufferOffset.get() + dataRead);
                                              currentOffset.set((long) 0);
                                          } catch (IOException e) {
                                              future.completeExceptionally(new CompletionException(e));
                                              return;
                                          }
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
     * @return
     */
    public CompletableFuture<SegmentProperties> getOrRetrieveStorageLedgerDetails(String streamSegmentName) {

        return getOrRetrieveStorageLedger(streamSegmentName)
                .thenApply(ledger -> StreamSegmentInformation.builder()
                                                             .name(streamSegmentName).length(ledger.getLength())
                                                             .sealed(ledger.isSealed())
                                                             .lastModified(ledger.getLastModified())
                                                             .build());
    }

    private CompletableFuture<StorageLedger> getOrRetrieveStorageLedger(String streamSegmentName) {
        CompletableFuture<StorageLedger> retVal = new CompletableFuture<>();

        synchronized (ledgers) {
            if (ledgers.containsKey(streamSegmentName)) {
                StorageLedger ledger = ledgers.get(streamSegmentName);
                retVal.complete(ledger);
                return retVal;
            }
        }
        return retrieveStorageLedgerMetadata(streamSegmentName);
    }

    private CompletableFuture<StorageLedger> retrieveStorageLedgerMetadata(String streamSegmentName) {
        StorageLedger storageLedger = new StorageLedger(this, streamSegmentName);
        return zkClient.getChildren().forPath(getZkPath(streamSegmentName)).toCompletableFuture()
                .exceptionally(exc -> {
                    translateZKException(streamSegmentName, exc);
                    return null;
                })
                .thenCompose(children -> {
                    CompletableFuture<LedgerData>[] retVal =  children.stream().map(child -> {
                        return zkClient.getData().forPath(child).toCompletableFuture().thenApply(bytes -> {
                            storageLedger.addToList(Integer.getInteger(child), deserializeLedgerData(Integer.getInteger(child), bytes));
                            return null;
                        });
                    }).toArray(CompletableFuture[]::new);
                    return CompletableFuture.allOf(retVal);
                }).thenApply(v -> {
                    ledgers.put(streamSegmentName, storageLedger);
                    return storageLedger;
                });
    }

    private LedgerData deserializeLedgerData(Integer startOffset, byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        LedgerHandle lh = null;
        try {
            lh = bookkeeper.openLedger(bb.getLong(), LEDGER_DIGEST_TYPE, config.getBKPassword());
        } catch (Exception e) {
          throw new CompletionException(e);
        }
        LedgerData ld = new LedgerData(lh, startOffset);
        ld.setLength((int) lh.getLength());
        ld.setReadonly(lh.isClosed());
        ld.setLastAddConfirmed(lh.getLastAddConfirmed());
        return ld;
    }

    public CompletableFuture<Void> write(String segmentName, long offset, InputStream data, int length) {
        return getOrRetrieveStorageLedger(segmentName)
                .thenCompose((StorageLedger ledger) -> {
                    if (ledger.length != offset) {
                        throw new CompletionException(new BadOffsetException(segmentName, ledger.length, offset));
                    }
                    if (ledger.sealed) {
                        throw new CompletionException(new StreamSegmentSealedException(segmentName));
                    }
                    return getORCreateLHForOffset(ledger, offset);
                }).thenCompose(ledgerData ->  {
                    CompletableFuture<Void> retVal = writeDataAt(ledgerData.getLh(), offset, data, length)
                            .thenApply(v -> {
                                ledgerData.increaseLengthBy(length);
                                ledgerData.setLastAddConfirmed(ledgerData.getLastAddConfirmed() + 1);
                                ledgers.get(segmentName).increaseLengthBy(length);
                                return v;
                            });
                    return retVal;
                });
        /**
         * TODO:
         * 3. and update the metadata/persist it*/
    }

    private  CompletableFuture<Void> writeDataAt(LedgerHandle lh, long offset, InputStream data, int length) {
        byte[] bytes = new byte[length];
        CompletableFuture<Void> retVal = new CompletableFuture<>();

        try {
            StreamHelpers.readAll(data, bytes, 0, length);
        } catch (IOException e) {
            retVal.completeExceptionally(e);
            return retVal;
        }

        lh.asyncAddEntry(bytes, (errorCode, ledgerHandle, l, future) -> {
            if( errorCode == 0) {
                retVal.complete(null);
            } else {
                retVal.completeExceptionally(BKException.create(errorCode));
            }
        }, retVal);

        return retVal;
    }

    private CompletableFuture<LedgerData> getORCreateLHForOffset(StorageLedger ledger, long offset) {
        return ledger.getLedgerDataForWriteAt(offset);
    }

    public CompletableFuture<Void> seal(String segmentName) {
        return this.getOrRetrieveStorageLedger(segmentName)
                .thenCompose(ledger -> this.sealLedger(ledger.getLastLedgerData()).thenApply(v -> ledger))
                .thenApply(ledger -> {
                    ledger.setSealed();
                    return null;
                });
        /**
        * TODO:
        * 1. Persist the fact that StorageLedger is sealed */
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
                .thenCompose(curatorOps -> {
                    return zkClient.transaction().forOperations(curatorOps).toCompletableFuture().thenApply(results -> null);
                });

    }

    private CompletableFuture<List<CuratorOp>> getZKOperationsForConcat(String segmentName, String sourceSegment, long offset) {
        return this.getOrRetrieveStorageLedger(segmentName)
                   .exceptionally(exc -> {
                                translateZKException(segmentName, exc);
                                return null;
                           }
                   )
            .thenCombine(this.getOrRetrieveStorageLedger(sourceSegment),
                (target, source) -> {
                if (!source.sealed) {
                    throw new IllegalStateException("source must be sealed");
                }
                return target.addLedgerDataFrom(source);
            });
    }

    public CompletableFuture<Void> delete(String segmentName) {
        return this.getOrRetrieveStorageLedger(segmentName)
            .thenCompose(ledger -> this.zkClient.delete()
                                                .withOptions(new HashSet<>(
                                                        Arrays.asList(DeleteOption.guaranteed,
                                                                DeleteOption.deletingChildrenIfNeeded)))
                                        .forPath(getZkPath(segmentName))
                                        .toCompletableFuture()
                                        .thenApply(v ->  {
                                            ledgers.remove(segmentName);
                                            return ledger;
                                        }))
            .thenCompose(ledger -> ledger.deleteAllLedgers());
    }

    public CompletableFuture<Void> deleteLedger(LedgerHandle lh) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        bookkeeper.asyncDeleteLedger(lh.getId(), (errorCode, o) -> {
            if (errorCode == 0) {
                future.complete(null);
            } else {
                future.completeExceptionally(BKException.create(errorCode));
            }

        }, future);
        return future;
    }

    public CuratorOp createAddOp(String segmentName, int newKey, LedgerData value) {
        return zkClient.transactionOp().create().forPath(getZkPath(segmentName)+ "/" + newKey, value.serialize());
    }
}
