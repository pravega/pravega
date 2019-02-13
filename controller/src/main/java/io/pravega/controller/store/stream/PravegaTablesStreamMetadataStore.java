/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.index.PravegaTablesHostIndex;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.utils.ZKPaths;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * ZK stream metadata store.
 */
@Slf4j
class PravegaTablesStreamMetadataStore extends AbstractStreamMetadataStore {
    private final ZkInt96Counter counter;
    private final ZKGarbageCollector completedTxnGC;
    private final PravegaTablesStoreHelper storeHelper;
    
    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, ZKGarbageCollector gc, ZkInt96Counter counter, Executor executor) {
        super(new PravegaTablesHostIndex(segmentHelper, "/hostTxnIndex", executor));
        this.counter = counter;
        this.completedTxnGC = gc;
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper);
    }
    
    @Override
    PravegaTablesStream newStream(final String scope, final String name) {
        return new PravegaTablesStream(scope, name, storeHelper, completedTxnGC::getLatestBatch);
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return counter.getNextCounter();
    }

    @Override
    Version getEmptyVersion() {
        return Version.IntVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
    }
    
    @Override
    PravegaTableScope newScope(final String scopeName) {
        return new PravegaTableScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        return segmentHelper.checkExists(String.format("/store/%s", scopeName))
                            .thenApply(scopeExists -> {
                    if (scopeExists) {
                        return scopeName;
                    } else {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName);
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        return segmentHelper.listScopes();
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName) {
        ZKStream stream = newStream(scopeName, streamName);
        return segmentHelper.checkExists(stream.getStreamPath());
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        return segmentHelper.getData(String.format(DELETED_STREAMS_PATH, getScopedStreamName(scopeName, streamName)))
                            .handleAsync((data, ex) -> {
                              if (ex == null) {
                                  return BitConverter.readInt(data.getData(), 0) + 1;
                              } else if (ex instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.error("Problem found while getting a safe starting segment number for {}.",
                                          getScopedStreamName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          });
    }

    @Override
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, final int lastActiveSegment,
                                                    OperationContext context, final Executor executor) {
        final String deletePath = String.format(DELETED_STREAMS_PATH, getScopedStreamName(scope, stream));
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return segmentHelper.getData(deletePath)
                            .exceptionally(e -> {
                              if (e instanceof StoreException.DataNotFoundException) {
                                  return null;
                              } else {
                                  throw new CompletionException(e);
                              }
                          })
                            .thenCompose(data -> {
                              log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
                              if (data == null) {
                                  return Futures.toVoid(segmentHelper.createZNodeIfNotExist(deletePath, maxSegmentNumberBytes));
                              } else {
                                  final int oldLastActiveSegment = BitConverter.readInt(data.getData(), 0);
                                  Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                          "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                          oldLastActiveSegment, scope, stream, lastActiveSegment);
                                  return Futures.toVoid(segmentHelper.setData(deletePath, new Data(maxSegmentNumberBytes, data.getVersion())));
                              }
                          });
    }

    private String encodedScopedStreamName(String scope, String stream) {
        String scopedStreamName = getScopedStreamName(scope, stream);
        return Base64.getEncoder().encodeToString(scopedStreamName.getBytes());
    }

    private String decodedScopedStreamName(String encodedScopedStreamName) {
        return new String(Base64.getDecoder().decode(encodedScopedStreamName));
    }

    private StreamImpl getStreamFromPath(String path) {
        String scopedStream = decodedScopedStreamName(ZKPaths.getNodeFromPath(path));
        String[] splits = scopedStream.split("/");
        return new StreamImpl(splits[0], splits[1]);
    }

    // region getters and setters for testing
    @VisibleForTesting
    void setCounterAndLimitForTesting(int counterMsb, long counterLsb, int limitMsb, long limitLsb) {
        synchronized (lock) {
            limit.set(limitMsb, limitLsb);
            counter.set(counterMsb, counterLsb);
        }
    }

    @VisibleForTesting
    Int96 getLimitForTesting() {
        synchronized (lock) {
            return limit.get();
        }
    }

    @VisibleForTesting
    Int96 getCounterForTesting() {
        synchronized (lock) {
            return counter.get();
        }
    }

    @VisibleForTesting
    public void setStoreHelperForTesting(ZKStoreHelper storeHelper) {
        this.segmentHelper = storeHelper;
    }

    @Override
    public void close() {
        completedTxnGC.stopAsync();
        completedTxnGC.awaitTerminated();
    }
    // endregion
}
