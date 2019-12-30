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
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.util.Config;
import io.pravega.shared.NameUtils;
import io.pravega.client.state.Revision;
import io.pravega.client.segment.impl.Segment;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedTableName;

/**
 * Pravega Tables stream metadata store.
 */
@Slf4j
public class PravegaTablesStreamMetadataStore extends AbstractStreamMetadataStore {
    static final String SEPARATOR = ".#.";
    static final String SCOPES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "scopes");
    static final String DELETED_STREAMS_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "deletedStreams");
    static final String COMPLETED_TRANSACTIONS_BATCHES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, 
            "completedTransactionsBatches");
    static final String COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT = "completedTransactionsBatch-%d";

    private static final String COMPLETED_TXN_GC_NAME = "completedTxnGC";

    private final ZkInt96Counter counter;
    private final AtomicReference<ZKGarbageCollector> completedTxnGCRef;
    private final ZKGarbageCollector completedTxnGC;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final PravegaTablesStoreHelper storeHelper;
    private final ZkOrderedStore orderer;
    private LocalController localController;
    private final CountDownLatch controllerReadyLatch;

    private final ScheduledExecutorService executor;
    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework client, ScheduledExecutorService executor, GrpcAuthHelper authHelper) {
        this(segmentHelper, client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), authHelper);
    }

    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework curatorClient, ScheduledExecutorService executor, Duration gcPeriod, GrpcAuthHelper authHelper) {
        super(new ZKHostIndex(curatorClient, "/hostTxnIndex", executor), new ZKHostIndex(curatorClient, "/hostRequestIndex", executor));
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(curatorClient, executor);
        this.orderer = new ZkOrderedStore("txnCommitOrderer", zkStoreHelper, executor);
        this.completedTxnGC = new ZKGarbageCollector(COMPLETED_TXN_GC_NAME, zkStoreHelper, this::gcCompletedTxn, gcPeriod);
        this.completedTxnGC.startAsync();
        this.completedTxnGC.awaitRunning();
        this.completedTxnGCRef = new AtomicReference<>(completedTxnGC);
        this.counter = new ZkInt96Counter(zkStoreHelper);
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
        this.executor = executor;
        this.controllerReadyLatch = new CountDownLatch(1);

        // GRPCServerConfig grpcServerConfig = serviceConfig.getGRPCServerConfig().get();
        // setController(new LocalController(controllerService, grpcServerConfig.isAuthorizationEnabled(),
        //            grpcServerConfig.getTokenSigningKey()));
    }

    @VisibleForTesting
    public LocalController getController() throws InterruptedException {
        controllerReadyLatch.await();
        return this.localController;
    }

    private void setController(LocalController controller) {
        this.localController = controller;
        controllerReadyLatch.countDown();
    }

    @VisibleForTesting 
    CompletableFuture<Void> gcCompletedTxn() {
        List<String> batches = new ArrayList<>();
        return withCompletion(storeHelper.expectingDataNotFound(storeHelper.getAllKeys(COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                                                         .collectRemaining(batches::add)
                                                         .thenApply(v -> findStaleBatches(batches))
                                                         .thenCompose(toDeleteList -> {
                                                             log.debug("deleting batches {} on new scheme", toDeleteList);

                                                             // delete all those marked for toDelete.
                                                             return Futures.allOf(
                                                                     toDeleteList.stream()
                                                                                 .map(toDelete -> {
                                                                                     String table = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, 
                                                                                             String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, Long.parseLong(toDelete)));
                                                                                     return storeHelper.deleteTable(table, false);
                                                                                 })
                                                                                 .collect(Collectors.toList()))
                                                                           .thenCompose(v -> storeHelper.removeEntries(COMPLETED_TRANSACTIONS_BATCHES_TABLE, toDeleteList));
                                                         }), null), executor);
    }

    @VisibleForTesting
    List<String> findStaleBatches(List<String> batches) {
        // exclude latest two batches and return remainder.
        if (batches.size() > 2) {
            int biggestIndex = Integer.MIN_VALUE;
            int secondIndex = Integer.MIN_VALUE;
            long biggest = Long.MIN_VALUE;
            long second = Long.MIN_VALUE;
            for (int i = 0; i < batches.size(); i++) {
                long element = Long.parseLong(batches.get(i));
                if (element > biggest) {
                    secondIndex = biggestIndex;
                    second = biggest;
                    biggest = element;
                    biggestIndex = i;
                } else if (element > second) {
                    secondIndex = i;
                    second = element;
                }
            }

            List<String> list = new ArrayList<>(batches);

            list.remove(biggestIndex);
            if (biggestIndex < secondIndex) {
                list.remove(secondIndex - 1);
            } else {
                list.remove(secondIndex);
            }
            return list;
        } else {
            return new ArrayList<>();
        }
    }

    @VisibleForTesting
    void setCompletedTxnGCRef(ZKGarbageCollector garbageCollector) {
        completedTxnGCRef.set(garbageCollector);
    }

    @Override
    PravegaTablesStream newStream(final String scope, final String name) {
        return new PravegaTablesStream(scope, name, storeHelper, orderer, completedTxnGCRef.get()::getLatestBatch,
                () -> ((PravegaTablesScope) getScope(scope)).getStreamsInScopeTableName(), executor);
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return withCompletion(counter.getNextCounter(), executor);
    }

    @Override
    CompletableFuture<Boolean> checkScopeExists(String scope) {
        return withCompletion(storeHelper.expectingDataNotFound(
                storeHelper.getEntry(SCOPES_TABLE, scope, x -> x).thenApply(v -> true),
                false), executor);
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(final String scope,
                                                                final String name,
                                                                final StreamConfiguration configuration,
                                                                final long createTimestamp,
                                                                final OperationContext context,
                                                                final Executor executor) {
        return withCompletion(
                ((PravegaTablesScope) getScope(scope))
                        .addStreamToScope(name)
                        .thenCompose(id -> super.createStream(scope, name, configuration, createTimestamp, context, executor)), 
                executor);
    }

    @Override
    public CompletableFuture<Void> deleteStream(final String scope,
                                                final String name,
                                                final OperationContext context,
                                                final Executor executor) {
        return withCompletion(super.deleteStream(scope, name, context, executor)
                    .thenCompose(status -> ((PravegaTablesScope) getScope(scope)).removeStreamFromScope(name).thenApply(v -> status)),
                executor);
    }

    @Override
    Version getEmptyVersion() {
        return Version.LongVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
    }

    @Override
    PravegaTablesScope newScope(final String scopeName) {
        return new PravegaTablesScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        return withCompletion(storeHelper.getEntry(SCOPES_TABLE, scopeName, x -> x)
                          .thenApply(x -> scopeName), executor);
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        List<String> scopes = new ArrayList<>();
        return withCompletion(Futures.exceptionallyComposeExpecting(storeHelper.getAllKeys(SCOPES_TABLE)
                                                                .collectRemaining(scopes::add)
                                                                .thenApply(v -> scopes), DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(SCOPES_TABLE).thenApply(v -> Collections.emptyList())),
                executor);
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName) {
        return withCompletion(((PravegaTablesScope) getScope(scopeName)).checkStreamExistsInScope(streamName), executor);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        return withCompletion(storeHelper.getEntry(DELETED_STREAMS_TABLE, getScopedStreamName(scopeName, streamName), 
                x -> BitConverter.readInt(x, 0))
                          .handle((data, ex) -> {
                              if (ex == null) {
                                  return data.getObject() + 1;
                              } else if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.error("Problem found while getting a safe starting segment number for {}.",
                                          getScopedStreamName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          }), executor);
    }

    @Override
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, final int lastActiveSegment,
                                                    OperationContext context, final Executor executor) {
        final String key = getScopedStreamName(scope, stream);
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return withCompletion(storeHelper.createTable(DELETED_STREAMS_TABLE)
                          .thenCompose(created -> {
                              return storeHelper.expectingDataNotFound(storeHelper.getEntry(
                                      DELETED_STREAMS_TABLE, key, x -> BitConverter.readInt(x, 0)),
                                      null)
                                            .thenCompose(existing -> {
                                                log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
                                                if (existing != null) {
                                                    final int oldLastActiveSegment = existing.getObject();
                                                    Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                                            "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                                            oldLastActiveSegment, scope, stream, lastActiveSegment);
                                                    return Futures.toVoid(storeHelper.updateEntry(DELETED_STREAMS_TABLE,
                                                            key, maxSegmentNumberBytes, existing.getVersion()));
                                                } else {
                                                    return Futures.toVoid(storeHelper.addNewEntryIfAbsent(DELETED_STREAMS_TABLE,
                                                            key, maxSegmentNumberBytes));
                                                }
                                            });
                          }), executor);
    }

    @Override
    public void close() {
        completedTxnGC.stopAsync();
        completedTxnGC.awaitTerminated();
    }


    /**
     * Appends an event to the stream.
     *
     * @param routingKey Name of routingKey to be used.
     * @param scopeName Name of scope to be used.
     * @param streamName Name of stream to be used.
     * @param message raw data to be appended to stream.
     */
    @Override
    public CompletableFuture<Void> createEvent(final String  routingKey,
                                               final String scopeName,
                                               final String streamName,
                                               final String message) {

        CompletableFuture<Void> ack = CompletableFuture.completedFuture(null);
        ClientConfig clientConfig = this.getStoreHelper().getSegmentHelper().getConnectionFactory().getClientConfig();
        SynchronizerClientFactory synchronizerClientFactory = SynchronizerClientFactory.withScope(scopeName, ClientConfig.builder().build());
        RevisionedStreamClient<String>  revisedStreamClient = synchronizerClientFactory.createRevisionedStreamClient(
                NameUtils.getMarkStreamForStream(streamName),
                new JavaSerializer<String>(), SynchronizerConfig.builder().build());
        revisedStreamClient.writeUnconditionally(message);
        return ack;

//        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder()
//                .connectionString(Config.ZK_URL)
//                .secureConnectionToZooKeeper(Config.SECURE_ZK)
//                .trustStorePath(Config.ZK_TRUSTSTORE_FILE_PATH)
//                .trustStorePasswordPath(Config.ZK_TRUSTSTORE_PASSWORD_FILE_PATH)
//                .namespace("pravega/" + Config.CLUSTER_NAME)
//                .initialSleepInterval(Config.ZK_RETRY_SLEEP_MS)
//                .maxRetries(Config.ZK_MAX_RETRIES)
//                .sessionTimeoutMs(Config.ZK_SESSION_TIMEOUT_MS)
//                .build();
//
//        StoreClientConfig storeClientConfig = StoreClientConfigImpl.withPravegaTablesClient(zkClientConfig);
//        ClientConfig clientConfig = ClientConfig.builder()
//                .credentials(new DefaultCredentials("well-known-password", "well-known-user"))
//                .controllerURI(URI.create("tcp://nautilus-pravega-controller.nautilus-pravega:9090"))
//                .build();
//
//        try {
//            ClientFactoryImpl clientFactory = new ClientFactoryImpl(scopeName, this.localController);
//            final Serializer<String> serializer = new JavaSerializer<>();
//            final Random random = new Random();
//            final Supplier<String> keyGenerator = () -> String.valueOf(random.nextInt());
//            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
//                    EventWriterConfig.builder().build());
//
//            ack = writer.writeEvent(keyGenerator.get(), message);
//            // ack.get();
//            log.info("event created for scope:{} stream:{}", scopeName, streamName);
//        } catch (Exception e) {
//            log.error("Exception:", e);
//        }
//        return localController.createEvent(routingKey, scopeName, streamName, message);
//        StreamSegments currentSegments = Futures.getAndHandleExceptions(localController.getCurrentSegments(scopeName, streamName), InvalidStreamException::new);
//        if ( currentSegments == null || currentSegments.getSegments().size() == 0) {
//            throw new InvalidStreamException("Stream does not exist: " + streamName);
//        }
//        io.pravega.client.segment.impl.Segment segment =  currentSegments.getSegmentForKey(0.0);
//        return storeHelper.createEvent("", scopeName, streamName, message, segment);
    }

    /**
     * Gets an event from the stream.
     *
     * @param routingKey Name of routingKey to be used.
     * @param scopeName Name of scope to be used.
     * @param streamName Name of stream to be used.
     * @param segmentNumber segment of the stream.
     */
    @Override
    public CompletableFuture<String> getEvent(final String  routingKey,
                                              final String scopeName,
                                              final String streamName,
                                              final Long segmentNumber) {
        ClientConfig clientConfig = this.getStoreHelper().getSegmentHelper().getConnectionFactory().getClientConfig();
        SynchronizerClientFactory synchronizerClientFactory = SynchronizerClientFactory.withScope(scopeName, ClientConfig.builder().build());
        RevisionedStreamClient<String>  revisedStreamClient = synchronizerClientFactory.createRevisionedStreamClient(
                NameUtils.getMarkStreamForStream(streamName),
                new JavaSerializer<String>(), SynchronizerConfig.builder().build());
        Revision r = revisedStreamClient.fetchLatestRevision();
        Segment s = r.getSegment();
        io.pravega.client.stream.Stream stream = s.getStream();
        revisedStreamClient.readFrom(r);
        Iterator<Map.Entry<Revision, String>> iter = revisedStreamClient.readFrom(r);
        StringBuffer sb = new StringBuffer();
        while (iter.hasNext()) {
            Map.Entry<Revision, String> entry = iter.next();
            sb.append(entry.getValue());
        }
        CompletableFuture<String> ack = CompletableFuture.completedFuture(sb.toString());
        return ack;
    }
}
