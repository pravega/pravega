/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.rpc.auth.PravegaInterceptor;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus.Status;
import io.pravega.controller.timeout.TimeoutService;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.timeout.TimerWheelTimeoutService;
import io.pravega.controller.util.Config;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.pravega.controller.util.RetryHelper.RETRYABLE_PREDICATE;
import static io.pravega.controller.util.RetryHelper.withRetriesAsync;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamTransactionMetadataTasks implements AutoCloseable {
    /**
     * We derive the maximum execution timeout from the lease time. We assume
     * a maximum number of renewals for the txn and compute the maximum execution
     * time by multiplying the lease timeout by the maximum number of renewals. This
     * multiplier is currently hardcoded because we do not expect applications to change
     * it. The maximum execution timeout is only a safety mechanism for application that
     * should rarely be triggered.
     */
    private static final int MAX_EXECUTION_TIME_MULTIPLIER = 1000;

    protected EventStreamWriter<CommitEvent> commitEventEventStreamWriter;
    protected EventStreamWriter<AbortEvent> abortEventEventStreamWriter;
    protected String commitStreamName;
    protected String abortStreamName;
    protected final String hostId;
    protected final ScheduledExecutorService executor;

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final SegmentHelper segmentHelper;
    private final ConnectionFactory connectionFactory;
    private final boolean authorizationEnabled;
    private final String tokenSigningKey;
    @Getter
    @VisibleForTesting
    private final TimeoutService timeoutService;

    private volatile boolean ready;
    private final CountDownLatch readyLatch;

    @VisibleForTesting
    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final String hostId,
                                          final TimeoutServiceConfig timeoutServiceConfig,
                                          final BlockingQueue<Optional<Throwable>> taskCompletionQueue,
                                          final ConnectionFactory connectionFactory,
                                          boolean authorizationEnabled,
                                          String tokenSigningKey) {
        this.hostId = hostId;
        this.executor = executor;
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.connectionFactory = connectionFactory;
        this.authorizationEnabled = authorizationEnabled;
        this.tokenSigningKey = tokenSigningKey;
        this.timeoutService = new TimerWheelTimeoutService(this, timeoutServiceConfig, taskCompletionQueue);
        readyLatch = new CountDownLatch(1);
    }

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final String hostId,
                                          final TimeoutServiceConfig timeoutServiceConfig,
                                          final ConnectionFactory connectionFactory, boolean authorizationEnabled, String tokenSigningKey) {
        this.hostId = hostId;
        this.executor = executor;
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.connectionFactory = connectionFactory;
        this.timeoutService = new TimerWheelTimeoutService(this, timeoutServiceConfig);
        this.authorizationEnabled = authorizationEnabled;
        this.tokenSigningKey = tokenSigningKey;
        readyLatch = new CountDownLatch(1);
    }

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final String hostId,
                                          final ConnectionFactory connectionFactory,
                                          boolean authorizationEnabled,
                                          String tokenSigningKey) {
        this(streamMetadataStore, hostControllerStore, segmentHelper, executor, hostId,
                TimeoutServiceConfig.defaultConfig(), connectionFactory, authorizationEnabled, tokenSigningKey);
    }

    protected void setReady() {
        ready = true;
        readyLatch.countDown();
    }

    boolean isReady() {
        return ready;
    }

    @VisibleForTesting
    public boolean awaitInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return readyLatch.await(timeout, timeUnit);
    }

    public void awaitInitialization() throws InterruptedException {
        readyLatch.await();
    }

    /**
     * Initializes stream writers for commit and abort streams.
     * This method should be called immediately after creating StreamTransactionMetadataTasks object.
     *
     * @param clientFactory Client factory reference.
     * @param config Controller event processor configuration.
     */
    public Void initializeStreamWriters(final ClientFactory clientFactory,
                                        final ControllerEventProcessorConfig config) {
        this.commitStreamName = config.getCommitStreamName();
        this.commitEventEventStreamWriter = clientFactory.createEventWriter(
                config.getCommitStreamName(),
                ControllerEventProcessors.COMMIT_EVENT_SERIALIZER,
                EventWriterConfig.builder().build());

        this.abortStreamName = config.getAbortStreamName();
        this.abortEventEventStreamWriter = clientFactory.createEventWriter(
                config.getAbortStreamName(),
                ControllerEventProcessors.ABORT_EVENT_SERIALIZER,
                EventWriterConfig.builder().build());

        this.setReady();
        return null;
    }

    @VisibleForTesting
    public Void initializeStreamWriters(final String commitStreamName, final EventStreamWriter<CommitEvent> commitWriter,
                                        final String abortStreamName, final EventStreamWriter<AbortEvent> abortWriter) {
        this.commitStreamName = commitStreamName;
        this.commitEventEventStreamWriter = commitWriter;
        this.abortStreamName = abortStreamName;
        this.abortEventEventStreamWriter = abortWriter;
        this.setReady();
        return null;
    }

    /**
     * Create transaction.
     *
     * @param scope              stream scope.
     * @param stream             stream name.
     * @param lease              Time for which transaction shall remain open with sending any heartbeat.
     *                           the scaling operation is initiated on the txn stream.
     * @param contextOpt         operational context
     * @return transaction id.
     */
    public CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxn(final String scope,
                                                                                      final String stream,
                                                                                      final long lease,
                                                                                      final OperationContext contextOpt) {
        return checkReady().thenComposeAsync(x -> {
            final OperationContext context = getNonNullOperationContext(scope, stream, contextOpt);
            return createTxnBody(scope, stream, lease, context);
        }, executor);
    }

    /**
     * Transaction heartbeat, that increases transaction timeout by lease number of milliseconds.
     *
     * @param scope Stream scope.
     * @param stream Stream name.
     * @param txId Transaction identifier.
     * @param lease Amount of time in milliseconds by which to extend the transaction lease.
     * @param contextOpt       operational context
     * @return Transaction metadata along with the version of it record in the store.
     */
    public CompletableFuture<PingTxnStatus> pingTxn(final String scope,
                                                    final String stream,
                                                    final UUID txId,
                                                    final long lease,
                                                    final OperationContext contextOpt) {
        return checkReady().thenComposeAsync(x -> {
            final OperationContext context = getNonNullOperationContext(scope, stream, contextOpt);
            return pingTxnBody(scope, stream, txId, lease, context);
        }, executor);
    }

    /**
     * Abort transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @param version Expected version of the transaction record in the store.
     * @param contextOpt       operational context
     * @return true/false.
     */
    public CompletableFuture<TxnStatus> abortTxn(final String scope,
                                                 final String stream,
                                                 final UUID txId,
                                                 final Integer version,
                                                 final OperationContext contextOpt) {
        return checkReady().thenComposeAsync(x -> {
            final OperationContext context = getNonNullOperationContext(scope, stream, contextOpt);
            return withRetriesAsync(() -> sealTxnBody(hostId, scope, stream, false, txId, version, context),
                    RETRYABLE_PREDICATE, 3, executor);
        }, executor);
    }

    /**
     * Commit transaction.
     *
     * @param scope      stream scope.
     * @param stream     stream name.
     * @param txId       transaction id.
     * @param contextOpt optional context
     * @return true/false.
     */
    public CompletableFuture<TxnStatus> commitTxn(final String scope, final String stream, final UUID txId,
                                                  final OperationContext contextOpt) {
        return checkReady().thenComposeAsync(x -> {
            final OperationContext context = getNonNullOperationContext(scope, stream, contextOpt);
            return withRetriesAsync(() -> sealTxnBody(hostId, scope, stream, true, txId, null, context),
                    RETRYABLE_PREDICATE, 3, executor);
        }, executor);
    }

    /**
     * Creates txn on the specified stream.
     *
     * Post-condition:
     * 1. If txn creation succeeds, then
     *     (a) txn node is created in the store,
     *     (b) txn segments are successfully created on respective segment stores,
     *     (c) txn is present in the host-txn index of current host,
     *     (d) txn's timeout is being tracked in timeout service.
     *
     * 2. If process fails after creating txn node, but before responding to the client, then since txn is
     * present in the host-txn index, some other controller process shall abort the txn after maxLeaseValue
     *
     * 3. If timeout service tracks timeout of specified txn,
     * then txn is also present in the host-txn index of current process.
     *
     * Invariant:
     * The following invariants are maintained throughout the execution of createTxn, pingTxn and sealTxn methods.
     * 1. If timeout service tracks timeout of a txn, then txn is also present in the host-txn index of current process.
     * 2. If txn znode is updated, then txn is also present in the host-txn index of current process.
     *
     * @param scope               scope name.
     * @param stream              stream name.
     * @param lease               txn lease.
     * @param ctx                 context.
     * @return                    identifier of the created txn.
     */
    CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createTxnBody(final String scope,
                                                                                   final String stream,
                                                                                   final long lease,
                                                                                   final OperationContext ctx) {
        // Step 1. Validate parameters.
        CompletableFuture<Void> validate = validate(lease);
        long maxExecutionPeriod = Math.min(MAX_EXECUTION_TIME_MULTIPLIER * lease, Duration.ofDays(1).toMillis());

        // 1. get latest epoch from history
        // 2. generateNewTransactionId.. this step can throw WriteConflictException
        // 3. txn id = 32 bit epoch + 96 bit counter
        // 4. if while creating txn epoch no longer exists, then we will get DataNotFoundException.
        // 5. Retry if we get WriteConflict or DataNotFoundException, from step 1.
        // Note: this is a low probability for either exceptions:
        // - WriteConflict, if multiple controllers are trying to get new range at the same time then we can get write conflict
        // - DataNotFoundException because it will only happen in rare case
        // when we generate the transactionid against latest epoch (if there is ongoing scale then this is new epoch)
        // and then epoch node is deleted as scale starts and completes.
        return validate.thenCompose(validated -> RetryHelper.withRetriesAsync(() ->
                streamMetadataStore.generateTransactionId(scope, stream, ctx, executor)
                .thenCompose(txnId -> {
                    CompletableFuture<Void> addIndex = addTxnToIndex(scope, stream, txnId);

                    // Step 3. Create txn node in the store.
                    CompletableFuture<VersionedTransactionData> txnFuture = createTxnInStore(scope, stream, lease,
                            ctx, maxExecutionPeriod, txnId, addIndex);

                    // Step 4. Notify segment stores about new txn.
                    CompletableFuture<List<Segment>> segmentsFuture = txnFuture.thenComposeAsync(txnData ->
                            streamMetadataStore.getActiveSegments(scope, stream, txnData.getEpoch(), ctx, executor), executor);

                    CompletableFuture<Void> notify = segmentsFuture.thenComposeAsync(activeSegments ->
                            notifyTxnCreation(scope, stream, activeSegments, txnId), executor).whenComplete((v, e) ->
                            // Method notifyTxnCreation ensures that notification completes
                            // even in the presence of n/w or segment store failures.
                            log.trace("Txn={}, notified segments stores", txnId));

                    // Step 5. Start tracking txn in timeout service
                    return notify.whenCompleteAsync((result, ex) -> {
                        addTxnToTimeoutService(scope, stream, lease, maxExecutionPeriod, txnId, txnFuture);
                    }, executor).thenApplyAsync(v -> {
                        List<Segment> segments = segmentsFuture.join().stream().map(x -> {
                            long generalizedSegmentId = TableHelper.generalizedSegmentId(x.segmentId(), txnId);
                            return new Segment(generalizedSegmentId, x.getStart(), x.getKeyStart(), x.getKeyEnd());
                        }).collect(Collectors.toList());

                        return new ImmutablePair<>(txnFuture.join(), segments);
                    }, executor);
                }), e -> {
            Throwable unwrap = Exceptions.unwrap(e);
            return unwrap instanceof StoreException.WriteConflictException || unwrap instanceof StoreException.DataNotFoundException;
        }, 5, executor));
    }

    private void addTxnToTimeoutService(String scope, String stream, long lease, long maxExecutionPeriod, UUID txnId, CompletableFuture<VersionedTransactionData> txnFuture) {
        int version = 0;
        long executionExpiryTime = System.currentTimeMillis() + maxExecutionPeriod;
        if (!txnFuture.isCompletedExceptionally()) {
            version = txnFuture.join().getVersion();
            executionExpiryTime = txnFuture.join().getMaxExecutionExpiryTime();
        }
        timeoutService.addTxn(scope, stream, txnId, version, lease, executionExpiryTime);
        log.trace("Txn={}, added to timeout service on host={}", txnId, hostId);
    }

    private CompletableFuture<VersionedTransactionData> createTxnInStore(String scope, String stream, long lease,
                                                                         OperationContext ctx, long maxExecutionPeriod, UUID txnId,
                                                                         CompletableFuture<Void> addIndex) {
        return addIndex.thenComposeAsync(ignore ->
                                streamMetadataStore.createTransaction(scope, stream, txnId, lease, maxExecutionPeriod,
                                        ctx, executor), executor).whenComplete((v, e) -> {
                            if (e != null) {
                                log.debug("Txn={}, failed creating txn in store", txnId);
                            } else {
                                log.debug("Txn={}, created in store", txnId);
                            }
                        });
    }

    private CompletableFuture<Void> addTxnToIndex(String scope, String stream, UUID txnId) {
        TxnResource resource = new TxnResource(scope, stream, txnId);
        // Step 2. Add txn to host-transaction index.
        return streamMetadataStore.addTxnToIndex(hostId, resource, 0)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.debug("Txn={}, failed adding txn to host-txn index of host={}", txnId, hostId);
                    } else {
                        log.debug("Txn={}, added txn to host-txn index of host={}", txnId, hostId);
                    }
                });
    }

    @SuppressWarnings("ReturnCount")
    private CompletableFuture<Void> validate(long lease) {
        if (lease < Config.MIN_LEASE_VALUE) {
            return Futures.failedFuture(new IllegalArgumentException("lease should be greater than minimum lease"));
        }
        // If lease value is too large return error
        if (lease > timeoutService.getMaxLeaseValue()) {
            return Futures.failedFuture(new IllegalArgumentException("lease value too large, max value is "
                    + timeoutService.getMaxLeaseValue()));
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Ping a txn thereby updating its timeout to current time + lease.
     *
     * Post-condition:
     * 1. If ping request completes successfully, then
     *     (a) txn timeout is set to lease + current time in timeout service,
     *     (b) txn version in timeout service equals version of txn node in store,
     *     (c) if txn's timeout was not previously tracked in timeout service of current process,
     *     then version of txn node in store is updated, thus fencing out other processes tracking timeout for this txn,
     *     (d) txn is present in the host-txn index of current host,
     *
     * 2. If process fails before responding to the client, then since txn is present in the host-txn index,
     * some other controller process shall abort the txn after maxLeaseValue
     *
     * Store read/update operation is not invoked on receiving ping request for a txn that is being tracked in the
     * timeout service. Otherwise, if the txn is not being tracked in the timeout service, txn node is read from
     * the store and updated.
     *
     * @param scope      scope name.
     * @param stream     stream name.
     * @param txnId      txn id.
     * @param lease      txn lease.
     * @param ctx        context.
     * @return           ping status.
     */
    CompletableFuture<PingTxnStatus> pingTxnBody(final String scope,
                                                 final String stream,
                                                 final UUID txnId,
                                                 final long lease,
                                                 final OperationContext ctx) {
        if (!timeoutService.isRunning()) {
            return CompletableFuture.completedFuture(createStatus(Status.DISCONNECTED));
        }

        log.debug("Txn={}, updating txn node in store and extending lease", txnId);
        return fenceTxnUpdateLease(scope, stream, txnId, lease, ctx);
    }

    private PingTxnStatus createStatus(Status status) {
        return PingTxnStatus.newBuilder().setStatus(status).build();
    }

    private CompletableFuture<PingTxnStatus> fenceTxnUpdateLease(final String scope,
                                                                 final String stream,
                                                                 final UUID txnId,
                                                                 final long lease,
                                                                 final OperationContext ctx) {
        // Step 1. Check whether lease value is within necessary bounds.
        // Step 2. Add txn to host-transaction index.
        // Step 3. Update txn node data in the store,thus updating its version
        //         and fencing other processes from tracking this txn's timeout.
        // Step 4. Add this txn to timeout service and start managing timeout for this txn.
        return streamMetadataStore.getTransactionData(scope, stream, txnId, ctx, executor).thenComposeAsync(txnData -> {
            // Step 1. Sanity check for lease value.
            if (lease > timeoutService.getMaxLeaseValue()) {
                return CompletableFuture.completedFuture(createStatus(Status.LEASE_TOO_LARGE));
            } else if (lease + System.currentTimeMillis() > txnData.getMaxExecutionExpiryTime()) {
                return CompletableFuture.completedFuture(createStatus(Status.MAX_EXECUTION_TIME_EXCEEDED));
            } else {
                TxnResource resource = new TxnResource(scope, stream, txnId);
                int expVersion = txnData.getVersion() + 1;

                // Step 2. Add txn to host-transaction index
                CompletableFuture<Void> addIndex = streamMetadataStore.addTxnToIndex(hostId, resource, expVersion).whenComplete((v, e) -> {
                    if (e != null) {
                        log.debug("Txn={}, failed adding txn to host-txn index of host={}", txnId, hostId);
                    } else {
                        log.debug("Txn={}, added txn to host-txn index of host={}", txnId, hostId);
                    }
                });

                return addIndex.thenComposeAsync(x -> {
                    // Step 3. Update txn node data in the store.
                    CompletableFuture<VersionedTransactionData> pingTxn = streamMetadataStore.pingTransaction(
                            scope, stream, txnData, lease, ctx, executor).whenComplete((v, e) -> {
                        if (e != null) {
                            log.debug("Txn={}, failed updating txn node in store", txnId);
                        } else {
                            log.debug("Txn={}, updated txn node in store", txnId);
                        }
                    });

                    // Step 4. Add it to timeout service and start managing timeout for this txn.
                    return pingTxn.thenApplyAsync(data -> {
                        int version = data.getVersion();
                        long expiryTime = data.getMaxExecutionExpiryTime();
                        // Even if timeout service has an active/executing timeout task for this txn, it is bound
                        // to fail, since version of txn node has changed because of the above store.pingTxn call.
                        // Hence explicitly add a new timeout task.
                        if (timeoutService.containsTxn(scope, stream, txnId)) {
                            // If timeout service knows about this transaction, attempt to increase its lease.
                            log.debug("Txn={}, extending lease in timeout service", txnId);
                            timeoutService.pingTxn(scope, stream, txnId, version, lease);
                        } else {
                            timeoutService.addTxn(scope, stream, txnId, version, lease, expiryTime);
                        }
                        return createStatus(Status.OK);
                    }, executor);
                }, executor);
            }
        }, executor);
    }

    /**
     * Seals a txn and transitions it to COMMITTING (resp. ABORTING) state if commit param is true (resp. false).
     *
     * Post-condition:
     * 1. If seal completes successfully, then
     *     (a) txn state is COMMITTING/ABORTING,
     *     (b) CommitEvent/AbortEvent is present in the commit stream/abort stream,
     *     (c) txn is removed from host-txn index,
     *     (d) txn is removed from the timeout service.
     *
     * 2. If process fails after transitioning txn to COMMITTING/ABORTING state, but before responding to client, then
     * since txn is present in the host-txn index, some other controller process shall put CommitEvent/AbortEvent to
     * commit stream/abort stream.
     *
     * @param host    host id. It is different from hostId iff invoked from TxnSweeper for aborting orphaned txn.
     * @param scope   scope name.
     * @param stream  stream name.
     * @param commit  boolean indicating whether to commit txn.
     * @param txnId   txn id.
     * @param version expected version of txn node in store.
     * @param ctx     context.
     * @return        Txn status after sealing it.
     */
    CompletableFuture<TxnStatus> sealTxnBody(final String host,
                                             final String scope,
                                             final String stream,
                                             final boolean commit,
                                             final UUID txnId,
                                             final Integer version,
                                             final OperationContext ctx) {
        TxnResource resource = new TxnResource(scope, stream, txnId);
        Optional<Integer> versionOpt = Optional.ofNullable(version);

        // Step 1. Add txn to current host's index, if it is not already present
        CompletableFuture<Void> addIndex = host.equals(hostId) && !timeoutService.containsTxn(scope, stream, txnId) ?
                // PS: txn version in index does not matter, because if update is successful,
                // then txn would no longer be open.
                streamMetadataStore.addTxnToIndex(hostId, resource, Integer.MAX_VALUE) :
                CompletableFuture.completedFuture(null);

        addIndex.whenComplete((v, e) -> {
            if (e != null) {
                log.debug("Txn={}, already present/newly added to host-txn index of host={}", txnId, hostId);
            } else {
                log.debug("Txn={}, added txn to host-txn index of host={}", txnId, hostId);
            }
        });

        // Step 2. Seal txn
        CompletableFuture<AbstractMap.SimpleEntry<TxnStatus, Integer>> sealFuture = addIndex.thenComposeAsync(x ->
                streamMetadataStore.sealTransaction(scope, stream, txnId, commit, versionOpt, ctx, executor), executor)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.debug("Txn={}, failed sealing txn", txnId);
                    } else {
                        log.debug("Txn={}, sealed successfully, commit={}", txnId, commit);
                    }
                });

        // Step 3. write event to corresponding stream.
        return sealFuture.thenComposeAsync(pair -> {
            TxnStatus status = pair.getKey();
            switch (status) {
                case COMMITTING:
                    return writeCommitEvent(scope, stream, pair.getValue(), txnId, status);
                case ABORTING:
                    return writeAbortEvent(scope, stream, pair.getValue(), txnId, status);
                case ABORTED:
                case COMMITTED:
                    return CompletableFuture.completedFuture(status);
                case OPEN:
                case UNKNOWN:
                default:
                    // Not possible after successful streamStore.sealTransaction call, because otherwise an
                    // exception would be thrown.
                    return CompletableFuture.completedFuture(status);
            }
        }, executor).thenComposeAsync(status -> {
            // Step 4. Remove txn from timeoutService, and from the index.
            timeoutService.removeTxn(scope, stream, txnId);
            log.debug("Txn={}, removed from timeout service", txnId);
            return streamMetadataStore.removeTxnFromIndex(host, resource, true).whenComplete((v, e) -> {
                if (e != null) {
                    log.debug("Txn={}, failed removing txn from host-txn index of host={}", txnId, hostId);
                } else {
                    log.debug("Txn={}, removed txn from host-txn index of host={}", txnId, hostId);
                }
            }).thenApply(x -> status);
        }, executor);
    }

    public CompletableFuture<Void> writeCommitEvent(CommitEvent event) {
        return TaskStepsRetryHelper.withRetries(() -> commitEventEventStreamWriter.writeEvent(event.getKey(), event), executor);
    }

    CompletableFuture<TxnStatus> writeCommitEvent(String scope, String stream, int epoch, UUID txnId, TxnStatus status) {
        String key = scope + stream;
        CommitEvent event = new CommitEvent(scope, stream, epoch);
        return TaskStepsRetryHelper.withRetries(() -> writeEvent(commitEventEventStreamWriter, commitStreamName,
                key, event, txnId, status), executor);
    }

    CompletableFuture<TxnStatus> writeAbortEvent(String scope, String stream, int epoch, UUID txnId, TxnStatus status) {
        String key = txnId.toString();
        AbortEvent event = new AbortEvent(scope, stream, epoch, txnId);
        return TaskStepsRetryHelper.withRetries(() -> writeEvent(abortEventEventStreamWriter, abortStreamName,
                key, event, txnId, status), executor);
    }

    private <T> CompletableFuture<TxnStatus> writeEvent(final EventStreamWriter<T> streamWriter,
                                                        final String streamName,
                                                        final String key,
                                                        final T event,
                                                        final UUID txnId,
                                                        final TxnStatus txnStatus) {
        log.debug("Txn={}, state={}, sending request to {}", txnId, txnStatus, streamName);
        return streamWriter.writeEvent(key, event)
                .thenApplyAsync(v -> {
                    log.debug("Transaction {}, sent request to {}", txnId, streamName);
                    return txnStatus;
                }, executor)
                .exceptionally(ex -> {
                    log.debug("Transaction {}, failed sending {} to {}. Retrying...", txnId, event.getClass()
                            .getSimpleName(), streamName);
                    throw new WriteFailedException(ex);
                });
    }

    private CompletableFuture<Void> notifyTxnCreation(final String scope, final String stream,
                                                      final List<Segment> segments, final UUID txnId) {
        return Futures.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyTxnCreation(scope, stream, segment.segmentId(), txnId))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<UUID> notifyTxnCreation(final String scope, final String stream,
                                                      final long segmentId, final UUID txnId) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.createTransaction(scope,
                stream,
                segmentId,
                txnId,
                this.hostControllerStore,
                this.connectionFactory, this.retrieveDelegationToken()), executor);
    }

    private CompletableFuture<Void> checkReady() {
        if (!ready) {
            return Futures.failedFuture(new IllegalStateException(getClass().getName() + " not yet ready"));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private OperationContext getNonNullOperationContext(final String scope,
                                                        final String stream,
                                                        final OperationContext contextOpt) {
        return contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
    }

    public String retrieveDelegationToken() {
        if (authorizationEnabled) {
            return PravegaInterceptor.retrieveDelegationToken(tokenSigningKey);
        } else {
            return "";
        }
    }

    @Override
    public void close() throws Exception {
        timeoutService.stopAsync();
        timeoutService.awaitTerminated();
        if (commitEventEventStreamWriter != null) {
            commitEventEventStreamWriter.close();
        }
        if (abortEventEventStreamWriter != null) {
            abortEventEventStreamWriter.close();
        }
    }
}
