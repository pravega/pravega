/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.task.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus.Status;
import io.pravega.controller.timeout.TimeoutService;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.timeout.TimerWheelTimeoutService;
import io.pravega.controller.util.Config;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Synchronized;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import static io.pravega.controller.util.RetryHelper.RETRYABLE_PREDICATE;
import static io.pravega.controller.util.RetryHelper.withRetriesAsync;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
public class StreamTransactionMetadataTasks implements AutoCloseable {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(StreamTransactionMetadataTasks.class));

    /**
     * We derive the maximum execution timeout from the lease time. We assume
     * a maximum number of renewals for the txn and compute the maximum execution
     * time by multiplying the lease timeout by the maximum number of renewals. This
     * multiplier is currently hardcoded because we do not expect applications to change
     * it. The maximum execution timeout is only a safety mechanism for application that
     * should rarely be triggered.
     */
    private static final int MAX_EXECUTION_TIME_MULTIPLIER = 1000;

    protected final String hostId;
    protected final ScheduledExecutorService executor;
    protected final ScheduledExecutorService eventExecutor;

    private final StreamMetadataStore streamMetadataStore;
    private final SegmentHelper segmentHelper;
    private final GrpcAuthHelper authHelper;
    @Getter
    @VisibleForTesting
    private final TimeoutService timeoutService;

    private volatile boolean ready;
    private final CountDownLatch readyLatch;
    private final CompletableFuture<EventStreamWriter<CommitEvent>> commitWriterFuture;
    private final CompletableFuture<EventStreamWriter<AbortEvent>> abortWriterFuture;
    private final AtomicLong maxTransactionExecutionTimeBound;

    @VisibleForTesting
    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final ScheduledExecutorService eventExecutor,
                                          final String hostId,
                                          final TimeoutServiceConfig timeoutServiceConfig,
                                          final BlockingQueue<Optional<Throwable>> taskCompletionQueue,
                                          final GrpcAuthHelper authHelper) {
        this.hostId = hostId;
        this.executor = executor;
        this.eventExecutor = eventExecutor;
        this.streamMetadataStore = streamMetadataStore;
        this.segmentHelper = segmentHelper;
        this.authHelper = authHelper;
        this.timeoutService = new TimerWheelTimeoutService(this, timeoutServiceConfig, taskCompletionQueue);
        readyLatch = new CountDownLatch(1);
        this.commitWriterFuture = new CompletableFuture<>();
        this.abortWriterFuture = new CompletableFuture<>();
        this.maxTransactionExecutionTimeBound = new AtomicLong(Duration.ofDays(Config.MAX_TXN_EXECUTION_TIMEBOUND_DAYS).toMillis());
    }

    @VisibleForTesting
    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final String hostId,
                                          final TimeoutServiceConfig timeoutServiceConfig,
                                          final BlockingQueue<Optional<Throwable>> taskCompletionQueue,
                                          final GrpcAuthHelper authHelper) {
        this(streamMetadataStore, segmentHelper, executor, executor, hostId, timeoutServiceConfig, taskCompletionQueue, authHelper);
    }

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final ScheduledExecutorService eventExecutor,
                                          final String hostId,
                                          final TimeoutServiceConfig timeoutServiceConfig,
                                          final GrpcAuthHelper authHelper) {
        this(streamMetadataStore, segmentHelper, executor, eventExecutor, hostId, timeoutServiceConfig, 
                null, authHelper);
    }

    @VisibleForTesting
    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final String hostId,
                                          final GrpcAuthHelper authHelper) {
        this(streamMetadataStore, segmentHelper, executor, executor, hostId, TimeoutServiceConfig.defaultConfig(), authHelper);
    }

    private void setReady() {
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

    void awaitInitialization() throws InterruptedException {
        readyLatch.await();
    }

    /**
     * Initializes stream writers for commit and abort streams.
     * This method should be called immediately after creating StreamTransactionMetadataTasks object.
     *
     * @param clientFactory Client factory reference.
     * @param config Controller event processor configuration.
     */
    @Synchronized
    public void initializeStreamWriters(final EventStreamClientFactory clientFactory,
                                        final ControllerEventProcessorConfig config) {
        if (!commitWriterFuture.isDone()) {
            commitWriterFuture.complete(clientFactory.createEventWriter(
                    config.getCommitStreamName(),
                    ControllerEventProcessors.COMMIT_EVENT_SERIALIZER,
                    EventWriterConfig.builder().enableConnectionPooling(true).retryAttempts(Integer.MAX_VALUE).build()));
        }
        if (!abortWriterFuture.isDone()) {
            abortWriterFuture.complete(clientFactory.createEventWriter(
                    config.getAbortStreamName(),
                    ControllerEventProcessors.ABORT_EVENT_SERIALIZER,
                    EventWriterConfig.builder().enableConnectionPooling(true).retryAttempts(Integer.MAX_VALUE).build()));
        }
        this.setReady();
    }

    @VisibleForTesting
    @Synchronized
    public void initializeStreamWriters(final EventStreamWriter<CommitEvent> commitWriter,
                                        final EventStreamWriter<AbortEvent> abortWriter) {
        this.commitWriterFuture.complete(commitWriter);
        this.abortWriterFuture.complete(abortWriter);
        this.setReady();
    }

    /**
     * Create transaction.
     *
     * @param scope              stream scope.
     * @param stream             stream name.
     * @param lease              Time for which transaction shall remain open with sending any heartbeat.
     *                           the scaling operation is initiated on the txn stream.
     * @param requestId          request id.
     * @param rolloverSizeBytes  rollover size for txn segment.
     * @return transaction id.
     */
    public CompletableFuture<Pair<VersionedTransactionData, List<StreamSegmentRecord>>> createTxn(final String scope,
                                                                                      final String stream,
                                                                                      final long lease,
                                                                                      final long requestId,
                                                                                      final long rolloverSizeBytes) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        return createTxnBody(scope, stream, lease, context, rolloverSizeBytes);
    }

    /**
     * Transaction heartbeat, that increases transaction timeout by lease number of milliseconds.
     *
     * @param scope Stream scope.
     * @param stream Stream name.
     * @param txId Transaction identifier.
     * @param lease Amount of time in milliseconds by which to extend the transaction lease.
     * @param requestId requestId
     * @return Transaction metadata along with the version of it record in the store.
     */
    public CompletableFuture<PingTxnStatus> pingTxn(final String scope,
                                                    final String stream,
                                                    final UUID txId,
                                                    final long lease,
                                                    final long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        return pingTxnBody(scope, stream, txId, lease, context);
    }

    /**
     * Abort transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @param version Expected version of the transaction record in the store.
     * @param requestId request id.
     * @return true/false.
     */
    public CompletableFuture<TxnStatus> abortTxn(final String scope,
                                                 final String stream,
                                                 final UUID txId,
                                                 final Version version,
                                                 final long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        return abortTxn(scope, stream, txId, version, context);
    }

    /**
     * Abort transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @param version Expected version of the transaction record in the store.
     * @param context operation context.
     * @return true/false.
     */
    public CompletableFuture<TxnStatus> abortTxn(final String scope,
                                                 final String stream,
                                                 final UUID txId,
                                                 final Version version,
                                                 final OperationContext context) {
        return withRetriesAsync(() -> sealTxnBody(hostId, scope, stream, false, txId, version, context),
                RETRYABLE_PREDICATE, 3, executor);
    }

    /**
     * Commit transaction.
     *
     * @param scope      stream scope.
     * @param stream     stream name.
     * @param txId       transaction id.
     * @param requestId  request id.
     * @return true/false.
     */
    public CompletableFuture<TxnStatus> commitTxn(final String scope, final String stream, final UUID txId,
                                                  final long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        return withRetriesAsync(() -> sealTxnBody(hostId, scope, stream, true, txId, 
                null, "", Long.MIN_VALUE, context),
                RETRYABLE_PREDICATE, 3, executor);
    }

    /**
     * Commit transaction.
     *
     * @param scope      stream scope.
     * @param stream     stream name.
     * @param txId       transaction id.
     * @param writerId   writer id
     * @param timestamp  commit time as recorded by writer. This is required for watermarking.
     * @param requestId  requestid
     * @return true/false.
     */
    public CompletableFuture<TxnStatus> commitTxn(final String scope, final String stream, final UUID txId,
                                                  final String writerId, final long timestamp,
                                                  final long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        return withRetriesAsync(() -> sealTxnBody(hostId, scope, stream, true, txId, null, 
                writerId, timestamp, context),
                RETRYABLE_PREDICATE, 3, executor);
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
     * @param rolloverSizeBytes   rollover size for txn segment.
     * @return                    identifier of the created txn.
     */
    CompletableFuture<Pair<VersionedTransactionData, List<StreamSegmentRecord>>> createTxnBody(final String scope,
                                                                                   final String stream,
                                                                                   final long lease,
                                                                                   final OperationContext ctx,
                                                                                   final long rolloverSizeBytes) {
        Preconditions.checkNotNull(ctx, "Operation context is null");
        // Step 1. Validate parameters.
        CompletableFuture<Void> validate = validate(lease);
        long maxExecutionPeriod = Math.min(MAX_EXECUTION_TIME_MULTIPLIER * lease, maxTransactionExecutionTimeBound.get());

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
                    CompletableFuture<Void> addIndex = addTxnToIndex(scope, stream, txnId, ctx.getRequestId());

                    // Step 3. Create txn node in the store.
                    CompletableFuture<VersionedTransactionData> txnFuture = createTxnInStore(scope, stream, lease,
                            ctx, maxExecutionPeriod, txnId, addIndex, ctx.getRequestId());

                    // Step 4. Notify segment stores about new txn.
                    CompletableFuture<List<StreamSegmentRecord>> segmentsFuture = txnFuture.thenComposeAsync(txnData ->
                            streamMetadataStore.getSegmentsInEpoch(scope, stream, txnData.getEpoch(), ctx, executor), executor);

                    CompletableFuture<Void> notify = segmentsFuture.thenComposeAsync(activeSegments ->
                            notifyTxnCreation(scope, stream, activeSegments, txnId, ctx.getRequestId(), rolloverSizeBytes), executor)
                                                                   .whenComplete((v, e) ->
                            // Method notifyTxnCreation ensures that notification completes
                            // even in the presence of n/w or segment store failures.
                            log.trace(ctx.getRequestId(), "Txn={}, notified segments stores", txnId));

                    // Step 5. Start tracking txn in timeout service
                    return notify.whenCompleteAsync((result, ex) -> {
                        addTxnToTimeoutService(scope, stream, lease, maxExecutionPeriod, txnId, txnFuture, ctx.getRequestId());
                    }, executor).thenApplyAsync(v -> {
                        List<StreamSegmentRecord> segments = segmentsFuture.join().stream().map(x -> {
                            long generalizedSegmentId = RecordHelper.generalizedSegmentId(x.segmentId(), txnId);
                            int epoch = NameUtils.getEpoch(generalizedSegmentId);
                            int segmentNumber = NameUtils.getSegmentNumber(generalizedSegmentId);
                            return StreamSegmentRecord.builder().creationEpoch(epoch).segmentNumber(segmentNumber)
                                    .creationTime(x.getCreationTime()).keyStart(x.getKeyStart()).keyEnd(x.getKeyEnd()).build();
                        }).collect(Collectors.toList());

                        return new ImmutablePair<>(txnFuture.join(), segments);
                    }, executor);
                }), e -> {
            Throwable unwrap = Exceptions.unwrap(e);
            return unwrap instanceof StoreException.WriteConflictException || unwrap instanceof StoreException.DataNotFoundException;
        }, 5, executor));
    }

    private void addTxnToTimeoutService(String scope, String stream, long lease, long maxExecutionPeriod, UUID txnId,
                                        CompletableFuture<VersionedTransactionData> txnFuture, long requestId) {
        Version version = null;
        long executionExpiryTime = System.currentTimeMillis() + maxExecutionPeriod;
        if (!txnFuture.isCompletedExceptionally()) {
            version = txnFuture.join().getVersion();
            executionExpiryTime = txnFuture.join().getMaxExecutionExpiryTime();
        }
        timeoutService.addTxn(scope, stream, txnId, version, lease, executionExpiryTime);
        log.trace(requestId, "Txn={}, added to timeout service on host={}", txnId, hostId);
    }

    private CompletableFuture<VersionedTransactionData> createTxnInStore(String scope, String stream, long lease,
                                                                         OperationContext ctx, long maxExecutionPeriod, UUID txnId,
                                                                         CompletableFuture<Void> addIndex, long requestId) {
        return addIndex.thenComposeAsync(ignore ->
                                streamMetadataStore.createTransaction(scope, stream, txnId, lease, maxExecutionPeriod,
                                        ctx, executor), executor).whenComplete((v, e) -> {
                            if (e != null) {
                                log.debug(requestId, "Txn={}, failed creating txn in store", txnId);
                            } else {
                                log.debug(requestId, "Txn={}, created in store", txnId);
                            }
                        });
    }

    private CompletableFuture<Void> addTxnToIndex(String scope, String stream, UUID txnId, long requestId) {
        TxnResource resource = new TxnResource(scope, stream, txnId);
        // Step 2. Add txn to host-transaction index.
        return streamMetadataStore.addTxnToIndex(hostId, resource, null)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.debug(requestId, "Txn={}, failed adding txn to host-txn index of host={}", txnId, hostId);
                    } else {
                        log.debug(requestId, "Txn={}, added txn to host-txn index of host={}", txnId, hostId);
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
    private CompletableFuture<PingTxnStatus> pingTxnBody(final String scope,
                                                 final String stream,
                                                 final UUID txnId,
                                                 final long lease,
                                                 final OperationContext ctx) {
        Preconditions.checkNotNull(ctx, "operation context is null");
        if (!timeoutService.isRunning()) {
            return CompletableFuture.completedFuture(createStatus(Status.DISCONNECTED));
        }

        log.debug(ctx.getRequestId(), "Txn={}, updating txn node in store and extending lease", txnId);
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
        CompletableFuture<PingTxnStatus> pingTxnFuture = streamMetadataStore.getTransactionData(scope, stream, txnId, ctx, executor).thenComposeAsync(txnData -> {
            final TxnStatus txnStatus = txnData.getStatus();
            if (!txnStatus.equals(TxnStatus.OPEN)) { // transaction is not open, dont ping it
                return CompletableFuture.completedFuture(getPingTxnStatus(txnStatus));
            }
            // Step 1. Sanity check for lease value.
            if (lease > timeoutService.getMaxLeaseValue()) {
                return CompletableFuture.completedFuture(createStatus(Status.LEASE_TOO_LARGE));
            } else if (lease + System.currentTimeMillis() > txnData.getMaxExecutionExpiryTime()) {
                return CompletableFuture.completedFuture(createStatus(Status.MAX_EXECUTION_TIME_EXCEEDED));
            } else {
                TxnResource resource = new TxnResource(scope, stream, txnId);

                // Step 2. Add txn to host-transaction index
                CompletableFuture<Void> addIndex = !timeoutService.containsTxn(scope, stream, txnId) ?
                        streamMetadataStore.addTxnToIndex(hostId, resource, txnData.getVersion()) :
                        CompletableFuture.completedFuture(null);

                addIndex.whenComplete((v, e) -> {
                    if (e != null) {
                        log.debug(ctx.getRequestId(), "Txn={}, failed adding txn to host-txn index of host={}", txnId, hostId);
                    } else {
                        log.debug(ctx.getRequestId(), "Txn={}, added txn to host-txn index of host={}", txnId, hostId);
                    }
                });

                return addIndex.thenComposeAsync(x -> {
                    // Step 3. Update txn node data in the store.
                    long requestId = ctx.getRequestId();
                    CompletableFuture<VersionedTransactionData> pingTxn = streamMetadataStore.pingTransaction(
                            scope, stream, txnData, lease, ctx, executor).whenComplete((v, e) -> {
                        if (e != null) {
                            log.debug(requestId, "Txn={}, failed updating txn node in store", txnId);
                        } else {
                            log.debug(requestId, "Txn={}, updated txn node in store", txnId);
                        }
                    });

                    // Step 4. Add it to timeout service and start managing timeout for this txn.
                    return pingTxn.thenApplyAsync(data -> {
                        Version version = data.getVersion();
                        long expiryTime = data.getMaxExecutionExpiryTime();
                        // Even if timeout service has an active/executing timeout task for this txn, it is bound
                        // to fail, since version of txn node has changed because of the above store.pingTxn call.
                        // Hence explicitly add a new timeout task.
                        if (timeoutService.containsTxn(scope, stream, txnId)) {
                            // If timeout service knows about this transaction, attempt to increase its lease.
                            log.debug(requestId, "Txn={}, extending lease in timeout service", txnId);
                            timeoutService.pingTxn(scope, stream, txnId, version, lease);
                        } else {
                            log.debug(requestId, "Txn={}, adding in timeout service", txnId);
                            timeoutService.addTxn(scope, stream, txnId, version, lease, expiryTime);
                        }
                        return createStatus(Status.OK);
                    }, executor);
                }, executor);
            }
        }, executor);
        return Futures.exceptionallyComposeExpecting(pingTxnFuture,
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException,
                () -> streamMetadataStore.transactionStatus(scope, stream, txnId, ctx, executor)
                                   .thenApply(this::getPingTxnStatus)
                );
    }

    private PingTxnStatus getPingTxnStatus(final TxnStatus txnStatus) {
        final PingTxnStatus status;
        if (txnStatus.equals(TxnStatus.COMMITTED) || txnStatus.equals(TxnStatus.COMMITTING)) {
            status = createStatus(PingTxnStatus.Status.COMMITTED);
        } else if (txnStatus.equals(TxnStatus.ABORTED) || txnStatus.equals(TxnStatus.ABORTING)) {
            status = createStatus(PingTxnStatus.Status.ABORTED);
        } else {
            status = createStatus(PingTxnStatus.Status.UNKNOWN);
        }
        return status;
    }
    
    CompletableFuture<TxnStatus> sealTxnBody(final String host,
                                             final String scope,
                                             final String stream,
                                             final boolean commit,
                                             final UUID txnId,
                                             final Version version,
                                             final OperationContext ctx) {
        return sealTxnBody(host, scope, stream, commit, txnId, version, "", Long.MIN_VALUE, ctx);
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
                                             final Version version,
                                             final String writerId,
                                             final long timestamp,
                                             final OperationContext ctx) {
        Preconditions.checkNotNull(ctx, "Operation context cannot be null");
        long requestId = ctx.getRequestId();
        TxnResource resource = new TxnResource(scope, stream, txnId);
        Optional<Version> versionOpt = Optional.ofNullable(version);

        // Step 1. Add txn to current host's index, if it is not already present
        CompletableFuture<Void> addIndex = host.equals(hostId) && !timeoutService.containsTxn(scope, stream, txnId) ?
                // PS: txn version in index does not matter, because if update is successful,
                // then txn would no longer be open.
                streamMetadataStore.addTxnToIndex(hostId, resource, version) :
                CompletableFuture.completedFuture(null);

        addIndex.whenComplete((v, e) -> {
            if (e != null) {
                log.debug(requestId, "Txn={}, already present/newly added to host-txn index of host={}", txnId, hostId);
            } else {
                log.debug(requestId, "Txn={}, added txn to host-txn index of host={}", txnId, hostId);
            }
        });

        // Step 2. Seal txn
        CompletableFuture<AbstractMap.SimpleEntry<TxnStatus, Integer>> sealFuture = addIndex.thenComposeAsync(x ->
                streamMetadataStore.sealTransaction(scope, stream, txnId, commit, versionOpt, writerId, timestamp, ctx, executor), executor)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        log.debug(requestId, "Txn={}, failed sealing txn", txnId);
                    } else {
                        log.debug(requestId, "Txn={}, sealed successfully, commit={}", txnId, commit);
                    }
                });

        // Step 3. write event to corresponding stream.
        return sealFuture.thenComposeAsync(pair -> {
            TxnStatus status = pair.getKey();
            switch (status) {
                case COMMITTING:
                    return writeCommitEvent(scope, stream, pair.getValue(), txnId, status, requestId);
                case ABORTING:
                    return writeAbortEvent(scope, stream, pair.getValue(), txnId, status, requestId);
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
            log.debug(requestId, "Txn={}, removed from timeout service", txnId);
            return streamMetadataStore.removeTxnFromIndex(host, resource, true).whenComplete((v, e) -> {
                if (e != null) {
                    log.debug(requestId, "Txn={}, failed removing txn from host-txn index of host={}", txnId, hostId);
                } else {
                    log.debug(requestId, "Txn={}, removed txn from host-txn index of host={}", txnId, hostId);
                }
            }).thenApply(x -> status);
        }, executor);
    }

    public CompletableFuture<Void> writeCommitEvent(CommitEvent event) {
        return commitWriterFuture
                .thenComposeAsync(commitWriter -> commitWriter.writeEvent(event.getKey(), event), eventExecutor);
    }

    CompletableFuture<TxnStatus> writeCommitEvent(String scope, String stream, int epoch, UUID txnId, TxnStatus status, 
                                                  long requestId) {
        CommitEvent event = new CommitEvent(scope, stream, epoch);
        return TaskStepsRetryHelper.withRetries(() -> writeCommitEvent(event)
                .handle((r, e) -> {
                    if (e != null) {
                        log.debug(requestId, "Transaction {}, failed posting commit event. Retrying...", txnId);
                        throw new WriteFailedException(e);
                    } else {
                        log.debug(requestId, "Transaction {} commit event posted", txnId);
                        return status;
                    }
                }), executor);
    }

    public CompletableFuture<Void> writeAbortEvent(AbortEvent event) {
        return abortWriterFuture
                .thenComposeAsync(abortWriter -> abortWriter.writeEvent(event.getKey(), event), eventExecutor);
    }

    CompletableFuture<TxnStatus> writeAbortEvent(String scope, String stream, int epoch, UUID txnId, TxnStatus status, long requestId) {
        AbortEvent event = new AbortEvent(scope, stream, epoch, txnId, requestId);
        return TaskStepsRetryHelper.withRetries(() -> writeAbortEvent(event)
                .handle((r, e) -> {
                    if (e != null) {
                        log.debug(requestId, "Transaction {}, failed posting abort event. Retrying...", txnId);
                        throw new WriteFailedException(e);
                    } else {
                        log.debug(requestId, "Transaction {} abort event posted", txnId);
                        return status;
                    }
                }), executor);
    }

    private CompletableFuture<Void> notifyTxnCreation(final String scope, final String stream,
                                                      final List<StreamSegmentRecord> segments, final UUID txnId, 
                                                      final long requestId, final long rolloverSizeBytes) {
        Timer timer = new Timer();
        return Futures.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyTxnCreation(scope, stream, segment.segmentId(), txnId, requestId, rolloverSizeBytes))
                .collect(Collectors.toList()))
                .thenRun(() -> TransactionMetrics.getInstance().createTransactionSegments(timer.getElapsed()));
    }

    private CompletableFuture<Void> notifyTxnCreation(final String scope, final String stream,
                                                      final long segmentId, final UUID txnId, final long requestId,
                                                      final long rolloverSizeBytes) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.createTransaction(scope,
                                                                                      stream,
                                                                                      segmentId,
                                                                                      txnId,
                                                                                      this.retrieveDelegationToken(),
                                                                                      requestId,
                                                                                      rolloverSizeBytes), executor);
    }

    public String retrieveDelegationToken() {
        return authHelper.retrieveMasterToken();
    }

    @Override
    @Synchronized
    public void close() throws Exception {
        timeoutService.stopAsync();
        timeoutService.awaitTerminated();
        CompletableFuture<Void> commitCloseFuture = commitWriterFuture.thenAccept(EventStreamWriter::close);
        CompletableFuture<Void> abortCloseFuture = abortWriterFuture.thenAccept(EventStreamWriter::close);

        // since we do the checks under synchronized block, we know the promise for writers cannot be completed 
        // by anyone if we have the lock and we can simply cancel the futures.  
        if (commitWriterFuture.isDone()) {
            commitCloseFuture.join();
        } else {
            commitWriterFuture.cancel(true);
        }
        if (abortWriterFuture.isDone()) {
            abortCloseFuture.join();
        } else {
            abortWriterFuture.cancel(true);
        }
    }
    
    @VisibleForTesting
    void setMaxExecutionTime(long timeInMillis) {
        maxTransactionExecutionTimeBound.set(timeInMillis);
    }
}
