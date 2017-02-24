/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.server.eventProcessor.AbortEvent;
import com.emc.pravega.controller.server.eventProcessor.CommitEvent;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.task.Resource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.TxnStatus;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
public class StreamTransactionMetadataTasks extends TaskBase {
    private static final long INITIAL_DELAY = 1;
    private static final long PERIOD = 1;
    private static final long TIMEOUT = 60 * 60 * 1000;
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 10;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = 100000;

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactoryImpl connectionFactory;

    private EventStreamWriter<CommitEvent> commitEventEventStreamWriter;
    private EventStreamWriter<AbortEvent> abortEventEventStreamWriter;

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final TaskMetadataStore taskMetadataStore,
                                          final ScheduledExecutorService executor,
                                          final String hostId) {
        this(streamMetadataStore, hostControllerStore, taskMetadataStore, executor, new Context(hostId));
    }

    private StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
            final HostControllerStore hostControllerStore,
            final TaskMetadataStore taskMetadataStore,
            final ScheduledExecutorService executor,
            final Context context) {
        super(taskMetadataStore, executor, context);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.connectionFactory = new ConnectionFactoryImpl(false);

        // abort timedout transactions periodically
        executor.scheduleAtFixedRate(() -> {
            // find transactions to be aborted
            try {
                final long currentTime = System.currentTimeMillis();
                streamMetadataStore.getAllActiveTx().get().stream()
                        .forEach(x -> {
                            if (currentTime - x.getTxRecord().getTxCreationTimestamp() > TIMEOUT) {
                                try {
                                    abortTx(x.getScope(), x.getStream(), x.getTxid());
                                } catch (Exception e) {
                                    // TODO: log and ignore
                                }
                            }
                        });
            } catch (Exception e) {
                // TODO: log! and ignore
            }
            // find completed transactions to be gc'd
        }, INITIAL_DELAY, PERIOD, TimeUnit.HOURS);

    }

    /**
     * Initializes stream writers for commit and abort streams.
     * This method should be called immediately after creating StreamTransactionMetadataTasks object.
     *
     * @param controller Local controller reference
     */
    public void initializeStreamWriters(Controller controller) {

        ClientFactory clientFactory = new ClientFactoryImpl(ControllerEventProcessors.CONTROLLER_SCOPE, controller);

        this.commitEventEventStreamWriter = clientFactory.createEventWriter(
                ControllerEventProcessors.COMMIT_STREAM,
                ControllerEventProcessors.COMMIT_EVENT_SERIALIZER,
                EventWriterConfig.builder().build());

        this.abortEventEventStreamWriter = clientFactory.createEventWriter(
                ControllerEventProcessors.ABORT_STREAM,
                ControllerEventProcessors.ABORT_EVENT_SERIALIZER,
                EventWriterConfig.builder().build());
    }

    /**
     * Create transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @return transaction id.
     */
    @Task(name = "createTransaction", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<UUID> createTx(final String scope, final String stream) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream},
                () -> createTxBody(scope, stream));
    }

    /**
     * Abort transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @return true/false.
     */
    @Task(name = "abortTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<TxnStatus> abortTx(final String scope, final String stream, final UUID txId) {
        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId},
                () -> abortTxBody(scope, stream, txId));
    }

    /**
     * Commit transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @return true/false.
     */
    @Task(name = "commitTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<TxnStatus> commitTx(final String scope, final String stream, final UUID txId) {
        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId},
                () -> commitTxBody(scope, stream, txId));
    }

    private CompletableFuture<UUID> createTxBody(final String scope, final String stream) {
        return streamMetadataStore.createTransaction(scope, stream)
                .thenCompose(txId ->
                        streamMetadataStore.getActiveSegments(scope, stream)
                                .thenCompose(activeSegments ->
                                        FutureHelpers.allOf(
                                                activeSegments.stream()
                                                        .parallel()
                                                        .map(segment -> notifyTxCreation(scope, stream, segment.getNumber(), txId))
                                                        .collect(Collectors.toList())))
                                .thenApply(x -> txId));
    }

    private CompletableFuture<TxnStatus> abortTxBody(final String scope, final String stream, final UUID txid) {
        return streamMetadataStore.sealTransaction(scope, stream, txid, false)
                .thenApply(status -> {
                    this.abortEventEventStreamWriter
                            .writeEvent(txid.toString(), new AbortEvent(scope, stream, txid));
                    return status;
                });
    }

    private CompletableFuture<TxnStatus> commitTxBody(final String scope, final String stream, final UUID txid) {
        return streamMetadataStore.sealTransaction(scope, stream, txid, true)
                .thenApply(status -> {
                    this.commitEventEventStreamWriter
                            .writeEvent(scope + stream, new CommitEvent(scope, stream, txid));
                    return status;
                });
    }

    private CompletableFuture<UUID> notifyTxCreation(final String scope, final String stream, final int segmentNumber, final UUID txid) {
        return Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
                .retryingOn(WireCommandFailedException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> SegmentHelper.createTransaction(scope,
                        stream,
                        segmentNumber,
                        txid,
                        this.hostControllerStore,
                        this.connectionFactory), executor);
    }

    @Override
    public TaskBase copyWithContext(Context context) {
        return new StreamTransactionMetadataTasks(streamMetadataStore,
                                                  hostControllerStore,
                                                  taskMetadataStore,
                                                  executor,
                                                  context);
    }
}
