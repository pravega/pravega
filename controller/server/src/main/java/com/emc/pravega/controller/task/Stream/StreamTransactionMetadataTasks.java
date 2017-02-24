/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.server.eventProcessor.AbortEvent;
import com.emc.pravega.controller.server.eventProcessor.CommitEvent;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.OperationContext;
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
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.emc.pravega.controller.task.Stream.TaskStepsRetryHelper.withRetries;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamTransactionMetadataTasks extends TaskBase {

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactoryImpl connectionFactory;
    private final SegmentHelper segmentHelper;

    private EventStreamWriter<CommitEvent> commitEventEventStreamWriter;
    private EventStreamWriter<AbortEvent> abortEventEventStreamWriter;

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final TaskMetadataStore taskMetadataStore,
                                          SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                                          final String hostId) {
        this(streamMetadataStore, hostControllerStore, taskMetadataStore, segmentHelper, executor, new Context(hostId));
    }

    private StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                           final HostControllerStore hostControllerStore,
                                           final TaskMetadataStore taskMetadataStore,
                                           SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                                           final Context context) {
        super(taskMetadataStore, executor, context);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.connectionFactory = new ConnectionFactoryImpl(false);
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
     * @param scope      stream scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @return transaction id.
     */
    @Task(name = "createTransaction", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<UUID> createTx(final String scope, final String stream, final OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, null},
                () -> createTxBody(scope, stream, context));
    }

    /**
     * Abort transaction.
     *
     * @param scope      stream scope.
     * @param stream     stream name.
     * @param txId       transaction id.
     * @param contextOpt optional context
     * @return true/false.
     */
    @Task(name = "abortTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<TxnStatus> abortTx(final String scope, final String stream, final UUID txId, final OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId, null},
                () -> abortTxBody(scope, stream, txId, context));
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
    @Task(name = "commitTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<TxnStatus> commitTx(final String scope, final String stream, final UUID txId, final OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId, null},
                () -> commitTxBody(scope, stream, txId, context));
    }

    private CompletableFuture<UUID> createTxBody(final String scope, final String stream, final OperationContext context) {
        return streamMetadataStore.createTransaction(scope, stream, context, executor)
                .thenCompose(txId ->
                        withRetries(() -> streamMetadataStore.getActiveSegments(scope, stream, context, executor), executor)
                                .thenCompose(activeSegments ->
                                        FutureHelpers.allOf(
                                                activeSegments.stream()
                                                        .parallel()
                                                        .map(segment -> notifyTxCreation(scope, stream, segment.getNumber(), txId))
                                                        .collect(Collectors.toList())))
                                .thenApply(y -> txId));
    }

    private CompletableFuture<TxnStatus> abortTxBody(final String scope, final String stream, final UUID txid, final OperationContext context) {
        return streamMetadataStore.sealTransaction(scope, stream, txid, false, context, executor)
                .thenApply(status -> {
                    this.abortEventEventStreamWriter
                            .writeEvent(txid.toString(), new AbortEvent(scope, stream, txid));
                    return status;
                });
    }

    private CompletableFuture<TxnStatus> commitTxBody(final String scope, final String stream, final UUID txid, final OperationContext context) {
        return withRetries(() -> streamMetadataStore.sealTransaction(scope, stream, txid, true, context, executor)
                .thenApply(status -> {
                    // Todo: this returns an ack future that we dont wait for. How do we know this was complete?
                    // And the problem is its Future and not completable future. So we cant chain to it here.
                    this.commitEventEventStreamWriter
                            .writeEvent(scope + stream, new CommitEvent(scope, stream, txid));
                    return status;
                }), executor);
    }


    private CompletableFuture<UUID> notifyTxCreation(final String scope, final String stream, final int segmentNumber, final UUID txid) {
        return withRetries(() -> segmentHelper.createTransaction(scope,
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
                segmentHelper, executor,
                context);
    }
}
