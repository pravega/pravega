/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.common.concurrent.FutureCollectionHelper;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.OperationContext;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.task.Resource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.impl.TxnStatus;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamTransactionMetadataTasks extends TaskBase implements Cloneable {
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 10;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = 100000;

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactoryImpl connectionFactory;

    private final TxTimeOutScheduler txTimeOutScheduler;

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final TaskMetadataStore taskMetadataStore,
                                          final ScheduledExecutorService executor,
                                          final String hostId,
                                          final TxTimeOutScheduler txTimeOutScheduler) {
        super(taskMetadataStore, executor, hostId);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.txTimeOutScheduler = txTimeOutScheduler;
        this.connectionFactory = new ConnectionFactoryImpl(false);
    }

    @Override
    public StreamTransactionMetadataTasks clone() throws CloneNotSupportedException {
        return (StreamTransactionMetadataTasks) super.clone();
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
    public CompletableFuture<UUID> createTx(final String scope, final String stream, final Optional<OperationContext> contextOpt) {
        final OperationContext context = contextOpt.orElse(streamMetadataStore.createContext(scope, stream));

        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream},
                () -> createTxBody(scope, stream, context));
    }

    /**
     * Drop transaction.
     *
     * @param scope      stream scope.
     * @param stream     stream name.
     * @param txId       transaction id.
     * @param contextOpt optional context
     * @return true/false.
     */
    @Task(name = "dropTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<TxnStatus> dropTx(final String scope, final String stream, final UUID txId, final Optional<OperationContext> contextOpt) {
        final OperationContext context = contextOpt.orElse(streamMetadataStore.createContext(scope, stream));

        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId},
                () -> dropTxBody(scope, stream, txId, context));
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
    public CompletableFuture<TxnStatus> commitTx(final String scope, final String stream, final UUID txId, final Optional<OperationContext> contextOpt) {
        final OperationContext context = contextOpt.orElse(streamMetadataStore.createContext(scope, stream));
        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId},
                () -> commitTxBody(scope, stream, txId, context));
    }

    private CompletableFuture<UUID> createTxBody(final String scope, final String stream, final OperationContext context) {
        return streamMetadataStore.createTransaction(scope, stream, context)
                .thenCompose(txId ->
                        txTimeOutScheduler.scheduleTimeOut(scope, stream, txId, Config.TXN_TIMEOUT_IN_SECONDS)
                                .thenCompose(x ->
                                        streamMetadataStore.getActiveSegments(scope, stream, context)
                                                .thenCompose(activeSegments ->
                                                        FutureCollectionHelper.sequence(
                                                                activeSegments.stream()
                                                                        .parallel()
                                                                        .map(segment -> notifyTxCreation(scope, stream, segment.getNumber(), txId))
                                                                        .collect(Collectors.toList())))
                                                .thenApply(y -> txId)));
    }

    private CompletableFuture<TxnStatus> dropTxBody(final String scope, final String stream, final UUID txid, final OperationContext context) {
        // notify hosts to abort transaction
        return streamMetadataStore.getActiveSegments(scope, stream, context)
                .thenCompose(segments ->
                        FutureCollectionHelper.sequence(
                                segments.stream()
                                        .parallel()
                                        .map(segment -> notifyDropToHost(scope, stream, segment.getNumber(), txid))
                                        .collect(Collectors.toList())))
                .thenCompose(x -> streamMetadataStore.dropTransaction(scope, stream, txid, context));
    }

    private CompletableFuture<TxnStatus> commitTxBody(final String scope, final String stream, final UUID txid, final OperationContext context) {
        return streamMetadataStore.sealTransaction(scope, stream, txid, context)
                .thenCompose(x ->
                        streamMetadataStore.getActiveSegments(scope, stream, context)
                                .thenCompose(segments ->
                                        FutureCollectionHelper.sequence(segments.stream()
                                                .parallel()
                                                .map(segment ->
                                                        notifyCommitToHost(scope, stream, segment.getNumber(), txid))
                                                .collect(Collectors.toList()))))
                .thenCompose(x -> streamMetadataStore.commitTransaction(scope, stream, txid, context));
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

    private CompletableFuture<com.emc.pravega.controller.stream.api.v1.TxnStatus> notifyDropToHost(final String scope, final String stream, final int segmentNumber, final UUID txId) {
        return Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
                .retryingOn(WireCommandFailedException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> SegmentHelper.dropTransaction(scope,
                        stream,
                        segmentNumber,
                        txId,
                        this.hostControllerStore,
                        this.connectionFactory), executor);
    }

    private CompletableFuture<com.emc.pravega.controller.stream.api.v1.TxnStatus> notifyCommitToHost(final String scope, final String stream, final int segmentNumber, final UUID txId) {
        return Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
                .retryingOn(WireCommandFailedException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> SegmentHelper.commitTransaction(scope,
                        stream,
                        segmentNumber,
                        txId,
                        this.hostControllerStore,
                        this.connectionFactory), executor);
    }
}
