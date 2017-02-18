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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.server.eventProcessor.AbortEvent;
import com.emc.pravega.controller.server.eventProcessor.CommitEvent;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.VersionedTransactionData;
import com.emc.pravega.controller.store.task.Resource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import com.emc.pravega.stream.impl.TxnStatus;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;

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
public class StreamTransactionMetadataTasks extends TaskBase implements Cloneable {
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 10;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = 100000;

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactoryImpl connectionFactory;

    public StreamTransactionMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final TaskMetadataStore taskMetadataStore,
                                          final ScheduledExecutorService executor,
                                          final String hostId) {
        super(taskMetadataStore, executor, hostId);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.connectionFactory = new ConnectionFactoryImpl(false);
    }

    @Override
    public StreamTransactionMetadataTasks clone() throws CloneNotSupportedException {
        return (StreamTransactionMetadataTasks) super.clone();
    }

    /**
     * Create transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param lease Time for which transaction shall remain open with sending any heartbeat.
     * @param maxExecutionTime Maximum time for which client may extend txn lease.
     * @param scaleGracePeriod Maximum time for which client may extend txn lease once
     *                         the scaling operation is initiated on the txn stream.
     * @return transaction id.
     */
    @Task(name = "createTransaction", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<VersionedTransactionData> createTx(final String scope, final String stream, final long lease,
                                            final long maxExecutionTime, final long scaleGracePeriod) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream},
                () -> createTxBody(scope, stream, lease, maxExecutionTime, scaleGracePeriod));
    }

    /**
     *
     */
    /**
     * Transaction heartbeat, that increases transaction timeout by lease number of milliseconds.
     *
     * @param scope Stream scope.
     * @param stream Stream name.
     * @param txId Transaction identifier.
     * @param lease Amount of time in milliseconds by which to extend the transaction lease.
     * @return Transaction metadata along with the version of it record in the store.
     */
    public CompletableFuture<VersionedTransactionData> pingTx(final String scope, final String stream,
                                                              final UUID txId, final long lease) {
        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId},
                () -> pingTxBody(scope, stream, txId, lease));
    }

    /**
     * Abort transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @param version Expected version of the transaction record in the store.
     * @return true/false.
     */
    @Task(name = "abortTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<TxnStatus> abortTx(final String scope, final String stream, final UUID txId,
                                                final Optional<Integer> version) {
        return execute(
                new Resource(scope, stream, txId.toString()),
                new Serializable[]{scope, stream, txId},
                () -> abortTxBody(scope, stream, txId, version));
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

    private CompletableFuture<VersionedTransactionData> createTxBody(final String scope, final String stream,
                                                                     final long lease, final long maxExecutionPeriod,
                                                                     final long scaleGracePeriod) {
        return streamMetadataStore.createTransaction(scope, stream, lease, maxExecutionPeriod, scaleGracePeriod)
                .thenCompose(txData ->
                        streamMetadataStore.getActiveSegments(stream)
                                .thenCompose(activeSegments ->
                                        FutureHelpers.allOf(
                                                activeSegments.stream()
                                                        .parallel()
                                                        .map(segment ->
                                                                notifyTxCreation(scope,
                                                                        stream,
                                                                        segment.getNumber(),
                                                                        txData.getId()))
                                                        .collect(Collectors.toList())))
                                .thenApply(x -> txData));
    }

    private CompletableFuture<VersionedTransactionData> pingTxBody(String scope, String stream, UUID txId, long lease) {
        return streamMetadataStore.pingTransaction(scope, stream, txId, lease);
    }

    private CompletableFuture<TxnStatus> abortTxBody(final String scope, final String stream, final UUID txid,
                                                     final Optional<Integer> version) {
        return streamMetadataStore.sealTransaction(scope, stream, txid, false, version)
                .thenApply(status -> {
                    ControllerEventProcessors
                            .getAbortEventProcessorsRef()
                            .writeEvent(txid.toString(), new AbortEvent(scope, stream, txid));
                    return status;
                });
    }

    private CompletableFuture<TxnStatus> commitTxBody(final String scope, final String stream, final UUID txid) {
        return streamMetadataStore.sealTransaction(scope, stream, txid, true, Optional.empty())
                .thenApply(status -> {
                    ControllerEventProcessors
                            .getCommitEventProcessorsRef()
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

    private CompletableFuture<com.emc.pravega.controller.stream.api.v1.TxnStatus> notifyDropToHost(final String scope, final String stream, final int segmentNumber, final UUID txId) {
        return Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
                    .retryingOn(WireCommandFailedException.class)
                    .throwingOn(RuntimeException.class)
                    .runAsync(() -> SegmentHelper.abortTransaction(scope,
                                                                  stream,
                                                                  segmentNumber,
                                                                  txId,
                                                                  this.hostControllerStore,
                                                                  this.connectionFactory),
                              executor);
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
                                                                    this.connectionFactory),
                              executor);
    }
}
