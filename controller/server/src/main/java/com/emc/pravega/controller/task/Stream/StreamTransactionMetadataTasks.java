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
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.task.Paths;
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import com.emc.pravega.stream.impl.TxStatus;
import com.emc.pravega.stream.impl.model.ModelHelper;
import org.apache.curator.framework.CuratorFramework;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
public class StreamTransactionMetadataTasks extends TaskBase {

    public StreamTransactionMetadataTasks(StreamMetadataStore streamMetadataStore, HostControllerStore hostControllerStore, CuratorFramework client) {
        super(streamMetadataStore, hostControllerStore, client);
    }

    /**
     * Create transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @return transaction id.
     */
    @Task(name = "createTransaction", version = "1.0")
    public CompletableFuture<UUID> createTx(String scope, String stream) {
        return execute(
                String.format(Paths.STREAM_LOCKS, scope, stream),
                String.format(Paths.STREAM_TASKS, scope, stream),
                new Serializable[]{scope, stream},
                () -> createTxBody(scope, stream));
    }

    /**
     * Drop transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @return true/false.
     */
    @Task(name = "dropTransaction", version = "1.0")
    public CompletableFuture<TxStatus> dropTx(String scope, String stream, UUID txId) {
        return execute(
                String.format(Paths.STREAM_LOCKS, scope, stream),
                String.format(Paths.STREAM_TASKS, scope, stream),
                new Serializable[]{scope, stream, txId},
                () -> dropTxBody(scope, stream, txId));
    }

    /**
     * Commit transaction.
     *
     * @param scope  stream scope.
     * @param stream stream name.
     * @param txId   transaction id.
     * @return true/false.
     */
    @Task(name = "commitTransaction", version = "1.0")
    public CompletableFuture<TxStatus> commitTx(String scope, String stream, UUID txId) {
        return execute(
                String.format(Paths.STREAM_LOCKS, scope, stream),
                String.format(Paths.STREAM_TASKS, scope, stream),
                new Serializable[]{scope, stream, txId},
                () -> commitTxBody(scope, stream, txId));
    }

    private CompletableFuture<UUID> createTxBody(String scope, String stream) {
        return streamMetadataStore.createTransaction(scope, stream)
                .thenApply(txId -> {
                    notifyTxCreationASync(scope, stream, txId);
                    return txId;
                });
    }

    private CompletableFuture<TxStatus> dropTxBody(String scope, String stream, UUID txid) {
        // notify hosts to drop transaction
        return streamMetadataStore.getActiveSegments(stream)
                .thenCompose(segments ->
                        FutureCollectionHelper.sequence(
                                segments.stream()
                                        .parallel()
                                        .map(segment -> notifyDropToHost(scope, stream, segment.getNumber(), txid))
                                        .collect(Collectors.toList())))
                .thenCompose(x -> streamMetadataStore.dropTransaction(scope, stream, txid));
    }

    private CompletableFuture<TxStatus> commitTxBody(String scope, String stream, UUID txid) {
        return streamMetadataStore.sealTransaction(scope, stream, txid)
                .thenCompose(x -> streamMetadataStore.getActiveSegments(stream)
                        .thenCompose(segments -> {
                            List<CompletableFuture<Void>> collect = segments.stream()
                                    .parallel()
                                    .map(segment -> notifyCommitToHost(scope, stream, segment.getNumber(), txid))
                                    .collect(Collectors.toList());
                            return FutureCollectionHelper.sequence(collect);
                        }))
                .thenCompose(x -> streamMetadataStore.commitTransaction(scope, stream, txid));
    }

    private void notifyTxCreationASync(String scope, String stream, UUID txid) {
        streamMetadataStore.getActiveSegments(stream)
                .thenApply(activeSegments -> {
                    activeSegments
                            .stream()
                            .forEach(segment -> {
                                NodeUri uri = SegmentHelper.getSegmentUri(scope,
                                        stream,
                                        segment.getNumber(),
                                        this.hostControllerStore);
                                CompletableFuture.runAsync(() ->
                                        SegmentHelper.createTransaction(scope,
                                                stream,
                                                segment.getNumber(),
                                                txid,
                                                ModelHelper.encode(uri),
                                                this.connectionFactory));
                            });
                    return null;
                });
    }

    public CompletableFuture<Void> notifyDropToHost(String scope, String stream, int segmentNumber, UUID txId) {
        return CompletableFuture.supplyAsync(() -> {
            // TODO: clean this. have exponential back off for retries.
            // Also dont block a thread trying to just do this
            boolean result = false;
            while (!result) {
                try {
                    NodeUri uri = SegmentHelper.getSegmentUri(scope,
                            stream,
                            segmentNumber,
                            this.hostControllerStore);

                    result = SegmentHelper.dropTransaction(scope,
                            stream,
                            segmentNumber,
                            txId,
                            ModelHelper.encode(uri),
                            this.connectionFactory);
                } catch (RuntimeException ex) {
                    //log exception and continue retrying
                }
            }
            return null;
        });
    }

    public CompletableFuture<Void> notifyCommitToHost(String scope, String stream, int segmentNumber, UUID txId) {
        return CompletableFuture.supplyAsync(() -> {
            // TODO: clean this. have exponential back off for retries
            // Also dont block a thread trying to just do this
            boolean result = false;
            while (!result) {
                try {
                    NodeUri uri = SegmentHelper.getSegmentUri(scope,
                            stream,
                            segmentNumber,
                            this.hostControllerStore);

                    result = SegmentHelper.commitTransaction(scope,
                            stream,
                            segmentNumber,
                            txId,
                            ModelHelper.encode(uri),
                            this.connectionFactory);
                } catch (RuntimeException ex) {
                    //log exception and continue retrying
                }
            }
            return null;
        });
    }
}
