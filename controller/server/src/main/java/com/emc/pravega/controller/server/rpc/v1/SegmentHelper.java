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

package com.emc.pravega.controller.server.rpc.v1;

import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import com.emc.pravega.common.util.Retry;
import com.emc.pravega.stream.impl.model.ModelHelper;
import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.hash.HashHelper;
import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.Segment;

public class SegmentHelper {

    public static NodeUri getSegmentUri(final String scope,
                                        final String stream,
                                        final int segmentNumber,
                                        final HostControllerStore hostStore) {
        final int container = HashHelper.seededWith("SegmentHelper").hashToBucket(stream + segmentNumber, hostStore.getContainerCount());
        final Host host = hostStore.getHostForContainer(container);
        return new NodeUri(host.getIpAddr(), host.getPort());
    }
    
    public static boolean createSegment(final String scope,
                                        final String stream,
                                        final int segmentNumber,
                                        final PravegaNodeUri uri,
                                        final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                result.complete(false);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                result.complete(true);
            }
        };
        final ClientConnection connection = FutureHelpers.getAndHandleExceptions(clientCF.establishConnection(uri, replyProcessor),
                RuntimeException::new);
        try {
            connection.send(new WireCommands.CreateSegment(Segment.getQualifiedName(scope, stream, segmentNumber)));
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
        return FutureHelpers.getAndHandleExceptions(result, RuntimeException::new);
    }

    /**
     * This method sends segment sealed message for the specified segment.
     * It owns up the responsibility of retrying the operation on failures until success.
     * @param scope stream scope
     * @param stream stream name
     * @param segmentNumber number of segment to be sealed
     * @return void
     */
    public static Boolean sealSegment(final String scope,
                                      final String stream,
                                      final int segmentNumber,
                                      final HostControllerStore hostControllerStore,
                                      final ConnectionFactory clientCF,
                                      final ScheduledExecutorService executor) {
        Retry.withExpBackoff(100, 10, Integer.MAX_VALUE, 100000)
                .retryingOn(SealingFailedException.class)
                .throwingOn(RuntimeException.class)
                .runFuture(() -> {
                    NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
                    return SegmentHelper.sealSegment(scope, stream, segmentNumber, ModelHelper.encode(uri), clientCF);
                }, executor);
        return true;
    }

    public static CompletableFuture<Boolean> sealSegment(final String scope,
                                                         final String stream,
                                                         final int segmentNumber,
                                                         final PravegaNodeUri uri,
                                                         final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new SealingFailedException(SealingFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new SealingFailedException(SealingFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                result.complete(true);
            }
        };
        return clientCF
                .establishConnection(uri, replyProcessor)
                .thenApply(connection -> {
                    try {
                        connection.send(new WireCommands.SealSegment(Segment.getQualifiedName(scope, stream, segmentNumber)));
                    } catch (ConnectionFailedException ex) {
                        throw new SealingFailedException(ex, SealingFailedException.Reason.ConnectionFailed);
                    }
                    return null;
                })
                .thenCompose(x -> result);
    }

    public static boolean createTransaction(final String scope,
                                            final String stream,
                                            final int segmentNumber,
                                            final UUID txId,
                                            final PravegaNodeUri uri,
                                            final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new UnknownHostException());
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                result.complete(true);
            }
        };
        final ClientConnection connection = FutureHelpers.getAndHandleExceptions(clientCF.establishConnection(uri, replyProcessor),
                RuntimeException::new);
        try {

            connection.send(new WireCommands.CreateTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId));

        } catch (ConnectionFailedException e) {

            throw new RuntimeException(e);

        }
        return FutureHelpers.getAndHandleExceptions(result, RuntimeException::new);
    }

    public static boolean commitTransaction(final String scope,
                                            final String stream,
                                            final int segmentNumber,
                                            final UUID txId,
                                            final HostControllerStore hostControllerStore,
                                            final ConnectionFactory clientCF,
                                            final ScheduledExecutorService executor) {
        Retry.withExpBackoff(100, 10, Integer.MAX_VALUE, 100000)
                .retryingOn(SealingFailedException.class)
                .throwingOn(RuntimeException.class)
                .runFuture(() -> {
                    NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
                    return SegmentHelper.commitTransaction(scope, stream, segmentNumber, txId, ModelHelper.encode(uri), clientCF);
                }, executor);
        return true;
    }

    private static CompletableFuture<Boolean> commitTransaction(final String scope,
                                                                final String stream,
                                                                final int segmentNumber,
                                                                final UUID txId,
                                                                final PravegaNodeUri uri,
                                                                final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new SealingFailedException(SealingFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new SealingFailedException(SealingFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                result.complete(true);
            }
        };

        return clientCF
                .establishConnection(uri, replyProcessor)
                .thenApply(connection -> {
                    try {
                        connection.send(new WireCommands.CommitTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId));
                    } catch (ConnectionFailedException ex) {
                        throw new SealingFailedException(ex, SealingFailedException.Reason.ConnectionFailed);
                    }
                    return null;
                })
                .thenCompose(x -> result);
    }

    public static boolean dropTransaction(final String scope,
                                          final String stream,
                                          final int segmentNumber,
                                          final UUID txId,
                                          final HostControllerStore hostControllerStore,
                                          final ConnectionFactory clientCF,
                                          final ScheduledExecutorService executor) {
        Retry.withExpBackoff(100, 10, Integer.MAX_VALUE, 100000)
                .retryingOn(SealingFailedException.class)
                .throwingOn(RuntimeException.class)
                .runFuture(() -> {
                    NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
                    return SegmentHelper.dropTransaction(scope, stream, segmentNumber, txId, ModelHelper.encode(uri), clientCF);
                }, executor);
        return true;
    }

    private static CompletableFuture<Boolean> dropTransaction(final String scope,
                                                              final String stream,
                                                              final int segmentNumber,
                                                              final UUID txId,
                                                              final PravegaNodeUri uri,
                                                              final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new SealingFailedException(SealingFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new SealingFailedException(SealingFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                result.complete(true);
            }
        };

        return clientCF
                .establishConnection(uri, replyProcessor)
                .thenApply(connection -> {
                    try {
                        connection.send(new WireCommands.DropTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId));
                    } catch (ConnectionFailedException ex) {
                        throw new SealingFailedException(ex, SealingFailedException.Reason.ConnectionFailed);
                    }
                    return null;
                })
                .thenCompose(x -> result);
    }
}
