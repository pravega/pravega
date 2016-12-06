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

import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommand;
import com.emc.pravega.common.netty.WireCommandType;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.TransactionStatus;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.model.ModelHelper;


public class SegmentHelper {

    public static NodeUri getSegmentUri(final String scope,
                                        final String stream,
                                        final int segmentNumber,
                                        final HostControllerStore hostStore) {
        final Host host = hostStore.getHostForSegment(scope, stream, segmentNumber);
        return new NodeUri(host.getIpAddr(), host.getPort());
    }

    public static CompletableFuture<Boolean> createSegment(final String scope,
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

        return sendRequestOverNewConnection(
                new WireCommands.CreateSegment(Segment.getQualifiedName(scope, stream, segmentNumber)),
                replyProcessor,
                clientCF,
                uri)
                .thenCompose(x -> result);
    }

    /**
     * This method sends segment sealed message for the specified segment.
     * It owns up the responsibility of retrying the operation on failures until success.
     *
     * @param scope               stream scope
     * @param stream              stream name
     * @param segmentNumber       number of segment to be sealed
     * @param hostControllerStore host controller store
     * @param clientCF            connection factory
     * @return void
     */
    public static CompletableFuture<Boolean> sealSegment(final String scope,
                                                         final String stream,
                                                         final int segmentNumber,
                                                         final HostControllerStore hostControllerStore,
                                                         final ConnectionFactory clientCF) {
        final NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
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

        return sendRequestOverNewConnection(
                new WireCommands.SealSegment(Segment.getQualifiedName(scope, stream, segmentNumber)),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri))
                .thenCompose(x -> result);
    }

    public static CompletableFuture<UUID> createTransaction(final String scope,
                                                            final String stream,
                                                            final int segmentNumber,
                                                            final UUID txId,
                                                            final HostControllerStore hostControllerStore,
                                                            final ConnectionFactory clientCF) {
        final NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        final CompletableFuture<UUID> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.CREATE_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCreated(WireCommands.TransactionCreated transactionCreated) {
                result.complete(txId);
            }
        };

        return sendRequestOverNewConnection(
                new WireCommands.CreateTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri))
                .thenCompose(x -> result);
    }

    public static CompletableFuture<TransactionStatus> commitTransaction(final String scope,
                                                                         final String stream,
                                                                         final int segmentNumber,
                                                                         final UUID txId,
                                                                         final HostControllerStore hostControllerStore,
                                                                         final ConnectionFactory clientCF) {
        final NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        final CompletableFuture<TransactionStatus> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.COMMIT_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
                result.complete(TransactionStatus.SUCCESS);
            }

            @Override
            public void transactionDropped(WireCommands.TransactionDropped transactionDropped) {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.PreconditionFailed));
            }
        };

        return sendRequestOverNewConnection(
                new WireCommands.CommitTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri))
                .thenCompose(x -> result);
    }

    public static CompletableFuture<TransactionStatus> dropTransaction(final String scope,
                                                                       final String stream,
                                                                       final int segmentNumber,
                                                                       final UUID txId,
                                                                       final HostControllerStore hostControllerStore,
                                                                       final ConnectionFactory clientCF) {
        final NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
        final CompletableFuture<TransactionStatus> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.DROP_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.PreconditionFailed));
            }

            @Override
            public void transactionDropped(WireCommands.TransactionDropped transactionDropped) {
                result.complete(TransactionStatus.SUCCESS);
            }
        };

        return sendRequestOverNewConnection(
                new WireCommands.DropTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri))
                .thenCompose(x -> result);
    }

    private static CompletableFuture<Void> sendRequestOverNewConnection(final WireCommand request,
                                                                        final ReplyProcessor replyProcessor,
                                                                        final ConnectionFactory connectionFactory,
                                                                        final PravegaNodeUri uri) {
        return connectionFactory.establishConnection(uri, replyProcessor)
                .thenApply(connection -> {
                    try {
                        connection.send(request);
                    } catch (ConnectionFailedException cfe) {
                        throw new WireCommandFailedException(cfe, request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
                    }
                    return null;
                });
    }

}
