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

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.hash.HashHelper;
import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommand;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.TransactionStatus;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.model.ModelHelper;

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

        sendRequestOverNewConnection(new WireCommands.CreateSegment(Segment.getQualifiedName(scope, stream, segmentNumber)),
                replyProcessor,
                clientCF,
                uri);

        return true;
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
    public static Boolean sealSegment(final String scope,
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
        sendRequestOverNewConnection(new WireCommands.SealSegment(Segment.getQualifiedName(scope, stream, segmentNumber)),
                replyProcessor, clientCF, ModelHelper.encode(uri));

        return true;
    }

    public static boolean createTransaction(final String scope,
                                            final String stream,
                                            final int segmentNumber,
                                            final UUID txId,
                                            final PravegaNodeUri uri,
                                            final ConnectionFactory clientCF) {
        final CompletableFuture<UUID> result = new CompletableFuture<>();

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
            public void transactionCreated(WireCommands.TransactionCreated transactionCreated) {
                result.complete(txId);
            }
        };

        sendRequestOverNewConnection(new WireCommands.CreateTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                uri);

        return true;
    }

    public static boolean commitTransaction(final String scope,
                                            final String stream,
                                            final int segmentNumber,
                                            final UUID txId,
                                            final HostControllerStore hostControllerStore,
                                            final ConnectionFactory clientCF) {
        final NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        CompletableFuture<TransactionStatus> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
                result.complete(TransactionStatus.SUCCESS);
            }

            @Override
            public void transactionDropped(WireCommands.TransactionDropped transactionDropped) {
                result.completeExceptionally(new TxFailedException("Transaction already dropped."));
            }
        };

        sendRequestOverNewConnection(new WireCommands.CommitTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId),
                replyProcessor, clientCF, ModelHelper.encode(uri));

        return true;
    }

    public static boolean dropTransaction(final String scope,
                                          final String stream,
                                          final int segmentNumber,
                                          final UUID txId,
                                          final HostControllerStore hostControllerStore,
                                          final ConnectionFactory clientCF) {
        final NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
        CompletableFuture<TransactionStatus> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
                result.completeExceptionally(new RuntimeException("Transaction already committed."));
            }

            @Override
            public void transactionDropped(WireCommands.TransactionDropped transactionDropped) {
                result.complete(TransactionStatus.SUCCESS);
            }
        };

        sendRequestOverNewConnection(new WireCommands.DropTransaction(Segment.getQualifiedName(scope, stream, segmentNumber), txId),
                replyProcessor, clientCF, ModelHelper.encode(uri));

        return true;
    }

    private static void sendRequestOverNewConnection(final WireCommand request,
                                                     final ReplyProcessor replyProcessor,
                                                     final ConnectionFactory connectionFactory,
                                                     final PravegaNodeUri uri) {
        // TODO: retry on connection failure
        ClientConnection connection = getAndHandleExceptions(connectionFactory
                .establishConnection(uri, replyProcessor), RuntimeException::new);
        try {
            connection.send(request);
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
    }

}
