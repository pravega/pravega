/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.stream.mock;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.Request;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.common.netty.WireCommands.CommitTransaction;
import com.emc.pravega.common.netty.WireCommands.CreateTransaction;
import com.emc.pravega.common.netty.WireCommands.DropTransaction;
import com.emc.pravega.common.netty.WireCommands.TransactionCommitted;
import com.emc.pravega.common.netty.WireCommands.TransactionCreated;
import com.emc.pravega.common.netty.WireCommands.TransactionDropped;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamSegments;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.PositionImpl;
import com.google.common.collect.ImmutableList;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MockController implements Controller {

    private final String endpoint;
    private final int port;
    private final ConnectionFactory connectionFactory;

    @Override
    public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
        Segment segmentId = new Segment(streamConfig.getScope(), streamConfig.getName(), 0);

        createSegment(segmentId.getQualifiedName(), new PravegaNodeUri(endpoint, port));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return CompletableFuture.completedFuture(Status.SUCCESS);
    }

    @Override
    public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
        return null;
    }

    boolean createSegment(String name, PravegaNodeUri uri) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
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
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                result.complete(false);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                result.complete(true);
            }
        };
        ClientConnection connection = getAndHandleExceptions(connectionFactory.establishConnection(uri, replyProcessor),
                                                             RuntimeException::new);
        try {
            connection.send(new WireCommands.CreateSegment(name));
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String scope, String stream) {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(1.0, new Segment(scope, stream, 0));
        return CompletableFuture.completedFuture(new StreamSegments(System.currentTimeMillis(), segments));
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, UUID txId) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionCommitted(TransactionCommitted transactionCommitted) {
                result.complete(null);
            }

            @Override
            public void transactionDropped(TransactionDropped transactionDropped) {
                result.completeExceptionally(new TxFailedException("Transaction already dropped."));
            }
        };
        sendRequestOverNewConnection(new CommitTransaction(Segment.getQualifiedName(stream.getScope(), stream.getStreamName(), 0), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<Void> dropTransaction(Stream stream, UUID txId) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionCommitted(TransactionCommitted transactionCommitted) {
                result.completeExceptionally(new RuntimeException("Transaction already committed."));
            }

            @Override
            public void transactionDropped(TransactionDropped transactionDropped) {
                result.complete(null);
            }
        };
        sendRequestOverNewConnection(new DropTransaction(Segment.getQualifiedName(stream.getScope(), stream.getStreamName(), 0), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Void> createTransaction(Stream stream, UUID txId, long timeout) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionCreated(TransactionCreated transactionCreated) {
                result.complete(null);
            }
        };
        sendRequestOverNewConnection(new CreateTransaction(Segment.getQualifiedName(stream.getScope(), stream.getStreamName(), 0), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(Stream stream, long timestamp, int count) {
        return CompletableFuture.completedFuture(ImmutableList.<PositionInternal>of(getInitialPosition(stream.getScope(), stream.getStreamName())));
    }

    @Override
    public CompletableFuture<List<PositionInternal>> updatePositions(Stream stream, List<PositionInternal> positions) {
        return CompletableFuture.completedFuture(positions);
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        return CompletableFuture.completedFuture(new PravegaNodeUri(endpoint, port));
    }
    
    public PositionImpl getInitialPosition(String scope, String stream) {
        return new PositionImpl(Collections.singletonMap(new Segment(scope, stream, 0), 0L), Collections.emptyMap());
    }
    
    private void sendRequestOverNewConnection(Request request, ReplyProcessor replyProcessor) {
        ClientConnection connection = getAndHandleExceptions(connectionFactory
            .establishConnection(new PravegaNodeUri(endpoint, port), replyProcessor), RuntimeException::new);
        try {
            connection.send(request);
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
    }
}

