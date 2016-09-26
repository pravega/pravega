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
package com.emc.pravega.stream.impl.segment;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.Request;
import com.emc.pravega.common.netty.WireCommands.CommitTransaction;
import com.emc.pravega.common.netty.WireCommands.CreateSegment;
import com.emc.pravega.common.netty.WireCommands.CreateTransaction;
import com.emc.pravega.common.netty.WireCommands.DropTransaction;
import com.emc.pravega.common.netty.WireCommands.GetTransactionInfo;
import com.emc.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.TransactionCommitted;
import com.emc.pravega.common.netty.WireCommands.TransactionCreated;
import com.emc.pravega.common.netty.WireCommands.TransactionDropped;
import com.emc.pravega.common.netty.WireCommands.TransactionInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.common.util.RetriesExaustedException;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.Transaction.Status;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.SingleSegmentStreamImpl;
import com.emc.pravega.stream.impl.StreamController;
import com.google.common.annotations.VisibleForTesting;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@VisibleForTesting
public class SingleSegmentStreamControllerImpl implements StreamController {

    private final String endpoint;
    private final ConnectionFactory connectionFactory;

    @Override
    @Synchronized
    public boolean createSegment(String segmentName) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
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
            public void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists) {
                result.complete(false);
            }

            @Override
            public void segmentCreated(SegmentCreated segmentCreated) {
                result.complete(true);
            }
        };
        sendRequestOverNewConnection(new CreateSegment(segmentName), replyProcessor);
        return getAndHandleExceptions(result, RuntimeException::new);
    }



    @Override
    public SegmentOutputStream openSegmentForAppending(String name, SegmentOutputConfiguration config)
            throws SegmentSealedException {
        SegmentOutputStreamImpl result = new SegmentOutputStreamImpl(this, connectionFactory, UUID.randomUUID(), name);
        try {
            result.getConnection();
        } catch (RetriesExaustedException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return result;
    }

    @Override
    public SegmentInputStream openSegmentForReading(String name, SegmentInputConfiguration config) {
        AsyncSegmentInputStreamImpl result = new AsyncSegmentInputStreamImpl(this, connectionFactory, name);
        try {
            Exceptions.handleInterrupted(() -> result.getConnection().get());
        } catch (ExecutionException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return new SegmentInputStreamImpl(result, 0);
    }

    @Override
    public SegmentOutputStream openTransactionForAppending(String segment, UUID txId) {
        CompletableFuture<String> name = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                name.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                name.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionInfo(TransactionInfo info) {
               name.complete(info.getTransactionName());
            }
        };
        sendRequestOverNewConnection(new GetTransactionInfo(segment, txId), replyProcessor);
        return new SegmentOutputStreamImpl(this, connectionFactory, UUID.randomUUID(), getAndHandleExceptions(name, RuntimeException::new));
    }

    @Override
    @Synchronized
    public void createTransaction(Stream stream, UUID txId, long timeout) {
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
        SegmentId segmentId = ((SingleSegmentStreamImpl) stream).getSegmentId();
        sendRequestOverNewConnection(new CreateTransaction(segmentId.getQualifiedName(), txId), replyProcessor);
        getAndHandleExceptions(result, RuntimeException::new);
    }

    @Override
    public void commitTransaction(Stream stream, UUID txId) throws TxFailedException {
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
        SegmentId segmentId = ((SingleSegmentStreamImpl) stream).getSegmentId();
        sendRequestOverNewConnection(new CommitTransaction(segmentId.getQualifiedName(), txId), replyProcessor);
        getAndHandleExceptions(result, TxFailedException::new);
    }

    @Override
    public void dropTransaction(Stream stream, UUID txId) {
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
        SegmentId segmentId = ((SingleSegmentStreamImpl) stream).getSegmentId();
        sendRequestOverNewConnection(new DropTransaction(segmentId.getQualifiedName(), txId), replyProcessor);
        getAndHandleExceptions(result, RuntimeException::new);
    }

    @Override
    public Status checkTransactionStatus(Stream stream, UUID txId) {
        CompletableFuture<Status> result = new CompletableFuture<>();
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
            public void transactionInfo(TransactionInfo info) {
                if (!info.exists()) {
                    throw new IllegalStateException("This transaction was forgotten about by the server: " + txId);
                }
                if (info.isCommitted()) {
                    result.complete(Status.COMMITTED);
                } else if (info.isDeleted()) {
                    result.complete(Status.DROPPED);
                } else if (info.isSealed()) {
                    result.complete(Status.SEALED);
                } else {
                    result.complete(Status.OPEN);
                }
            }
        };
        SegmentId segmentId = ((SingleSegmentStreamImpl) stream).getSegmentId();
        sendRequestOverNewConnection(new GetTransactionInfo(segmentId.getQualifiedName(), txId), replyProcessor);
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    @Override
    public String getEndpointForSegment(String segment) {
        return endpoint;
    }

}
