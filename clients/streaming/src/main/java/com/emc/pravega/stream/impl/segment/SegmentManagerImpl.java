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
import com.emc.pravega.common.netty.WireCommands.CreateSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.common.util.RetriesExaustedException;
import com.emc.pravega.stream.Transaction.Status;
import com.emc.pravega.stream.TxFailedException;
import com.google.common.annotations.VisibleForTesting;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@VisibleForTesting
public class SegmentManagerImpl implements SegmentManager {

    private final String endpoint;
    private final ConnectionFactory connectionFactory;

    @Override
    @Synchronized
    public boolean createSegment(String name) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {
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
        ClientConnection connection = getAndHandleExceptions(connectionFactory.establishConnection(endpoint, replyProcessor),
                                                             RuntimeException::new);
        try {
            connection.send(new CreateSegment(name));
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    @Override
    public SegmentOutputStream openSegmentForAppending(String name, SegmentOutputConfiguration config)
            throws SegmentSealedException {
        SegmentOutputStreamImpl result = new SegmentOutputStreamImpl(connectionFactory, endpoint, UUID.randomUUID(), name);
        try {
            result.getConnection();
        } catch (RetriesExaustedException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return result;
    }

    @Override
    public SegmentInputStream openSegmentForReading(String name, SegmentInputConfiguration config) {
        AsyncSegmentInputStreamImpl result = new AsyncSegmentInputStreamImpl(connectionFactory, endpoint, name);
        try {
            Exceptions.handleInterupted(() -> result.getConnection().get()/*, ExecutionException.class*/);
        } catch (ExecutionException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return new SegmentInputStreamImpl(result, 0);
    }

    @Override
    public SegmentOutputStream openTransactionForAppending(String segmentName, UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public void createTransaction(String segmentName, UUID txId, long timeout) {
        throw new NotImplementedException();
    }

    @Override
    public void commitTransaction(UUID txId) throws TxFailedException {
        throw new NotImplementedException();
    }

    @Override
    public boolean dropTransaction(UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public Status checkTransactionStatus(UUID txId) {
        throw new NotImplementedException();
    }

}
