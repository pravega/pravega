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

import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands.GetTransactionInfo;
import com.emc.pravega.common.netty.WireCommands.TransactionInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.common.util.RetriesExaustedException;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.Controller;
import com.google.common.annotations.VisibleForTesting;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@VisibleForTesting
@RequiredArgsConstructor
public class SegmentOutputStreamFactoryImpl implements SegmentOutputStreamFactory {

    private final Controller controller;
    private final ConnectionFactory cf;

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId) {
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
        controller.getEndpointForSegment(segment.getQualifiedName()).thenCompose((PravegaNodeUri endpointForSegment)
                -> {
            return cf.establishConnection(endpointForSegment, replyProcessor);
        }).thenAccept((ClientConnection connection) -> {
            try {
                connection.send(new GetTransactionInfo(segment.getQualifiedName(), txId));
            } catch (ConnectionFailedException e) {
                throw new RuntimeException(e);
            }
        }).exceptionally((Throwable t) -> {
            name.completeExceptionally(t);
            return null;
        });
        return new SegmentOutputStreamImpl(getAndHandleExceptions(name, RuntimeException::new), controller, cf, UUID
                .randomUUID());
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, SegmentOutputConfiguration config)
            throws SegmentSealedException {
        SegmentOutputStreamImpl result = new SegmentOutputStreamImpl(segment.getQualifiedName(), controller, cf, UUID
                .randomUUID());
        try {
            result.getConnection();
        } catch (RetriesExaustedException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return result;
    }
}
