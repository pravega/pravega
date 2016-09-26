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

package com.emc.pravega.integrationtests.mockController;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamSegments;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;

public class MockController {

    public static MockHost getHost(String endpoint, int port) {
        return new MockHost(endpoint, port);
    }
    
    public static MockAdmin getAdmin(String endpoint, int port) {
        return new MockAdmin(endpoint, port);
    }

    public static MockProducer getProducer(String endpoint, int port) {
        return new MockProducer(endpoint, port);
    }

    public static MockConsumer getConsumer(String endpoint, int port) {
        return new MockConsumer(endpoint, port);
    }

    @AllArgsConstructor
    private static class MockAdmin implements Controller.Admin {
        private final String endpoint;
        private final int port;

        @Override
        public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
            SegmentId segmentId = new SegmentId(streamConfig.getName(), streamConfig.getName(), 0, -1);

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
        
        static boolean createSegment(String name, PravegaNodeUri uri) {
            ConnectionFactory clientCF = new ConnectionFactoryImpl(false);

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
            ClientConnection connection = getAndHandleExceptions(clientCF.establishConnection(uri, replyProcessor),
                    RuntimeException::new);
            try {
                connection.send(new WireCommands.CreateSegment(name));
            } catch (ConnectionFailedException e) {
                throw new RuntimeException(e);
            }
            return getAndHandleExceptions(result, RuntimeException::new);
        }
    }

    @AllArgsConstructor
    private static class MockProducer implements Controller.Producer {

        private final String endpoint;
        private final int port;

        @Override
        public CompletableFuture<StreamSegments> getCurrentSegments(String stream) {
            return CompletableFuture.completedFuture(new StreamSegments(
                    Lists.newArrayList(new SegmentId(stream, stream, 0, -1)),
                    System.currentTimeMillis()));
        }

        @Override
        public void commitTransaction(Stream stream, UUID txId) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void dropTransaction(Stream stream, UUID txId) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public Transaction.Status checkTransactionStatus(UUID txId) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void createTransaction(SegmentId s, UUID txId, long timeout) {
            // TODO Auto-generated method stub
            
        }

    }

    @AllArgsConstructor
    private static class MockConsumer implements Controller.Consumer {
        private final String endpoint;
        private final int port;

        @Override
        public CompletableFuture<List<PositionInternal>> getPositions(String stream, long timestamp, int count) {
            return null;
        }

        @Override
        public CompletableFuture<List<PositionInternal>> updatePositions(String stream, List<PositionInternal> positions) {
            return null;
        }
    }
    
    @AllArgsConstructor
    public static class MockHost implements Controller.Host {
        private final String endpoint;
        private final int port;
        @Override
        public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
            return CompletableFuture.completedFuture(new PravegaNodeUri(endpoint, port));
        }
    }

}
