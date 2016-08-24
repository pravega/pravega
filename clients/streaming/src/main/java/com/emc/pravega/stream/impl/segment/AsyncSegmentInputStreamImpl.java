/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.stream.impl.segment;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.stream.ConnectionClosedException;
import com.google.common.base.Preconditions;

import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AsyncSegmentInputStreamImpl extends AsyncSegmentInputStream {

    private final ConnectionFactory connectionFactory;
    private final String endpoint;
    private final String segment;
    @GuardedBy("$lock")
    private CompletableFuture<ClientConnection> connection = null;
    private final ConcurrentHashMap<Long, CompletableFuture<SegmentRead>> outstandingRequests = new ConcurrentHashMap<>();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();

    private final class ResponseProcessor extends FailingReplyProcessor {

        @Override
        public void wrongHost(WrongHost wrongHost) {
            closeConnection();
            nackInflight(new ConnectionFailedException(wrongHost.toString()));
        }

        @Override
        public void noSuchSegment(NoSuchSegment noSuchSegment) {
            closeConnection();
            nackInflight(new IllegalArgumentException(noSuchSegment.toString()));
        }

        @Override
        public void segmentRead(SegmentRead segmentRead) {
            CompletableFuture<SegmentRead> future = outstandingRequests.remove(segmentRead.getOffset());
            if (future != null) {
                future.complete(segmentRead);
            }
        }
    }

    public AsyncSegmentInputStreamImpl(ConnectionFactory connectionFactory, String endpoint, String segment) {
        Preconditions.checkNotNull(connectionFactory);
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(segment);
        this.connectionFactory = connectionFactory;
        this.endpoint = endpoint;
        this.segment = segment;
    }


    @Override
    public void close() {
        closeConnection();
        nackInflight(new ConnectionClosedException());
    }

    @Override
    public CompletableFuture<SegmentRead> read(long offset, int length) {
        CompletableFuture<SegmentRead> future = new CompletableFuture<>();
        outstandingRequests.put(offset, future);
        getConnection().thenApply( (ClientConnection c) -> {
            try {
                c.send(new ReadSegment(segment, offset, length));
            } catch (ConnectionFailedException e) {
                closeConnection();
                nackInflight(e);
            }
            return null;
        });
        return future;
    }

    @Synchronized
    private void closeConnection() {
        if (connection != null && FutureHelpers.isSuccessful(connection)) {
            try {
                connection.getNow(null).close();
            } catch (Exception e) {
                log.warn("Exception tearing down connection: ",e);
            }
            connection = null;
        }
    }
    
    @Synchronized 
    CompletableFuture<ClientConnection> getConnection() {
        if (connection == null) {
            connection = connectionFactory.establishConnection(endpoint, responseProcessor);
        }
        return connection;
    }
    
    private void nackInflight(Exception e) {
        for (Iterator<Entry<Long, CompletableFuture<SegmentRead>>> iterator = outstandingRequests.entrySet()
                .iterator(); iterator.hasNext();) {
            Entry<Long, CompletableFuture<SegmentRead>> read = iterator.next();
            read.getValue().completeExceptionally(e);
            iterator.remove();
        }
    }
    
}
