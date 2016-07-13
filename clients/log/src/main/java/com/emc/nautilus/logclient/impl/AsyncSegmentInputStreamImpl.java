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
package com.emc.nautilus.logclient.impl;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.ConnectionFailedException;
import com.emc.nautilus.common.netty.FailingReplyProcessor;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncSegmentInputStreamImpl extends AsyncSegmentInputStream {

    private final ConnectionFactory connectionFactory;
    private final String endpoint;
    private final String segment;
    private final AtomicReference<ClientConnection> connection = new AtomicReference<>();
    private final ConcurrentHashMap<Long, CompletableFuture<SegmentRead>> outstandingRequests = new ConcurrentHashMap<>();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();

    private final class ResponseProcessor extends FailingReplyProcessor {

        @Override
        public void wrongHost(WrongHost wrongHost) {
            reconnect(new ConnectionFailedException(wrongHost.toString()));
        }

        @Override
        public void noSuchSegment(NoSuchSegment noSuchSegment) {
            reconnect(new IllegalArgumentException(noSuchSegment.toString()));
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
        reconnect(null);
    }

    private void reconnect(Exception e) {
        log.warn("Connection failure. Will Reconnect.", e);
        ClientConnection newConnection = connectionFactory.establishConnection(endpoint, responseProcessor);
        ClientConnection oldConnection = connection.getAndSet(newConnection);
        if (oldConnection != null) {
            oldConnection.drop();
        }
        for (Iterator<Entry<Long, CompletableFuture<SegmentRead>>> iterator = outstandingRequests.entrySet()
            .iterator(); iterator.hasNext();) {
            Entry<Long, CompletableFuture<SegmentRead>> read = iterator.next();
            read.getValue().completeExceptionally(e);
            iterator.remove();
        }
    }

    @Override
    public void close() {
        ClientConnection c = connection.getAndSet(null);
        if (c != null) {
            c.drop();
        }
    }

    @Override
    public CompletableFuture<SegmentRead> read(long offset, int length) {
        ClientConnection c = connection.get();
        Preconditions.checkState(c != null, "Not Connected");
        CompletableFuture<SegmentRead> future = new CompletableFuture<>();
        outstandingRequests.put(offset, future);
        try {
            c.send(new ReadSegment(segment, offset, length));
        } catch (ConnectionFailedException e) {
            reconnect(e);
            // While this does not provide backoff, it waits for the next read call to send the
            // request again.
        }
        return future;
    }

}
