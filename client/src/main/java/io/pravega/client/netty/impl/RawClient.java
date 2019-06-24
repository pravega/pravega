/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.pravega.auth.AuthenticationException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawClient implements AutoCloseable {
    private static final Duration DEFAULT_RAWCLIENT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final CompletableFuture<ClientConnection> connection;
    private final Segment segmentId;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<Reply>> requests = new HashMap<>();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @Getter
    private final Flow flow = Flow.create();
    private final ConnectionFactory connectionFactory;

    private final class ResponseProcessor extends FailingReplyProcessor {

        @Override
        public void process(Reply reply) {
            if (reply instanceof Hello) {
                Hello hello = (Hello) reply;
                log.info("Received hello: {}", hello);
                if (hello.getLowVersion() > WireCommands.WIRE_VERSION || hello.getHighVersion() < WireCommands.OLDEST_COMPATIBLE_VERSION) {
                    closeConnection(new IllegalStateException("Incompatible wire protocol versions " + hello));
                }
            } else if (reply instanceof WireCommands.WrongHost) {
                closeConnection(new ConnectionFailedException(reply.toString()));
            } else {
                log.debug("Received reply {}", reply);
                reply(reply);
            }
        }

        @Override
        public void connectionDropped() {
            closeConnection(new ConnectionFailedException());
        }

        @Override
        public void processingFailure(Exception error) {
            log.warn("Processing failure: ", error);
            closeConnection(error);
        }

        @Override
        public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
            log.warn("Auth token failure: {}", authTokenCheckFailed);
            closeConnection(new AuthenticationException(authTokenCheckFailed.toString()));
        }
    }

    public RawClient(PravegaNodeUri uri, ConnectionFactory connectionFactory) {
        this.segmentId = null;
        this.connectionFactory = connectionFactory;
        this.connection = connectionFactory.establishConnection(flow, uri, responseProcessor);
        Futures.exceptionListener(connection, e -> closeConnection(e));
    }

    public RawClient(Controller controller, ConnectionFactory connectionFactory, Segment segmentId) {
        this.segmentId = segmentId;
        this.connectionFactory = connectionFactory;
        this.connection = controller.getEndpointForSegment(segmentId.getScopedName())
                                    .thenCompose((PravegaNodeUri uri) -> connectionFactory.establishConnection(flow, uri, responseProcessor));
        Futures.exceptionListener(connection, e -> closeConnection(e));
    }

    private void reply(Reply reply) {
        CompletableFuture<Reply> future;
        synchronized (lock) {
            future = requests.remove(reply.getRequestId());
        }
        if (future != null) {
            future.complete(reply);
        }
    }

    private void closeConnection(Throwable exceptionToInflightRequests) {
        if (closed.get() || exceptionToInflightRequests instanceof ConnectionClosedException) {
            log.debug("Closing connection with exception {}", exceptionToInflightRequests);
        } else {
            log.warn("Closing connection with exception: {}", exceptionToInflightRequests);
        }
        if (closed.compareAndSet(false, true)) {
            connection.thenAccept(c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    log.warn("Exception tearing down connection: ", e);
                }
            });
        }
        List<CompletableFuture<Reply>> requestsToFail;
        synchronized (lock) {
            requestsToFail = new ArrayList<>(requests.values());
            requests.clear();
        }
        for (CompletableFuture<Reply> request : requestsToFail) {
            request.completeExceptionally(exceptionToInflightRequests);
        }
    }

    public <T extends Request & WireCommand> CompletableFuture<Reply> sendRequest(long requestId, T request) {
        return sendRequest(requestId, request, DEFAULT_RAWCLIENT_REQUEST_TIMEOUT);
    }

    public <T extends Request & WireCommand> CompletableFuture<Reply> sendRequest(long requestId, T request,
                                                                                  Duration timeout) {
        return connection.thenCompose(c -> {
            log.debug("Sending request: {}", request);
            CompletableFuture<Reply> reply = new CompletableFuture<>();
            synchronized (lock) {
                requests.put(requestId, reply);
            }
            CompletableFuture<Void> timer = Futures.futureWithTimeout(timeout, connectionFactory.getInternalExecutor());
            Futures.onTimeout(timer, ex -> {
                synchronized (lock) {
                    requests.remove(requestId);
                }
                reply.completeExceptionally(ex);
                closeConnection(ex);
            });
            c.sendAsync(request, cfe -> {
                if (cfe != null) {
                    synchronized (lock) {
                        requests.remove(requestId);
                    }
                    reply.completeExceptionally(cfe);
                    closeConnection(cfe);
                }
                timer.completeExceptionally(new InterruptedException());
            });
            return reply;
        });
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closeConnection(new ConnectionClosedException());
    }
}
