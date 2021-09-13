/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.connection.impl;

import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.ErrorMessage;
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

    private final CompletableFuture<ClientConnection> connection;
    private final Segment segmentId;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<Reply>> requests = new HashMap<>();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @Getter
    private final Flow flow = Flow.create();

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
            } else if (reply instanceof WireCommands.ErrorMessage) {
                ErrorMessage errorMessage = (ErrorMessage) reply;
                log.info("Received an errorMessage containing an unhandled {} on segment {}",
                        errorMessage.getErrorCode().getExceptionType().getSimpleName(),
                        errorMessage.getSegment());
                closeConnection(errorMessage.getThrowableException());
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
            log.warn("Processing failure on segment {}", segmentId, error);
            closeConnection(error);
        }

        @Override
        public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
            log.warn("Auth token check failed on segment {} with {}", segmentId, authTokenCheckFailed);
            if (authTokenCheckFailed.isTokenExpired()) {
                closeConnection(new TokenExpiredException(authTokenCheckFailed.getServerStackTrace()));
            } else {
                closeConnection(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        }

    }

    public RawClient(PravegaNodeUri uri, ConnectionPool connectionPool) {
        this.segmentId = null;
        this.connection = new CompletableFuture<>();
        this.connection.exceptionally(e -> {
            log.warn("Exception observed while attempting to obtain a connection to segment store {}", uri, e);
            return null;
        });
        Futures.exceptionListener(connection, this::closeConnection);
        connectionPool.getClientConnection(flow, uri, responseProcessor, this.connection);
    }

    public RawClient(Controller controller, ConnectionPool connectionPool, Segment segmentId) {
        this.segmentId = segmentId;
        this.connection = new CompletableFuture<>();
        this.connection.exceptionally(e -> {
            log.warn("Exception observed while attempting to obtain a connection to segment {}", segmentId, e);
            return null;
        });
        Futures.exceptionListener(connection, this::closeConnection);
        controller.getEndpointForSegment(segmentId.getScopedName())
                                    .thenAccept((PravegaNodeUri uri) -> connectionPool.getClientConnection(flow, uri, responseProcessor, this.connection))
                                    .exceptionally(e -> {
                                        this.connection.completeExceptionally(e);
                                        return null;
                                    });
    }

    private void reply(Reply reply) {
        CompletableFuture<Reply> future;
        synchronized (lock) {
            future = requests.remove(reply.getRequestId());
        }
        if (future != null) {
            future.complete(reply);
        } else {
            if (reply.isFailure()) {
                log.info("Could not find any matching request for error {}. Closing connection.", reply);
                closeConnection(new ConnectionFailedException("Unexpected reply from server: " + reply));
            } else {                
                log.info("Could not find any matching request for {}. Ignoring.", reply);
            }
        }
    }

    private void closeConnection(Throwable exceptionToInflightRequests) {

        if (closed.get() || exceptionToInflightRequests instanceof ConnectionClosedException) {
            log.debug("Closing connection as requested");
        } else {
            log.warn("Closing connection to segment {} with exception", segmentId, exceptionToInflightRequests);
        }
        if (closed.compareAndSet(false, true)) {
            connection.thenAccept(c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    log.warn("Exception tearing down connection {} : ", c, e);
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
        return connection.thenCompose(c -> {
            log.debug("Sending request: {}", request);
            CompletableFuture<Reply> reply = new CompletableFuture<>();
            synchronized (lock) {
                requests.put(requestId, reply);
            }
            try {
                c.send(request);
            } catch (ConnectionFailedException cfe) {
                synchronized (lock) {
                    requests.remove(requestId);
                }
                reply.completeExceptionally(cfe);
                closeConnection(cfe);
            }
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
