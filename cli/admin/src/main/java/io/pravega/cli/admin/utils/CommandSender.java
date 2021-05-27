package io.pravega.cli.admin.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.util.Config;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.SneakyThrows;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Used by the Controller for interacting with Segment Store. Think of this class as a 'SegmentStoreHelper'.
 */
public class CommandSender implements AutoCloseable {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(io.pravega.controller.server.SegmentHelper.class));

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_SUCCESS_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
                    .put(WireCommands.FlushToStorage.class, ImmutableSet.of(WireCommands.FlushedStorage.class))
                    .build();

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_FAILING_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
                    .build();

    private final HostControllerStore hostStore;
    private final ConnectionPool connectionPool;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<Duration> timeout;

    public CommandSender(final ConnectionPool connectionPool, HostControllerStore hostStore,
                         ScheduledExecutorService executorService) {
        this.connectionPool = connectionPool;
        this.hostStore = hostStore;
        this.executorService = executorService;
        this.timeout = new AtomicReference<>(Duration.ofSeconds(Config.REQUEST_TIMEOUT_SECONDS_SEGMENT_STORE));
    }

    @VisibleForTesting
    void setTimeout(Duration duration) {
        timeout.set(duration);
    }

    public CompletableFuture<WireCommands.FlushedStorage> flushToStorage(PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.FLUSH_TO_STORAGE;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.FlushToStorage request = new WireCommands.FlushToStorage(delegationToken,
                requestId);
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, null, WireCommands.FlushToStorage.class, type);
                    assert r instanceof WireCommands.FlushedStorage;
                    return (WireCommands.FlushedStorage) r;
                });
    }

    private void closeConnection(Reply reply, RawClient client, long callerRequestId) {
        log.debug(callerRequestId, "Closing connection as a result of receiving: flowId: {}: reply: {}",
                reply.getRequestId(), reply);
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn(callerRequestId, "Exception tearing down connection: ", e);
            }
        }
    }

    private <T extends Request & WireCommand> CompletableFuture<Reply> sendRequest(RawClient connection, long clientRequestId,
                                                                                   T request) {
        log.trace(clientRequestId, "Sending request to segment store with: flowId: {}: request: {}",
                request.getRequestId(), request);

        CompletableFuture<Reply> future = Futures.futureWithTimeout(() -> connection.sendRequest(request.getRequestId(), request),
                timeout.get(), "request", executorService);
        return future.exceptionally(e -> {
            processAndRethrowException(clientRequestId, request, e);
            return null;
        });
    }

    @VisibleForTesting
    <T extends Request & WireCommand> void processAndRethrowException(long callerRequestId, T request, Throwable e) {
        Throwable unwrap = Exceptions.unwrap(e);
        WireCommandFailedException ex = null;
        if (unwrap instanceof ConnectionFailedException || unwrap instanceof ConnectionClosedException) {
            log.warn(callerRequestId, "Connection dropped {}", request.getRequestId());
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
        } else if (unwrap instanceof AuthenticationException) {
            log.warn(callerRequestId, "Authentication Exception {}", request.getRequestId());
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.AuthFailed);
        } else if (unwrap instanceof TokenExpiredException) {
            log.warn(callerRequestId, "Token expired {}", request.getRequestId());
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.AuthFailed);
        } else if (unwrap instanceof TimeoutException) {
            log.warn(callerRequestId, "Request timed out. {}", request.getRequestId());
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
        } else {
            log.error(callerRequestId, "Request failed {}", request.getRequestId(), e);
            throw new CompletionException(e);
        }
    }

    /**
     * This method handle reply returned from RawClient.sendRequest.
     *
     * @param callerRequestId     request id issues by the client
     * @param reply               actual reply received
     * @param client              RawClient for sending request
     * @param qualifiedStreamSegmentName StreamSegmentName
     * @param requestType         request which reply need to be transformed
     * @param type                WireCommand for this request
     * @return true if reply is in the expected reply set for the given requestType or throw exception.
     */
    @SneakyThrows(ConnectionFailedException.class)
    private void handleReply(long callerRequestId,
                             Reply reply,
                             RawClient client,
                             String qualifiedStreamSegmentName,
                             Class<? extends Request> requestType,
                             WireCommandType type) {
        closeConnection(reply, client, callerRequestId);
        Set<Class<? extends Reply>> expectedReplies = EXPECTED_SUCCESS_REPLIES.get(requestType);
        Set<Class<? extends Reply>> expectedFailingReplies = EXPECTED_FAILING_REPLIES.get(requestType);
        if (expectedReplies != null && expectedReplies.contains(reply.getClass())) {
            log.debug(callerRequestId, "{} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
        } else if (expectedFailingReplies != null && expectedFailingReplies.contains(reply.getClass())) {
            log.debug(callerRequestId, "{} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
            if (reply instanceof WireCommands.NoSuchSegment) {
                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist);
            } else if (reply instanceof WireCommands.TableSegmentNotEmpty) {
                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.TableSegmentNotEmpty);
            } else if (reply instanceof WireCommands.TableKeyDoesNotExist) {
                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist);
            } else if (reply instanceof WireCommands.TableKeyBadVersion) {
                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyBadVersion);
            }
        } else if (reply instanceof WireCommands.AuthTokenCheckFailed) {
            log.warn(callerRequestId, "Auth Check Failed {} {} {} {} with error code {}.",
                    requestType.getSimpleName(), qualifiedStreamSegmentName, reply.getClass().getSimpleName(),
                    reply.getRequestId(), ((WireCommands.AuthTokenCheckFailed) reply).getErrorCode());
            throw new WireCommandFailedException(new AuthenticationException(reply.toString()),
                    type, WireCommandFailedException.Reason.AuthFailed);
        } else if (reply instanceof WireCommands.WrongHost) {
            log.warn(callerRequestId, "Wrong Host {} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
            throw new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost);
        } else {
            log.error(callerRequestId, "Unexpected reply {} {} {} {}.", requestType.getSimpleName(),
                    qualifiedStreamSegmentName, reply.getClass().getSimpleName(), reply.getRequestId());

            throw new ConnectionFailedException("Unexpected reply of " + reply + " when expecting one of "
                    + expectedReplies.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }
    }

    @Override
    public void close() {
        connectionPool.close();
    }
}
