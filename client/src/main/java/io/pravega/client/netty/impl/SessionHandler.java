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

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.ScheduledFuture;
import io.pravega.client.Session;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ReusableFutureLatch;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionHandler extends ChannelInboundHandlerAdapter implements AutoCloseable {

    public static final int SESSION_DISABLED = -1;
    private final String connectionName;
    private final AtomicReference<Channel> channel = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> keepAliveFuture = new AtomicReference<>();
    private final AtomicBoolean recentMessage = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AppendBatchSizeTracker batchSizeTracker;
    private final ReusableFutureLatch<Void> registeredFutureLatch = new ReusableFutureLatch<>();
    private final ConcurrentHashMap<Integer, ReplyProcessor> sessionIdReplyProcessorMap = new ConcurrentHashMap<>();
    private final AtomicBoolean disableSession = new AtomicBoolean(false);

    public SessionHandler(String connectionName, AppendBatchSizeTracker batchSizeTracker) {
        this.connectionName = connectionName;
        this.batchSizeTracker = batchSizeTracker;
    }

    /**
     * Create a session on existing connection.
     * @param session Session.
     * @param rp ReplyProcessor for the specified session.
     * @return Client Connection object.
     */
    public ClientConnection createSession(final Session session, final ReplyProcessor rp) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkState(!disableSession.get(), "Ensure sessions are enabled");
        log.info("Creating Session: {} for Endpoint: {}. The current Channel is {}.", session.getSessionId(), connectionName,
                 channel.get());
        if (sessionIdReplyProcessorMap.put(session.getSessionId(), rp) != null) {
            throw new IllegalArgumentException("Multiple sessions cannot be created with the same Session id " + session.getSessionId());
        }
        return new ClientConnectionImpl(connectionName, session.getSessionId(), batchSizeTracker, this);
    }

    /**
     * Create a {@link ClientConnection} where sessions are disabled. This implies that there is only one session on the underlying
     * network connection.
     * @param rp  The ReplyProcessor.
     * @return Client Connection object.
     */
    public ClientConnection createConnectionWithSessionDisabled(final ReplyProcessor rp) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkState(!disableSession.getAndSet(true), "Sessions are disabled, incorrect usage patter");
        log.info("Creating a new connection with session disabled for Endpoint: {}. The current Channel is {}.", connectionName,
                 channel.get());
        sessionIdReplyProcessorMap.put(SESSION_DISABLED, rp);
        return new ClientConnectionImpl(connectionName, SESSION_DISABLED, batchSizeTracker, this);
    }

    /**
     * Close a session. This is invoked when the ClientConnection is closed.
     * @param clientConnection Client Connection.
     */
    public void closeSession(ClientConnection clientConnection) {
        final ClientConnectionImpl clientConnectionImpl = (ClientConnectionImpl) clientConnection;
        int session = clientConnectionImpl.getSession();
        log.info("Closing Session: {} for Endpoint: {}", session, clientConnectionImpl.getConnectionName());
        sessionIdReplyProcessorMap.remove(session);
    }

    /**
     * Check the current status of Connection.
     * @return True if the connection is established.
     */
    public boolean isConnectionEstablished() {
        return channel.get() != null;
    }

    /**
     * Fetch the netty channel. If {@link Channel} is null then throw a ConnectionFailedException.
     * @return  The current {@link Channel}
     * @throws ConnectionFailedException Throw if connection is not established.
     */
    Channel getChannel() throws ConnectionFailedException {
        Channel ch = channel.get();
        if (ch == null) {
            throw new ConnectionFailedException("Connection to " + connectionName + " is not established.");
        }
        return ch;
    }

    /**
     * Set the Recent Message flag. This is used to avoid sending redundant KeepAlives over the connection.
     */
    void setRecentMessage() {
        recentMessage.set(true);
    }

    /**
     * This function completes the input future when the channel is registered.
     *
     * @param future CompletableFuture which will be completed once the channel is registered.
     */
    void completeWhenRegistered(final CompletableFuture<Void> future) {
        Preconditions.checkNotNull(future, "future");
        registeredFutureLatch.register(future);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        Channel ch = ctx.channel();
        channel.set(ch);
        log.info("Connection established with Endpoint: {} on ChannelId: {}.", connectionName, ch);
        ch.write(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION), ch.voidPromise());
        registeredFutureLatch.release(null); //release all futures waiting for channel registration to complete.
        // WireCommands.KeepAlive messages are sent for every network connection to a SegmentStore.
        ScheduledFuture<?> old = keepAliveFuture.getAndSet(ch.eventLoop().scheduleWithFixedDelay(new KeepAliveTask(), 20, 10, TimeUnit.SECONDS));
        if (old != null) {
            old.cancel(false);
        }
    }

    /**
     * Invoke all the {@link ReplyProcessor#connectionDropped()} for all the registered sessions once the
     * connection is disconnected.
     *
     * @see io.netty.channel.ChannelInboundHandler#channelUnregistered(ChannelHandlerContext)
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        registeredFutureLatch.reset();
        ScheduledFuture<?> future = keepAliveFuture.get();
        if (future != null) {
            future.cancel(false);
        }
        channel.set(null);
        sessionIdReplyProcessorMap.forEach((sessionId, rp) -> {
            rp.connectionDropped();
            log.debug("Connection dropped for session id : {}", sessionId);
        });
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Reply cmd = (Reply) msg;
        log.debug(connectionName + " processing reply: {} with session {}.", cmd, Session.from(cmd.getRequestId()));

        if (cmd instanceof WireCommands.Hello) {
            sessionIdReplyProcessorMap.forEach((sessionId, rp) -> rp.hello((WireCommands.Hello) cmd));
            return;
        }

        if (cmd instanceof WireCommands.DataAppended) {
            batchSizeTracker.recordAck(((WireCommands.DataAppended) cmd).getEventNumber());
        }
        // Obtain ReplyProcessor and process the reply.
        getReplyProcessor(cmd).ifPresent(processor -> {
            try {
                processor.process(cmd);
            } catch (Throwable e) {
                processor.processingFailure(new ExecutionException(e));
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        sessionIdReplyProcessorMap.forEach((sessionId, rp) -> {
            rp.processingFailure(new ConnectionFailedException(cause));
            log.debug("Exception observed for session id : {}", sessionId);
        });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Channel ch = channel.get();
            if (ch != null) {
                log.debug("Closing channel:{} ", ch);
                final int openSessionCount = sessionIdReplyProcessorMap.size();
                if (openSessionCount != 0) {
                    log.warn("{} sessions are not closed", openSessionCount);
                }
                ch.close();
            }
        }
    }

    private final class KeepAliveTask implements Runnable {
        @Override
        public void run() {
            try {
                if (!recentMessage.getAndSet(false)) {
                    recentMessage.set(true);
                    Futures.getAndHandleExceptions(getChannel().writeAndFlush(new WireCommands.KeepAlive()), ConnectionFailedException::new);
                }
            } catch (Exception e) {
                log.warn("Keep alive failed, killing connection {} due to {} ", connectionName, e.getMessage());
                close();
            }
        }
    }

    private Optional<ReplyProcessor> getReplyProcessor(Reply cmd) {
        int sessionId = disableSession.get() ? SESSION_DISABLED : Session.from(cmd.getRequestId()).getSessionId();
        final ReplyProcessor processor = sessionIdReplyProcessorMap.get(sessionId);
        if (processor == null) {
            log.warn("No ReplyProcessor found for the provided sessionId {}. Ignoring response", sessionId);
        }
        return Optional.ofNullable(processor);
    }
}
