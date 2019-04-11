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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.pravega.client.ClientConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectionPoolImpl implements ConnectionPool {

    private static final Comparator<Connection> COMPARATOR = new Comparator<Connection>() {
        @Override
        public int compare(Connection c1, Connection c2) {
            int v1 = c1.getSessionCount().orElse(Integer.MAX_VALUE);
            int v2 = c2.getSessionCount().orElse(Integer.MAX_VALUE);
            return Integer.compare(v1, v2);
        }
    }; 
    private final ClientConfig clientConfig;
    private final EventLoopGroup group;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    @GuardedBy("$lock")
    private final Map<PravegaNodeUri, List<Connection>> connectionMap = new HashMap<>();

    public ConnectionPoolImpl(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        // EventLoopGroup objects are expensive, do not create a new one for every connection.
        this.group = getEventLoopGroup();
    }

    @Override
    @Synchronized
    public CompletableFuture<ClientConnection> getClientConnection(Flow flow, PravegaNodeUri location, ReplyProcessor rp) {
        Preconditions.checkNotNull(flow, "Flow");
        Preconditions.checkNotNull(location, "Location");
        Preconditions.checkNotNull(rp, "ReplyProcessor");
        Exceptions.checkNotClosed(closed.get(), this);

        final List<Connection> connectionList = connectionMap.getOrDefault(location, new ArrayList<>());

        // remove connections for which the underlying network connection is disconnected.
        List<Connection> prunedConnectionList = connectionList.stream().filter(connection -> {
            // Filter out Connection objects which have been completed exceptionally or have been disconnected.
            CompletableFuture<SessionHandler> r = connection.getSessionHandler();
            return !r.isDone() || (Futures.isSuccessful(r) && r.join().isConnectionEstablished());
        }).collect(Collectors.toList());
        log.debug("List of connections to {} that can be used: {}", location, prunedConnectionList);

        // Choose the connection with the least number of sessions.
        Optional<Connection> suggestedConnection = prunedConnectionList.stream().min(COMPARATOR);

        final Connection connection;
        if (suggestedConnection.isPresent() && (prunedConnectionList.size() >= clientConfig.getMaxConnectionsPerSegmentStore() || isUnused(suggestedConnection.get()))) {
            log.debug("Reusing connection: {}", suggestedConnection.get());
            Connection oldConnection = suggestedConnection.get();
            prunedConnectionList.remove(oldConnection);
            connection = new Connection(oldConnection.getUri(), oldConnection.getSessionHandler());
        } else {
            // create a new connection.
            log.debug("Creating a new connection to {}", location);
            CompletableFuture<SessionHandler> sessionHandlerFuture = establishConnection(location);
            connection = new Connection(location, sessionHandlerFuture);
        }
        prunedConnectionList.add(connection);
        connectionMap.put(location, prunedConnectionList);
        return connection.getSessionHandler().thenApply(sessionHandler -> sessionHandler.createSession(flow, rp));
    }

    @Override
    public CompletableFuture<ClientConnection> getClientConnection(PravegaNodeUri location, ReplyProcessor rp) {
        Preconditions.checkNotNull(location, "Location");
        Preconditions.checkNotNull(rp, "ReplyProcessor");
        Exceptions.checkNotClosed(closed.get(), this);
        
        // create a new connection.
        CompletableFuture<SessionHandler> sessionHandlerFuture = establishConnection(location);
        Connection connection = new Connection(location, sessionHandlerFuture);
        return connection.getSessionHandler().thenApply(sessionHandler -> sessionHandler.createConnectionWithSessionDisabled(rp));
    }

    private boolean isUnused(Connection connection) {
        return connection.getSessionCount().isPresent() && connection.getSessionCount().get() == 0;
    }

    /**
     * Used only for testing.
     */
    @VisibleForTesting
    public void pruneUnusedConnections() {
        for (List<Connection> connections : connectionMap.values()) {
            for (Iterator<Connection> iterator = connections.iterator(); iterator.hasNext();) {
                Connection connection = iterator.next();
                if (isUnused(connection)) {
                    connection.getSessionHandler().join().close();
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public int getActiveChannelCount() {
        return getActiveChannels().size();
    }

    @VisibleForTesting
    public List<Channel> getActiveChannels() {
        return this.channelGroup.stream().filter(Channel::isActive)
                                .peek(ch -> log.debug("Channel with id {} localAddress {} and remoteAddress {} is active.", ch.id(),
                                                      ch.localAddress(), ch.remoteAddress()))
                                .collect(Collectors.toList());
    }

    /**
     * Establish a new connection to the Pravega Node.
     * @param location The Pravega Node Uri
     * @return A future, which completes once the connection has been established, returning a SessionHandler that can be used to create
     * sessions on the connection.
     */
    private CompletableFuture<SessionHandler> establishConnection(PravegaNodeUri location) {
        final AppendBatchSizeTracker batchSizeTracker = new AppendBatchSizeTrackerImpl();
        final SessionHandler handler = new SessionHandler(location.getEndpoint(), batchSizeTracker);
        final Bootstrap b = getNettyBootstrap().handler(getChannelInitializer(location, batchSizeTracker, handler));

        // Initiate Connection.
        final CompletableFuture<SessionHandler> connectionComplete = new CompletableFuture<>();
        try {
            b.connect(location.getEndpoint(), location.getPort()).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    //since ChannelFuture is complete future.channel() is not a blocking call.
                    Channel ch = future.channel();
                    log.debug("Connect operation completed for channel:{}, local address:{}, remote address:{}",
                              ch.id(), ch.localAddress(), ch.remoteAddress());
                    channelGroup.add(ch); // Once a channel is closed the channel group implementation removes it.
                    connectionComplete.complete(handler);
                } else {
                    connectionComplete.completeExceptionally(new ConnectionFailedException(future.cause()));
                }
            });
        } catch (Exception e) {
            connectionComplete.completeExceptionally(new ConnectionFailedException(e));
        }

        final CompletableFuture<Void> channelRegisteredFuture = new CompletableFuture<>(); //to track channel registration.
        handler.completeWhenRegistered(channelRegisteredFuture);

        return connectionComplete.thenCombine(channelRegisteredFuture, (sessionHandler, v) -> sessionHandler);
    }

    /**
     * Create {@link Bootstrap}.
     */
    private Bootstrap getNettyBootstrap() {
        Bootstrap b = new Bootstrap();
        b.group(group)
         .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true);
        return b;
    }

    /**
     * Create a Channel Initializer which is to to setup {@link ChannelPipeline}.
     */
    private ChannelInitializer<SocketChannel> getChannelInitializer(final PravegaNodeUri location,
                                                                    final AppendBatchSizeTracker batchSizeTracker,
                                                                    final SessionHandler handler) {
        final SslContext sslCtx = getSslContext();

        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    SslHandler sslHandler = sslCtx.newHandler(ch.alloc(), location.getEndpoint(), location.getPort());

                    if (clientConfig.isValidateHostName()) {
                        SSLEngine sslEngine = sslHandler.engine();
                        SSLParameters sslParameters = sslEngine.getSSLParameters();
                        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
                        sslEngine.setSSLParameters(sslParameters);
                    }
                    p.addLast(sslHandler);
                }
                p.addLast(
                        new ExceptionLoggingHandler(location.getEndpoint()),
                        new CommandEncoder(batchSizeTracker),
                        new LengthFieldBasedFrameDecoder(WireCommands.MAX_WIRECOMMAND_SIZE, 4, 4),
                        new CommandDecoder(),
                        handler);
            }
        };
    }

    /**
     * Obtain {@link SslContext} based on {@link ClientConfig}.
     */
    private SslContext getSslContext() {
        final SslContext sslCtx;
        if (clientConfig.isEnableTls()) {
            try {
                SslContextBuilder clientSslCtxBuilder = SslContextBuilder.forClient();
                if (Strings.isNullOrEmpty(clientConfig.getTrustStore())) {
                    clientSslCtxBuilder = clientSslCtxBuilder.trustManager(FingerprintTrustManagerFactory
                                                                                   .getInstance(FingerprintTrustManagerFactory.getDefaultAlgorithm()));
                    log.debug("SslContextBuilder was set to an instance of {}", FingerprintTrustManagerFactory.class);
                } else {
                    clientSslCtxBuilder = SslContextBuilder.forClient()
                                                           .trustManager(new File(clientConfig.getTrustStore()));
                }
                sslCtx = clientSslCtxBuilder.build();
            } catch (SSLException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        } else {
            sslCtx = null;
        }
        return sslCtx;
    }

    private EventLoopGroup getEventLoopGroup() {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup();
        } else {
            log.warn("Epoll not available. Falling back on NIO.");
            return new NioEventLoopGroup();
        }
    }

    @Override
    public void close() {
        log.info("Shutting down connection pool");
        if (closed.compareAndSet(false, true)) {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
    }
}
