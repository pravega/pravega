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
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.PromiseCombiner;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.Flush;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientConnectionImpl implements ClientConnection {

    @Getter
    private final String connectionName;
    @Getter
    private final int flowId;
    @VisibleForTesting
    @Getter
    private final FlowHandler nettyHandler;
    private final AppendBatchSizeTracker batchSizeTracker;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ClientConnectionImpl(String connectionName, int flowId, AppendBatchSizeTracker batchSizeTracker,
                                FlowHandler nettyHandler) {
        this.connectionName = connectionName;
        this.flowId = flowId;
        this.batchSizeTracker = batchSizeTracker;
        this.nettyHandler = nettyHandler;
    }

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        checkClientConnectionClosed();
        nettyHandler.setRecentMessage();
        Futures.getAndHandleExceptions(nettyHandler.getChannel().writeAndFlush(cmd), ConnectionFailedException::new);
    }

    @Override
    public void send(Append append) throws ConnectionFailedException {
        checkClientConnectionClosed();
        nettyHandler.setRecentMessage();
        batchSizeTracker.recordAppend(append.getEventNumber(), append.getData().readableBytes());
        Futures.getAndHandleExceptions(nettyHandler.getChannel().writeAndFlush(append), ConnectionFailedException::new);
    }


    @Override
    public void send(Flush flush) throws ConnectionFailedException {
        checkClientConnectionClosed();
        Futures.getAndHandleExceptions(nettyHandler.getChannel().writeAndFlush(flush), ConnectionFailedException::new);
    }


    @Override
    public void sendAsync(WireCommand cmd, CompletedCallback callback) {
        Channel channel = null;
        try {
            checkClientConnectionClosed();
            nettyHandler.setRecentMessage();

            channel = nettyHandler.getChannel();
            channel.writeAndFlush(cmd)
                   .addListener((Future<? super Void> f) -> {
                       if (f.isSuccess()) {
                           callback.complete(null);
                       } else {
                           callback.complete(new ConnectionFailedException(f.cause()));
                       }
                   });
        } catch (ConnectionFailedException cfe) {
            log.debug("ConnectionFaileException observed when attempting to write WireCommand {} ", cmd);
            callback.complete(cfe);
        } catch (Exception e) {
            log.warn("Exception while attempting to write WireCommand {} on netty channel {}", cmd, channel);
            callback.complete(new ConnectionFailedException(e));
        }
    }

    @Override
    public void sendAsync(List<Append> appends, CompletedCallback callback) {
        Channel ch;
        try {
            checkClientConnectionClosed();
            nettyHandler.setRecentMessage();
            ch = nettyHandler.getChannel();
        } catch (ConnectionFailedException e) {
            callback.complete(new ConnectionFailedException("Connection to " + connectionName + " is not established."));
            return;
        }
        PromiseCombiner combiner = new PromiseCombiner();
        for (Append append : appends) {
            batchSizeTracker.recordAppend(append.getEventNumber(), append.getData().readableBytes());
            combiner.add(ch.write(append));
        }
        ch.flush();
        ChannelPromise promise = ch.newPromise();
        promise.addListener(future -> {
            Throwable cause = future.cause();
            callback.complete(cause == null ? null : new ConnectionFailedException(cause));
        });
        combiner.finish(promise);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            nettyHandler.closeFlow(this);
        }
    }

    private void checkClientConnectionClosed() throws ConnectionFailedException {
        if (closed.get()) {
            log.error("ClientConnection to {} with flow id {} is already closed", connectionName, flowId);
            throw new ConnectionFailedException("Client connection already closed for flow " + flowId);
        }
    }

}
