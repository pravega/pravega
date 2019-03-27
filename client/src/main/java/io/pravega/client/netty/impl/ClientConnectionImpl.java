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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.PromiseCombiner;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.WireCommand;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientConnectionImpl implements ClientConnection {
    private final String connectionName;
    private final SessionHandler nettyHandler;
    private final AppendBatchSizeTracker batchSizeTracker;

    public ClientConnectionImpl(String connectionName,  AppendBatchSizeTracker batchSizeTracker, SessionHandler nettyHandler) {
        this.connectionName = connectionName;
        this.batchSizeTracker = batchSizeTracker;
        this.nettyHandler = nettyHandler;
    }

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        nettyHandler.setRecentMessage();
        Futures.getAndHandleExceptions(nettyHandler.getChannel().writeAndFlush(cmd), ConnectionFailedException::new);
    }

    @Override
    public void send(Append append) throws ConnectionFailedException {
        nettyHandler.setRecentMessage();
        batchSizeTracker.recordAppend(append.getEventNumber(), append.getData().readableBytes());
        Futures.getAndHandleExceptions(nettyHandler.getChannel().writeAndFlush(append), ConnectionFailedException::new);
    }

    @Override
    public void sendAsync(WireCommand cmd, CompletedCallback callback) {
        nettyHandler.setRecentMessage();
        try {
            Channel channel = nettyHandler.getChannel();
            channel.writeAndFlush(cmd)
                   .addListener((Future<? super Void> f) -> {
                       if (f.isSuccess()) {
                           callback.complete(null);
                       } else {
                           callback.complete(new ConnectionFailedException(f.cause()));
                       }
                   });
        } catch (ConnectionFailedException cfe) {
            callback.complete(cfe);
        } catch (RuntimeException e) {
            callback.complete(new ConnectionFailedException(e));
        }
    }

    @Override
    public void sendAsync(List<Append> appends, CompletedCallback callback) {
        nettyHandler.setRecentMessage();
        Channel ch;
        try {
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
        promise.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                Throwable cause = future.cause();
                callback.complete(cause == null ? null : new ConnectionFailedException(cause));
            }
        });
        combiner.finish(promise);
    }

    @Override
    public void close() {
        log.debug("Closing session connection {}", connectionName);
        nettyHandler.closeSession(this);
    }

}
