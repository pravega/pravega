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
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.WireCommand;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.metrics.ClientMetricKeys.CLIENT_APPEND_LATENCY;
import static io.pravega.shared.segment.StreamSegmentNameUtils.segmentTags;

@Slf4j
public class ClientConnectionImpl implements ClientConnection {

    @Getter
    private final String connectionName;
    @Getter
    private final int flowId;
    @VisibleForTesting
    @Getter
    private final FlowHandler nettyHandler;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ClientConnectionImpl(String connectionName, int flowId, FlowHandler nettyHandler) {
        this.connectionName = connectionName;
        this.flowId = flowId;
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
        Timer timer = new Timer();
        checkClientConnectionClosed();
        nettyHandler.setRecentMessage();
        Futures.getAndHandleExceptions(nettyHandler.getChannel().writeAndFlush(append), ConnectionFailedException::new);
        nettyHandler.getMetricNotifier()
                    .updateSuccessMetric(CLIENT_APPEND_LATENCY, segmentTags(append.getSegment(), append.getWriterId().toString()),
                                         timer.getElapsedMillis());
    }

    @Override
    public void sendAsync(WireCommand cmd, CompletedCallback callback) {
        Channel channel = null;
        try {
            checkClientConnectionClosed();
            nettyHandler.setRecentMessage();

            channel = nettyHandler.getChannel();
            log.debug("Write and flush message {} on channel {}", cmd, channel);
            channel.writeAndFlush(cmd)
                   .addListener((Future<? super Void> f) -> {
                       if (f.isSuccess()) {
                           callback.complete(null);
                       } else {
                           callback.complete(new ConnectionFailedException(f.cause()));
                       }
                   });
        } catch (ConnectionFailedException cfe) {
            log.debug("ConnectionFailedException observed when attempting to write WireCommand {} ", cmd);
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
