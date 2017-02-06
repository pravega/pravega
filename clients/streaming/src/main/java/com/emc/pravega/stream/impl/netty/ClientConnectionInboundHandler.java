/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.stream.impl.netty;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.AppendBatchSizeTracker;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.Reply;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommand;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.google.common.base.Preconditions;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Bridges the gap between netty and the ReplyProcessor on the client.
 */
@Slf4j
public class ClientConnectionInboundHandler extends ChannelInboundHandlerAdapter implements ClientConnection {

    private final String connectionName;
    private final ReplyProcessor processor;
    private final AtomicReference<Channel> channel = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> keepAliveFuture = new AtomicReference<>();
    private final AtomicBoolean recentMessage = new AtomicBoolean(false);
    private final AppendBatchSizeTracker batchSizeTracker;

    ClientConnectionInboundHandler(String connectionName, ReplyProcessor processor, AppendBatchSizeTracker batchSizeTracker) {
        Preconditions.checkNotNull(processor);
        Preconditions.checkNotNull(batchSizeTracker);
        this.connectionName = connectionName;
        this.processor = processor;
        this.batchSizeTracker = batchSizeTracker;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        Channel c = ctx.channel();
        channel.set(c);
        ScheduledFuture<?> old = keepAliveFuture.getAndSet(c.eventLoop().scheduleWithFixedDelay(new KeepAliveTask(ctx), 20, 10, TimeUnit.SECONDS));
        if (old != null) {
            old.cancel(false);
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        ScheduledFuture<?> future = keepAliveFuture.get();
        if (future != null) {
            future.cancel(false);
        }
        channel.set(null);
        processor.connectionDropped();
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Reply cmd = (Reply) msg;
        log.debug(connectionName + " processing reply: {}", cmd);
        if (cmd instanceof DataAppended) {
            batchSizeTracker.recordAck(((DataAppended) cmd).getEventNumber());
        }
        cmd.process(processor);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // Close the connection when an exception is raised.
        log.error("Caught exception on connection: ", cause);
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }

    @Override
    public Future<Void> sendAsync(WireCommand cmd) {
        recentMessage.set(true);
        return getChannel().writeAndFlush(cmd);
    }

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        recentMessage.set(true);
        FutureHelpers.getAndHandleExceptions(getChannel().writeAndFlush(cmd), ConnectionFailedException::new);
    }
    
    @Override
    public void send(Append append) throws ConnectionFailedException {
        recentMessage.set(true);
        batchSizeTracker.recordAppend(append.getEventNumber(), append.getData().readableBytes());
        FutureHelpers.getAndHandleExceptions(getChannel().writeAndFlush(append), ConnectionFailedException::new);
    }
    
    @Override
    public void close() {
        Channel ch = channel.get();
        if (ch != null) {
            ch.close();
        }
    }

    private Channel getChannel() {
        Channel ch = channel.get();
        Preconditions.checkState(ch != null, connectionName + " Connection not yet established.");
        return ch;
    }
    
    @RequiredArgsConstructor
    private final class KeepAliveTask implements Runnable {
        private final ChannelHandlerContext ctx;

        @Override
        public void run() {
            try {
                if (!recentMessage.getAndSet(false)) {
                    send(new KeepAlive());
                }
            } catch (Exception e) {
                log.warn("Keep alive failed, killing connection " + connectionName);
                ctx.close();
            }
        }
    }

}