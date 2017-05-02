/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl.netty;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.pravega.shared.protocol.netty.WireCommands;
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
        c.write(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATABLE_VERSION), c.voidPromise());
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
        if (cmd instanceof WireCommands.DataAppended) {
            batchSizeTracker.recordAck(((WireCommands.DataAppended) cmd).getEventNumber());
        }
        try {
            cmd.process(processor);
        } catch (Exception e) {
            processor.processingFailure(e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        processor.processingFailure(new ConnectionFailedException(cause));
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
                    send(new WireCommands.KeepAlive());
                }
            } catch (Exception e) {
                log.warn("Keep alive failed, killing connection " + connectionName);
                ctx.close();
            }
        }
    }

}