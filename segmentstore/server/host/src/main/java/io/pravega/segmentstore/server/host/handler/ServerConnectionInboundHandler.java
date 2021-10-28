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
package io.pravega.segmentstore.server.host.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Acts as a bridge between Netty and the RequestProcessor on the server.
 */
@Slf4j
public class ServerConnectionInboundHandler extends ChannelInboundHandlerAdapter implements ServerConnection {
    private final AtomicReference<RequestProcessor> processor = new AtomicReference<>();
    private final AtomicReference<Channel> channel = new AtomicReference<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        channel.set(ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Request cmd = (Request) msg;
        if (cmd.mustLog()) {
            log.debug("Received request: {}", cmd);
        } else {
            log.trace("Received request: {}", cmd);
        }

        RequestProcessor requestProcessor = processor.get();
        if (requestProcessor == null) {
            throw new IllegalStateException("No command processor set for connection");
        }
        try {
            cmd.process(requestProcessor);
        } catch (Throwable ex) {
            // Release buffers in case of an unhandled exception.
            if (cmd instanceof WireCommands.ReleasableCommand) {
                ((WireCommands.ReleasableCommand) cmd).release(); // Idempotent. Invoking multiple times has no side effects.
            }
            throw ex;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        logError(cause);
        ctx.close();
    }

    @Override
    public void send(WireCommand cmd) {
        Channel c = getChannel();
        // Work around for https://github.com/netty/netty/issues/3246
        EventLoop eventLoop = c.eventLoop();
        eventLoop.execute(() -> write(c, cmd));
    }

    private static void write(Channel channel, WireCommand data) {
        channel.write(data).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }  
    
    @Override
    public void setRequestProcessor(RequestProcessor rp) {
        processor.set(rp);
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            Channel ch = channel.get();
            if (ch != null) {
                // wait for all messages to be sent before closing the channel.
                ch.eventLoop().execute(() -> ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE));
            }
            RequestProcessor rp = processor.get();
            if (rp != null) {
                rp.connectionDropped();
            }
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void pauseReading() {
        log.debug("Pausing reading from connection {}", this);
        getChannel().config().setAutoRead(false);
    }

    @Override
    public void resumeReading() {
        log.trace("Resuming reading from connection {}", this);
        getChannel().config().setAutoRead(true);
    }

    private Channel getChannel() {
        Channel ch = channel.get();
        if (ch == null) {
            throw new IllegalStateException("Connection not yet established.");
        }
        return ch;
    }

    private void logError(Throwable cause) {
        if (Exceptions.unwrap(cause) instanceof IllegalContainerStateException) {
            log.warn("Caught exception on connection: {}", cause.toString());
        } else {
            log.error("Caught exception on connection: ", cause);
        }
    }

    @Override
    public String toString() {
        Channel c = channel.get();
        if (c == null) {
            return "NewServerConnection";
        }
        return c.toString();
    }
    
}
