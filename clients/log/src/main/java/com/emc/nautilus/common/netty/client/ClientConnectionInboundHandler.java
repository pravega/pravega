package com.emc.nautilus.common.netty.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.ConnectionFailedException;
import com.emc.nautilus.common.netty.Reply;
import com.emc.nautilus.common.netty.ReplyProcessor;
import com.emc.nautilus.common.netty.WireCommand;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ClientConnectionInboundHandler extends ChannelInboundHandlerAdapter implements ClientConnection {

	private final ReplyProcessor processor;
	private final AtomicReference<Channel> channel = new AtomicReference<>();

	ClientConnectionInboundHandler(ReplyProcessor processor) {
	    this.processor = processor;
	}
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
		channel.set(ctx.channel());
	}

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    	channel.set(null);
    	super.channelUnregistered(ctx);
    }
	
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
    	Reply cmd = (Reply) msg;
    	log.debug("Processing reply: {}",cmd);
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
	public void send(WireCommand cmd) throws ConnectionFailedException {
		try {
            getChannel().writeAndFlush(cmd).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send call was interrupted", e);
        } catch (ExecutionException e) {
            throw new ConnectionFailedException(e.getCause());
        }
	}

	@Override
	public void drop() {
		Channel ch = channel.get();
		if (ch != null) {
			ch.close();
		}
	}
	
	private Channel getChannel() {
		Channel ch = channel.get();
		if (ch == null) {
			throw new IllegalStateException("Connection not yet established.");
		}
		return ch;
	}

//	@Override
//	public boolean isConnected() {
//		Channel c = channel.get();
//		return c!=null && c.isOpen();
//	}
	
}