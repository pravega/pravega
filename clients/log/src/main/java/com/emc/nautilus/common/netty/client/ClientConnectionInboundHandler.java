package com.emc.nautilus.common.netty.client;

import java.util.concurrent.atomic.AtomicReference;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.Reply;
import com.emc.nautilus.common.netty.ReplyProcessor;
import com.emc.nautilus.common.netty.WireCommand;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ClientConnectionInboundHandler extends ChannelInboundHandlerAdapter implements ClientConnection {

	private AtomicReference<ReplyProcessor> processor = new AtomicReference<>();
	private AtomicReference<Channel> channel = new AtomicReference<>();

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
		ReplyProcessor replyProcessor = processor.get();
		if (replyProcessor == null) {
			throw new IllegalStateException("No command processor set for connection");
		}
		cmd.process(replyProcessor);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // Close the connection when an exception is raised.
        log.error("Caught exception on connection: ", cause);
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }

	@Override
	public void send(WireCommand cmd) {
		getChannel().writeAndFlush(cmd);
	}

	@Override
	public void setResponseProcessor(ReplyProcessor cp) {
		processor.set(cp);
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

	@Override
	public boolean isConnected() {
		Channel c = channel.get();
		return c!=null && c.isOpen();
	}
	
}