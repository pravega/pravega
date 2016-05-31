package com.emc.logservice.server.handler;

import java.util.concurrent.atomic.AtomicReference;

import com.emc.nautilus.common.netty.ConnectionFailedException;
import com.emc.nautilus.common.netty.Request;
import com.emc.nautilus.common.netty.RequestProcessor;
import com.emc.nautilus.common.netty.ServerConnection;
import com.emc.nautilus.common.netty.WireCommand;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler implementation for the echo server.
 */
public class LogServiceServerHandler extends ChannelInboundHandlerAdapter implements ServerConnection {

	private AtomicReference<RequestProcessor> processor = new AtomicReference<>();
	private AtomicReference<Channel> channel = new AtomicReference<>();
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
		channel.set(ctx.channel());
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		Request cmd = (Request) msg;
		RequestProcessor requestProcessor = processor.get();
		if (requestProcessor == null) {
			throw new IllegalStateException("No command processor set for connection");
		}
		cmd.process(requestProcessor);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		// Close the connection when an exception is raised.
		cause.printStackTrace();
		ctx.close();
	}

	@Override
	public void send(WireCommand cmd) throws ConnectionFailedException {
		getChannel().writeAndFlush(cmd);
	}
	
	@Override
	public void sendAsync(WireCommand cmd) {
		getChannel().write(cmd);
	}

	@Override
	public void setRequestProcessor(RequestProcessor rp) {
		processor.set(rp);
	}

	@Override
	public void drop() {
		Channel ch = channel.get();
		if (ch != null) {
			ch.close();
		}
	}

	@Override
	public void pauseReading() {
		getChannel().config().setAutoRead(false); 
	}

	@Override
	public void resumeReading() {
		getChannel().config().setAutoRead(true); 
	}
	
	private Channel getChannel() {
		Channel ch = channel.get();
		if (ch == null) {
			throw new IllegalStateException("Connection not yet established.");
		}
		return ch;
	}
	
}