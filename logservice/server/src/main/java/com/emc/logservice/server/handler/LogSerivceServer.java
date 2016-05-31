package com.emc.logservice.server.handler;

import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.nautilus.common.netty.CommandDecoder;
import com.emc.nautilus.common.netty.CommandEncoder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

/**
 * Hands off any received data from a client to the CommandProcessor.
 */
public final class LogSerivceServer {

	static final boolean SSL = System.getProperty("ssl") != null;
	static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));
	static final StreamSegmentStore store; //TODO: How do we get one?
	
	
	public static void main(String[] args) throws Exception {
		// Configure SSL.
		final SslContext sslCtx;
		if (SSL) {
			SelfSignedCertificate ssc = new SelfSignedCertificate();
			sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
		} else {
			sslCtx = null;
		}

		// Configure the server.
		EventLoopGroup bossGroup = new EpollEventLoopGroup(1);
		EventLoopGroup workerGroup = new EpollEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup)
				.channel(EpollServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, 100)
				.handler(new LoggingHandler(LogLevel.INFO))
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						if (sslCtx != null) {
							p.addLast(sslCtx.newHandler(ch.alloc()));
						}
						// p.addLast(new LoggingHandler(LogLevel.INFO));
						LogServiceServerHandler lsh = new LogServiceServerHandler();
						p.addLast(	new CommandEncoder(),
						          	new LengthFieldBasedFrameDecoder(1024*1024, 4, 4),
									new CommandDecoder(),
									lsh);
						lsh.setRequestProcessor(new AppendProcessor(store, lsh,
								new LogServiceRequestProcessor(store, lsh)));
					}
				});

			// Start the server.
			ChannelFuture f = b.bind(PORT).sync();

			// Wait until the server socket is closed.
			f.channel().closeFuture().sync();
		} finally {
			// Shut down all event loops to terminate all threads.
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
}