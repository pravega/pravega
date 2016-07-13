/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.nautilus.common.netty.client;

import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLException;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.CommandDecoder;
import com.emc.nautilus.common.netty.CommandEncoder;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.ExceptionLoggingHandler;
import com.emc.nautilus.common.netty.ReplyProcessor;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConnectionFactoryImpl implements ConnectionFactory {

	private final boolean ssl;
	private final int port;
	private EventLoopGroup group;
	private boolean nio = false;

	public ConnectionFactoryImpl(boolean ssl, int port) {
		this.ssl = ssl;
		this.port = port;
		try {
			this.group = new EpollEventLoopGroup();
		} catch (ExceptionInInitializerError e) {
		    log.warn("Epoll not available. Falling back on NIO.");
			nio = true;
			this.group = new NioEventLoopGroup();
		}
	}

	@Override
	public ClientConnection establishConnection(String host, ReplyProcessor rp) {
		final SslContext sslCtx;
		if (ssl) {
			try {
				sslCtx = SslContextBuilder.forClient()
					.trustManager(FingerprintTrustManagerFactory
						.getInstance(FingerprintTrustManagerFactory.getDefaultAlgorithm()))
					.build();
			} catch (SSLException | NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
		} else {
			sslCtx = null;
		}
		ClientConnectionInboundHandler handler = new ClientConnectionInboundHandler(host, rp);
		Bootstrap b = new Bootstrap();
		b.group(group)
			.channel(nio ? NioSocketChannel.class : EpollSocketChannel.class)
			.option(ChannelOption.TCP_NODELAY, true)
			.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					ChannelPipeline p = ch.pipeline();
					if (sslCtx != null) {
						p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
					}
					// p.addLast(new LoggingHandler(LogLevel.INFO));
					p.addLast(	new ExceptionLoggingHandler(ch.remoteAddress().toString()),
								new CommandEncoder(),
								new LengthFieldBasedFrameDecoder(1024 * 1024, 4, 4),
								new CommandDecoder(),
								handler);
				}
			});

		// Start the client.
		try {
			b.connect(host, port).sync();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
		return handler;
	}

	@Override
	public void close() {
		// Shut down the event loop to terminate all threads.
		group.shutdownGracefully();
	}
	
	protected void finalize() {
		group.shutdownGracefully();
	}

}