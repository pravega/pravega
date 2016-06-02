package com.emc.logservice.server.handler;

import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.SSLException;

import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.server.CacheFactory;
import com.emc.logservice.server.MetadataRepository;
import com.emc.logservice.server.OperationLogFactory;
import com.emc.logservice.server.containers.StreamSegmentContainer;
import com.emc.logservice.server.logs.DurableLogFactory;
import com.emc.logservice.server.mocks.InMemoryMetadataRepository;
import com.emc.logservice.server.reading.ReadIndexFactory;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.storageabstraction.StorageFactory;
import com.emc.logservice.storageabstraction.mocks.InMemoryDurableDataLogFactory;
import com.emc.logservice.storageabstraction.mocks.InMemoryStorageFactory;
import com.emc.nautilus.common.netty.CommandDecoder;
import com.emc.nautilus.common.netty.CommandEncoder;
import com.emc.nautilus.common.netty.ConnectionListener;
import com.emc.nautilus.common.netty.ExceptionLoggingHandler;
import com.google.common.annotations.VisibleForTesting;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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
public final class LogSerivceConnectionListener implements ConnectionListener {

	private final boolean ssl;
	private final int port;
	private final StreamSegmentStore store;
	private Channel serverChannel;
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;

	public LogSerivceConnectionListener(boolean ssl, int port, String containerId) {
		this.ssl = ssl;
		this.port = port;
		try {
			store = createStore(containerId);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@VisibleForTesting
	public static final StreamSegmentContainer createStore(String containerId) throws InterruptedException, ExecutionException {
		MetadataRepository metadataRepository = new InMemoryMetadataRepository();
		DurableDataLogFactory dataFrameLogFactory = new InMemoryDurableDataLogFactory();
		OperationLogFactory durableLogFactory = new DurableLogFactory(dataFrameLogFactory);
		StorageFactory storageFactory = new InMemoryStorageFactory();
		CacheFactory cacheFactory = new ReadIndexFactory();
		StreamSegmentContainer store = new StreamSegmentContainer(containerId,
		           				metadataRepository,
		           				durableLogFactory,
		           				cacheFactory,
		           				storageFactory);
		store.initialize(Duration.ofMinutes(1)).get();
		store.start(Duration.ofMinutes(1)).get();
		return store;
	}

	public void startListening() {
		// Configure SSL.
		final SslContext sslCtx;
		if (ssl) {
			try {
				SelfSignedCertificate ssc = new SelfSignedCertificate();
				sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
			} catch (CertificateException | SSLException e) {
				throw new RuntimeException(e);
			}
		} else {
			sslCtx = null;
		}

		bossGroup = new EpollEventLoopGroup(1);
		workerGroup = new EpollEventLoopGroup();

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
					ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
					//p.addLast(new LoggingHandler(LogLevel.INFO));
					p.addLast(	new ExceptionLoggingHandler(),
					          	new CommandEncoder(),
								new LengthFieldBasedFrameDecoder(1024 * 1024, 4, 4),
								new CommandDecoder(),
								lsh);
					lsh.setRequestProcessor(new AppendProcessor(store,
							lsh,
							new LogServiceRequestProcessor(store, lsh)));
				}
			});

		// Start the server.
		serverChannel = b.bind(port).awaitUninterruptibly().channel();
	}

	public void shutdown() {
		// Wait until the server socket is closed.
		try {
			serverChannel.close();
			serverChannel.closeFuture().sync();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
		// Shut down all event loops to terminate all threads.
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
	}

}