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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.pravega.common.Exceptions;
import io.pravega.common.io.filesystem.FileModificationEventWatcher;
import io.pravega.common.io.filesystem.FileModificationMonitor;
import io.pravega.common.io.filesystem.FileModificationPollingMonitor;
import io.pravega.segmentstore.server.host.security.TLSConfigChangeEventConsumer;
import io.pravega.segmentstore.server.host.security.TLSConfigChangeFileConsumer;
import io.pravega.segmentstore.server.host.security.TLSHelper;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthServiceManager;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import io.pravega.shared.protocol.netty.RequestProcessor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public abstract class AbstractConnectionListener implements AutoCloseable {

    //region Members

    private final String host;
    private final int port;

    private Channel serverChannel; // tracks the status of the connection

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @VisibleForTesting
    @Getter
    private final HealthServiceManager healthServiceManager;
    private final ConnectionTracker connectionTracker;

    // TLS related params
    private final boolean enableTls; // whether to enable TLS

    @VisibleForTesting
    @Getter
    private final boolean enableTlsReload; // whether to reload TLS certificate when the certificate changes

    private final String pathToTlsCertFile;
    private final String pathToTlsKeyFile;
    private final String[] tlsProtocolVersion;

    private FileModificationMonitor tlsCertFileModificationMonitor; // used only if tls reload is enabled

    /**
     * Creates a new instance of the AdminConnectionListener class.
     *
     * @param enableTls          Whether to enable SSL/TLS.
     * @param enableTlsReload    Whether to reload TLS when the X.509 certificate file is replaced.
     * @param host               The name of the host to listen to.
     * @param port               The port to listen on.
     * @param certFile           Path to the certificate file to be used for TLS.
     * @param keyFile            Path to be key file to be used for TLS.
     * @param tlsProtocolVersion the version of the TLS protocol
     */
    public AbstractConnectionListener(boolean enableTls, boolean enableTlsReload, String host, int port,
                                      String certFile, String keyFile, String[] tlsProtocolVersion) {

        this(enableTls, enableTlsReload, host, port, certFile, keyFile, tlsProtocolVersion, null);
    }

    /**
     * Creates a new instance of the AdminConnectionListener class with HealthServiceManager.
     *
     * @param enableTls          Whether to enable SSL/TLS.
     * @param enableTlsReload    Whether to reload TLS when the X.509 certificate file is replaced.
     * @param host               The name of the host to listen to.
     * @param port               The port to listen on.
     * @param certFile           Path to the certificate file to be used for TLS.
     * @param keyFile            Path to be key file to be used for TLS.
     * @param tlsProtocolVersion The version of the TLS protocol
     * @param healthServiceManager The healthService to register new health contributors related to the listeners.
     */
    public AbstractConnectionListener(boolean enableTls, boolean enableTlsReload, String host, int port,
                                      String certFile, String keyFile, String[] tlsProtocolVersion,
                                      HealthServiceManager healthServiceManager) {
        this.enableTls = enableTls;
        this.enableTlsReload = this.enableTls && enableTlsReload;
        this.host = Exceptions.checkNotNullOrEmpty(host, "host");
        this.port = port;
        this.pathToTlsCertFile = certFile;
        this.pathToTlsKeyFile = keyFile;
        this.tlsProtocolVersion = Arrays.copyOf(tlsProtocolVersion, tlsProtocolVersion.length);
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        this.connectionTracker = new ConnectionTracker();
        this.healthServiceManager = healthServiceManager;
    }

    /**
     * Any subclass extending this class should provide a list of {@link ChannelHandler} objects to encode/decode
     * incoming messages.
     *
     * @param connectionName Incoming connection IP for information purposes.
     *
     * @return Sorted list of encoders/decoders to process requests.
     */
    public abstract List<ChannelHandler> createEncodingStack(String connectionName);

    /**
     * Any subclass extending this class should provide a {@link RequestProcessor} object to handle incoming requests.
     *
     * @param trackedConnection {@link TrackedConnection} to be used by the {@link RequestProcessor}.
     *
     * @return A {@link RequestProcessor} object to handle incoming messages.
     */
    public abstract RequestProcessor createRequestProcessor(TrackedConnection trackedConnection);

    /**
     * Initializes the connection listener internals and starts listening.
     */
    public void startListening() {
        final AtomicReference<SslContext> sslCtx = this.enableTls ?
                new AtomicReference<>(TLSHelper.newServerSslContext(pathToTlsCertFile, pathToTlsKeyFile, tlsProtocolVersion)) : null;
        boolean nio = false;
        try {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
        } catch (ExceptionInInitializerError | UnsatisfiedLinkError | NoClassDefFoundError e) {
            nio = true;
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(nio ? NioServerSocketChannel.class : EpollServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();

                        // Add SslHandler to the channel's pipeline, if TLS is enabled.
                        if (enableTls) {
                            SslHandler sslHandler = sslCtx.get().newHandler(ch.alloc());

                            // We add a name to SSL/TLS handler, unlike the other handlers added later, to make it
                            // easier to find and replace the handler.
                            p.addLast(TLSHelper.TLS_HANDLER_NAME, sslHandler);
                        }

                        // Configure the class-specific encoder stack and request processors.
                        ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
                        createEncodingStack(ch.remoteAddress().toString()).forEach(p::addLast);
                        lsh.setRequestProcessor(createRequestProcessor(new TrackedConnection(lsh, connectionTracker)));
                        p.addLast(lsh);
                    }
                });

        if (enableTls && enableTlsReload) {
            enableTlsContextReload(sslCtx);
        }

        // Start the server.
        serverChannel = b.bind(host, port).awaitUninterruptibly().channel();

        if (healthServiceManager != null) {
            healthServiceManager.register(new ConnectionListenerHealthContributor(this));
        }
    }

    @VisibleForTesting
    void enableTlsContextReload(AtomicReference<SslContext> sslCtx) {
        tlsCertFileModificationMonitor = prepareCertificateMonitor(this.pathToTlsCertFile, this.pathToTlsKeyFile, sslCtx);
        tlsCertFileModificationMonitor.startMonitoring();
        log.info("Successfully started file modification monitoring for TLS certificate: [{}]", this.pathToTlsCertFile);
    }

    @VisibleForTesting
    FileModificationMonitor prepareCertificateMonitor(String tlsCertificatePath, String tlsKeyPath,
                                                      AtomicReference<SslContext> sslCtx) {
        return prepareCertificateMonitor(Files.isSymbolicLink(Paths.get(tlsCertificatePath)),
                tlsCertificatePath, tlsKeyPath, sslCtx);
    }

    @VisibleForTesting
    FileModificationMonitor prepareCertificateMonitor(boolean isTLSCertPathSymLink, String tlsCertificatePath,
                                                      String tlsKeyPath,
                                                      AtomicReference<SslContext> sslCtx) {
        FileModificationMonitor result;
        try {
            if (isTLSCertPathSymLink) {
                // For symbolic links, the event-based watcher doesn't work, so we use a polling monitor.
                log.info("The path to certificate file [{}] was found to be a symbolic link, " +
                                " so using [{}] to monitor for certificate changes",
                        tlsCertificatePath, FileModificationPollingMonitor.class.getSimpleName());

                result = new FileModificationPollingMonitor(Paths.get(tlsCertificatePath),
                        new TLSConfigChangeFileConsumer(sslCtx, tlsCertificatePath, tlsKeyPath, tlsProtocolVersion));
            } else {
                // For non symbolic links we'll use the event-based watcher, which is more efficient than a
                // polling-based monitor.
                result = new FileModificationEventWatcher(Paths.get(tlsCertificatePath),
                        new TLSConfigChangeEventConsumer(sslCtx, tlsCertificatePath, tlsKeyPath, tlsProtocolVersion));
            }
            return result;
        } catch (FileNotFoundException e) {
            log.error("Failed to prepare a monitor for the certificate at path [{}]", tlsCertificatePath, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // Wait until the server socket is closed.
        Exceptions.handleInterrupted(() -> {
            if (serverChannel != null) {
                serverChannel.close();
                serverChannel.closeFuture().sync();
            }
        });

        // Shut down all event loops to terminate all threads.
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (tlsCertFileModificationMonitor != null) {
            tlsCertFileModificationMonitor.stopMonitoring();
        }
    }

    /**
     *  A contributor for managing health of a connection listener.
      */
    private static class ConnectionListenerHealthContributor extends AbstractHealthContributor {
        @NonNull
        private final AbstractConnectionListener listener;

        private ConnectionListenerHealthContributor(AbstractConnectionListener listener) {
            super(listener.getClass().getSimpleName());
            this.listener = listener;
        }

        @Override
        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.DOWN;
            boolean running = listener.serverChannel.isOpen();
            if (running) {
                status = Status.NEW;
            }

            boolean ready = listener.serverChannel.isActive();
            if (ready) {
                status = Status.UP;
            }

            builder.details(ImmutableMap.of("host", listener.host, "port", listener.port));
            return status;
        }
    }
}
