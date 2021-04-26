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
import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
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
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.security.TLSConfigChangeEventConsumer;
import io.pravega.segmentstore.server.host.security.TLSConfigChangeFileConsumer;
import io.pravega.segmentstore.server.host.security.TLSHelper;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.metrics.MetricNotifier.NO_OP_METRIC_NOTIFIER;
import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;

/**
 * Hands off any received data from a client to the CommandProcessor.
 */
@Slf4j
public final class PravegaConnectionListener implements AutoCloseable {
    //region Members

    private final String host;
    private final int port;
    private final StreamSegmentStore store;
    private final TableStore tableStore;
    private final DelegationTokenVerifier tokenVerifier;
    private final ConnectionTracker connectionTracker;

    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final SegmentStatsRecorder statsRecorder;
    private final TableSegmentStatsRecorder tableStatsRecorder;
    private final boolean replyWithStackTraceOnError;

    // TLS related params
    private final boolean enableTls; // whether to enable TLS

    @VisibleForTesting
    @Getter
    private final boolean enableTlsReload; // whether to reload TLS certificate when the certificate changes

    private final String pathToTlsCertFile;
    private final String pathToTlsKeyFile;

    private FileModificationMonitor tlsCertFileModificationMonitor; // used only if tls reload is enabled

    // Used for running token expiry handling tasks.
    private final ScheduledExecutorService tokenExpiryHandlerExecutor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the PravegaConnectionListener class listening on localhost with no StatsRecorder.
     *
     * @param enableTls           Whether to enable SSL/TLS.
     * @param port                The port to listen on.
     * @param streamSegmentStore  The SegmentStore to delegate all requests to.
     * @param tableStore          The SegmentStore to delegate all requests to.
     * @param tokenExpiryExecutor The executor to be used for running token expiration handling tasks.
     */
    @VisibleForTesting
    public PravegaConnectionListener(boolean enableTls, int port, StreamSegmentStore streamSegmentStore, TableStore tableStore, ScheduledExecutorService tokenExpiryExecutor) {
        this(enableTls, false, "localhost", port, streamSegmentStore, tableStore, SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(),
                new PassingTokenVerifier(), null, null, true, tokenExpiryExecutor);
    }

    /**
     * Creates a new instance of the PravegaConnectionListener class.
     *
     * @param enableTls          Whether to enable SSL/TLS.
     * @param enableTlsReload    Whether to reload TLS when the X.509 certificate file is replaced.
     * @param host               The name of the host to listen to.
     * @param port               The port to listen on.
     * @param streamSegmentStore The SegmentStore to delegate all requests to.
     * @param tableStore         The TableStore to delegate all requests to.
     * @param statsRecorder      (Optional) A StatsRecorder for Metrics for Stream Segments.
     * @param tableStatsRecorder (Optional) A Table StatsRecorder for Metrics for Table Segments.
     * @param tokenVerifier      The object to verify delegation token.
     * @param certFile           Path to the certificate file to be used for TLS.
     * @param keyFile            Path to be key file to be used for TLS.
     * @param replyWithStackTraceOnError Whether to send a server-side exceptions to the client in error messages.
     * @param executor           The executor to be used for running token expiration handling tasks.
     */
    public PravegaConnectionListener(boolean enableTls, boolean enableTlsReload, String host, int port, StreamSegmentStore streamSegmentStore, TableStore tableStore,
                                     SegmentStatsRecorder statsRecorder, TableSegmentStatsRecorder tableStatsRecorder,
                                     DelegationTokenVerifier tokenVerifier, String certFile, String keyFile,
                                     boolean replyWithStackTraceOnError, ScheduledExecutorService executor) {
        this.enableTls = enableTls;
        if (this.enableTls) {
            this.enableTlsReload = enableTlsReload;
        } else {
            this.enableTlsReload = false;
        }
        this.host = Exceptions.checkNotNullOrEmpty(host, "host");
        this.port = port;
        this.store = Preconditions.checkNotNull(streamSegmentStore, "streamSegmentStore");
        this.tableStore = Preconditions.checkNotNull(tableStore, "tableStore");
        this.statsRecorder = Preconditions.checkNotNull(statsRecorder, "statsRecorder");
        this.tableStatsRecorder = Preconditions.checkNotNull(tableStatsRecorder, "tableStatsRecorder");
        this.pathToTlsCertFile = certFile;
        this.pathToTlsKeyFile = keyFile;
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        if (tokenVerifier != null) {
            this.tokenVerifier = tokenVerifier;
        } else {
            this.tokenVerifier = new PassingTokenVerifier();
        }
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
        this.connectionTracker = new ConnectionTracker();
        this.tokenExpiryHandlerExecutor = executor;
    }

    //endregion

    public void startListening() {

        final AtomicReference<SslContext> sslCtx;
        if (this.enableTls) {
            sslCtx = new AtomicReference<>(TLSHelper.newServerSslContext(pathToTlsCertFile, pathToTlsKeyFile));
        } else {
            sslCtx = null;
        }

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

                 ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
                 p.addLast(new ExceptionLoggingHandler(ch.remoteAddress().toString()),
                         new CommandEncoder(null, NO_OP_METRIC_NOTIFIER),
                         new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                         new CommandDecoder(),
                         new AppendDecoder(),
                         lsh);

                 TrackedConnection c = new TrackedConnection(lsh, connectionTracker);
                 PravegaRequestProcessor prp = new PravegaRequestProcessor(store, tableStore, c, statsRecorder,
                         tableStatsRecorder, tokenVerifier, replyWithStackTraceOnError);

                 lsh.setRequestProcessor(new AppendProcessor(store, c, prp, statsRecorder, tokenVerifier,
                         replyWithStackTraceOnError, tokenExpiryHandlerExecutor));
             }
         });

        if (enableTls && enableTlsReload) {
            enableTlsContextReload(sslCtx);
        }

        // Start the server.
        serverChannel = b.bind(host, port).awaitUninterruptibly().channel();
    }

    @VisibleForTesting
    void enableTlsContextReload(AtomicReference<SslContext> sslCtx) {
        tlsCertFileModificationMonitor = prepareCertificateMonitor(this.pathToTlsCertFile, this.pathToTlsKeyFile,
                sslCtx);
        tlsCertFileModificationMonitor.startMonitoring();
        log.info("Successfully started file modification monitoring for TLS certificate: [{}]",
                this.pathToTlsCertFile);
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
                        new TLSConfigChangeFileConsumer(sslCtx, tlsCertificatePath, tlsKeyPath));
            } else {
                // For non symbolic links we'll use the event-based watcher, which is more efficient than a
                // polling-based monitor.
                result = new FileModificationEventWatcher(Paths.get(tlsCertificatePath),
                        new TLSConfigChangeEventConsumer(sslCtx, tlsCertificatePath, tlsKeyPath));
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
}
