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
package io.pravega.client.connection.impl;

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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.pravega.client.ClientConfig;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConnectionFactoryImplTest {

    boolean ssl = false;
    private Channel serverChannel;
    private int port;
    private SslContext sslCtx;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @Before
    public void setUp() throws Exception {
        // Configure SSL.
        port = TestUtils.getAvailableListenPort();
        if (ssl) {
            try {
                sslCtx = SslContextBuilder.forServer(new File(SecurityConfigDefaults.TLS_SERVER_CERT_PATH),
                             new File(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)).build();
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }
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
             public void initChannel(SocketChannel ch) throws Exception {
                 ChannelPipeline p = ch.pipeline();
                 if (sslCtx != null) {
                     SslHandler handler = sslCtx.newHandler(ch.alloc());
                     SSLEngine sslEngine = handler.engine();
                     SSLParameters sslParameters = sslEngine.getSSLParameters();
                     sslParameters.setEndpointIdentificationAlgorithm("LDAPS");
                     sslEngine.setSSLParameters(sslParameters);
                     p.addLast(handler);
                 }
             }
         });

        // Start the server.
        serverChannel = b.bind("localhost", port).awaitUninterruptibly().channel();
    }

    @After
    public void tearDown() throws Exception {
        serverChannel.close().awaitUninterruptibly();
        bossGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS).await();
        workerGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS).await();
        if (sslCtx != null) {
            ReferenceCountUtil.safeRelease(sslCtx);
        }
    }

    @Test
    public void establishConnection() throws ConnectionFailedException {
        @Cleanup
        ConnectionFactory factory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                              .controllerURI(URI.create((this.ssl ? "tls://" : "tcp://") + "localhost"))
                                                                              .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                                                                              .build());
        @Cleanup
        ClientConnection connection = factory.establishConnection(new PravegaNodeUri("localhost", port), new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {

            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }
        }).join();

        connection.send(new WireCommands.Hello(0, 0));
    }

    @Test
    public void getActiveChannelTestWithConnectionPooling() {
        ClientConfig config = ClientConfig.builder()
                .controllerURI(URI.create((this.ssl ? "tls://" : "tcp://") + "localhost"))
                .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                .build();
        @Cleanup
        SocketConnectionFactoryImpl factory = new SocketConnectionFactoryImpl(config);
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(config, factory);
        Flow flow = Flow.create();
        @Cleanup
        ClientConnection connection = connectionPool.getClientConnection(flow, new PravegaNodeUri("localhost", port),
                                                                         new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {

            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }
        }).join();

        assertEquals("Expected active channel count is 1", 1, factory.getOpenSocketCount());

        // close the connection, this does not close the underlying network connection due to connection pooling.
        connection.close();
        assertEquals("Expected active channel count is 1", 1, factory.getOpenSocketCount());
        
        // the underlying connection is closed only on closing the connection pool
        connectionPool.close();

        assertEquals("Expected active channel count is 0", 0, factory.getOpenSocketCount());
    }

    @Test
    public void getActiveChannelTestWithoutConnectionPooling() {
        @Cleanup
        SocketConnectionFactoryImpl factory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(URI.create((this.ssl ? "tls://" : "tcp://") + "localhost"))
                .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                .build());
                
        final FailingReplyProcessor rp = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {

            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }
        };
        // establish a connection with Flow.
        @Cleanup
        ClientConnection connection = factory.establishConnection(new PravegaNodeUri("localhost", port), rp).join();

        assertEquals("Expected active channel count is 1", 1, factory.getOpenSocketCount());

        // close the connection, this closes the underlying network connection since connection pooling is disabled.
        connection.close();

        // wait until the channel is closed.
        assertEquals("Expected active channel count is 0", 0, factory.getOpenSocketCount());
    }
}
