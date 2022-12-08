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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.pravega.client.ClientConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.CertificateUtils;
import io.pravega.common.util.ReusableLatch;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.EnhancedByteBufInputStream;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.pravega.common.io.StreamHelpers.closeQuietly;
import static io.pravega.shared.protocol.netty.AppendBatchSizeTracker.MAX_BATCH_TIME_MILLIS;

@Slf4j
public class TcpClientConnection implements ClientConnection {

    static final int CONNECTION_TIMEOUT = 5000;
    static final int TCP_BUFFER_SIZE = 256 * 1024;
    static final int SOCKET_TIMEOUT_MS = 3 * 60 * 1000;
    
    private final Socket socket;
    private final CommandEncoder encoder;
    private final ConnectionReader reader;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final PravegaNodeUri location;
    private final Runnable onClose;
    private final ScheduledFuture<?> timeoutFuture;
   
    private TcpClientConnection(Socket socket, CommandEncoder encoder, ConnectionReader reader, PravegaNodeUri location,
                                Runnable onClose, ScheduledExecutorService executor) {
        this.socket = checkNotNull(socket);
        this.encoder = checkNotNull(encoder);
        this.reader = checkNotNull(reader);
        this.location = checkNotNull(location);
        this.onClose = onClose;
        this.timeoutFuture = executor.scheduleWithFixedDelay(new TimeoutBatch(encoder),
                                                             MAX_BATCH_TIME_MILLIS,
                                                             MAX_BATCH_TIME_MILLIS,
                                                             TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    static class ConnectionReader implements Runnable {
        static final ThreadFactory THREAD_FACTORY = ExecutorServiceHelpers.getThreadFactory("ClientSocketReaders", (Thread.NORM_PRIORITY + Thread.MAX_PRIORITY) / 2);
        
        private final String name;
        private final InputStream in;
        private final ReplyProcessor callback;
        private final Thread thread;
        private final FlowToBatchSizeTracker flowToBatchSizeTracker;
        private final AtomicBoolean stop = new AtomicBoolean(false);
        private final ReusableLatch hasStopped = new ReusableLatch(false);

        public ConnectionReader(String name, InputStream in, ReplyProcessor callback, FlowToBatchSizeTracker flowToBatchSizeTracker) {
            this.name = name;
            this.in = in;
            this.callback = callback;
            this.thread = THREAD_FACTORY.newThread(this);
            this.flowToBatchSizeTracker = flowToBatchSizeTracker;
        }
        
        public void start() {
            thread.start();
        }
        
        @Override
        public void run() {
            IoBuffer buffer = new IoBuffer();
            while (!stop.get()) {
                try {
                    // This method blocks until it is able to read data from the tcp socket InputStream.
                    WireCommand command = readCommand(in, buffer);
                    if (stop.get()) {
                        // stop has already been invoked ignore the message received from the socket.
                        break;
                    }
                    if (command instanceof WireCommands.DataAppended) {
                        WireCommands.DataAppended dataAppended = (WireCommands.DataAppended) command;
                        flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(Flow.toFlowID(dataAppended.getRequestId())).recordAck(dataAppended.getEventNumber());
                    }
                    try {
                        callback.process((Reply) command);
                    } catch (Exception e) {
                        callback.processingFailure(e);
                    }
                } catch (SocketException e) {
                    if (e.getMessage().equals("Socket closed")) {
                        log.info("Closing TcpConnection.Reader because socket is closed.");
                    } else {
                        log.warn("Error in reading from socket.", e);
                    }
                    stop();
                } catch (EOFException e) {
                    log.info("Closing TcpClientConnection.Reader because end of input reached.");
                    stop();
                } catch (Exception e) {
                    log.warn("Error processing data from from server " + name, e);
                    stop();
                }
            }
            hasStopped.release();
        }

        @VisibleForTesting
        static WireCommand readCommand(InputStream in, IoBuffer buffer) throws IOException {
            ByteBuf header = buffer.getBuffOfSize(in, 8);

            int t = header.getInt(0);
            WireCommandType type = WireCommands.getType(t);
            if (type == null) {
                throw new InvalidMessageException("Unknown wire command: " + t);
            }

            int length = header.getInt(4);
            if (length < 0 || length > WireCommands.MAX_WIRECOMMAND_SIZE) {
                throw new InvalidMessageException("Event of invalid length: " + length);
            }

            ByteBuf payload = buffer.getBuffOfSize(in, length);
            
            return type.readFrom(new EnhancedByteBufInputStream(payload), length);
        }
        
        public void stop() {
            if (stop.getAndSet(true)) {
                return;
            }
            // close the input stream to ensure no further data can be received.
            closeQuietly(in, log, "Got error while shutting down reader {}. ", name);
            // No need to await termination if stop is invoked by the callback method.
            if (Thread.currentThread().getId() != this.thread.getId()) {
                // wait until we have completed the current call to the reply processors.
                Exceptions.handleInterrupted(hasStopped::await);
            }
            callback.connectionDropped();
        }
    }
    
    @RequiredArgsConstructor
    private static final class TimeoutBatch implements Runnable {
        private final AtomicLong token = new AtomicLong(-1);
        private final CommandEncoder encoder;
        @Override
        public void run() {
            token.set(encoder.batchTimeout(token.get()));
        }    
    }

    /**
     * Connects to the specified location.
     * 
     * @param location Location to connect to.
     * @param clientConfig config for socket.
     * @param callback ReplyProcessor for replies from the server.
     * @param executor Thread pool to perform the connect in.
     * @param onClose A callback to be notified when this connection closes.
     * @return A future for a new connection. If the connect attempt fails the future will be failed with a {@link ConnectionFailedException}
     */
    public static CompletableFuture<TcpClientConnection> connect(PravegaNodeUri location, ClientConfig clientConfig, ReplyProcessor callback,
                                              ScheduledExecutorService executor, Runnable onClose) {
        return CompletableFuture.supplyAsync(() -> {
            Socket socket = createClientSocket(location, clientConfig); 
            try {
                InputStream inputStream = socket.getInputStream();
                FlowToBatchSizeTracker flowToBatchSizeTracker = new FlowToBatchSizeTracker();
                ConnectionReader reader = new ConnectionReader(location.toString(), inputStream, callback, flowToBatchSizeTracker);
                reader.start();
                // We use the flow id on both CommandEncoder and ConnectionReader to locate AppendBatchSizeTrackers.
                CommandEncoder encoder = new CommandEncoder(requestId ->
                        flowToBatchSizeTracker.getAppendBatchSizeTrackerByFlowId(Flow.toFlowID(requestId)), null, socket.getOutputStream(), callback, location);
                return new TcpClientConnection(socket, encoder, reader, location, onClose, executor);
            } catch (Exception e) {
                closeQuietly(socket, log, "Failed to close socket while failing.");
                onClose.run();
                throw Exceptions.sneakyThrow(new ConnectionFailedException(e));
            }
        }, executor);
    }

    private static TrustManagerFactory createFromCert(String trustStoreFilePath)
            throws CertificateException, IOException, NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory factory = null;
        if (!Strings.isNullOrEmpty(trustStoreFilePath)) {
            KeyStore trustStore = CertificateUtils.createTrustStore(trustStoreFilePath);

            factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(trustStore);
        }
        return factory;
    }

    /**
     * Creates a socket connected to the provided endpoint. 
     * Note that this is a sync call even though it is called in an async context. 
     * While this is normally frowned upon, it is simply not possible to construct an SSL socket asynchronously in Java.
     * @throws ConnectionFailedException (Sneakily thrown) If the connect attempt fails.
     */
    private static Socket createClientSocket(PravegaNodeUri location, ClientConfig clientConfig) {
        try {
            Socket result;
            if (clientConfig.isEnableTlsToSegmentStore()) {
                TrustManagerFactory trustMgrFactory = createFromCert(clientConfig.getTrustStore());

                // Prepare a TLS context that uses the trust manager
                SSLContext tlsContext = SSLContext.getInstance("TLS");
                tlsContext.init(null,
                                trustMgrFactory != null ? trustMgrFactory.getTrustManagers() : null,
                                        null);

                SSLSocket tlsClientSocket = (SSLSocket) tlsContext.getSocketFactory().createSocket();

                // SSLSocket does not perform hostname verification by default. So, we must explicitly enable it.
                if (clientConfig.isValidateHostName()) {
                    SSLParameters tlsParams = new SSLParameters();

                    // While the connection is to a TCP service and not an HTTP server, we use `HTTPS` as the endpoint
                    // identification algorithm, which in turn ensures that the SSLSocket will verify the server's host
                    // name during TLS handshake. This is a commonly used way of enabling hostname verification
                    // regardless of whether the service itself is HTTP (no in this case).
                    tlsParams.setEndpointIdentificationAlgorithm("HTTPS");
                    tlsClientSocket.setSSLParameters(tlsParams);
                }
                result = tlsClientSocket;

            } else {
                result = new Socket();
            }
            result.setSendBufferSize(TCP_BUFFER_SIZE);
            result.setReceiveBufferSize(TCP_BUFFER_SIZE);
            result.setTcpNoDelay(true);
            result.connect(new InetSocketAddress(location.getEndpoint(), location.getPort()), CONNECTION_TIMEOUT);
            result.setSoTimeout(SOCKET_TIMEOUT_MS);
            return result;
        } catch (Exception e) {
            throw Exceptions.sneakyThrow(new ConnectionFailedException(e));
        }
    }

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        checkIfClosed();
        try {
            encoder.write(cmd);
        } catch (IOException e) {
            log.warn("Error writing to connection: {}", e.toString());
            close();
            throw new ConnectionFailedException(e);
        }
    }

    @Override
    public void send(Append append) throws ConnectionFailedException {
        checkIfClosed();
        try {
            encoder.write(append);
        } catch (IOException e) {
            log.warn("Error writing to connection: {}", e.toString());
            close();
            throw new ConnectionFailedException(e);
        }
    }

    private void checkIfClosed() throws ConnectionFailedException {
        if (closed.get()) {
            throw new ConnectionFailedException("Connection already closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            reader.stop();
            timeoutFuture.cancel(false);
            closeQuietly(socket, log, "Error closing TcpClientConnection.socket");
            if (onClose != null) {
                onClose.run();
            }
        }
    }

    @VisibleForTesting
    boolean isClosed() {
        return closed.get();
    }

    @Override
    public void sendAsync(List<Append> appends, CompletedCallback callback) {
        try {
            for (Append append : appends) {
                encoder.write(append);
            }
            callback.complete(null);
        } catch (IOException e) {
            log.warn("Error writing to connection: {}", e.toString());
            close();
            callback.complete(new ConnectionFailedException(e));
        }
    }
    
    @Override
    public String toString() {
        return "TcpClientConnection [location=" + location + ", isClosed=" + closed.get() + "]";
    }

    @VisibleForTesting
    FlowToBatchSizeTracker getConnectionReaderFlowToBatchSizeTracker() {
        return this.reader.flowToBatchSizeTracker;
    }

    @Override
    public PravegaNodeUri getLocation() {
        return location;
    }
}
