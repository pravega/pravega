/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.connection.impl;

import com.google.common.base.Strings;
import io.netty.buffer.Unpooled;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.AppendBatchSizeTrackerImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.CertificateUtils;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.EnhancedByteBufInputStream;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

@Slf4j
public class TcpClientConnection implements ClientConnection {

    private final Socket socket;
    private final CommandEncoder encoder;
    private final ConnectionReader reader;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String host;

    private static class ConnectionReader {

        private final String name;
        private final ReadableByteChannel channel;
        private final ReplyProcessor callback;
        private final ScheduledExecutorService thread;
        private final AppendBatchSizeTracker batchSizeTracker;
        private final AtomicBoolean stop = new AtomicBoolean(false);

        public ConnectionReader(String name, ReadableByteChannel channel, ReplyProcessor callback,
                                AppendBatchSizeTracker batchSizeTracker) {
            this.name = name;
            this.channel = channel;
            this.callback = callback;
            this.thread = ExecutorServiceHelpers.newScheduledThreadPool(1, "Reading from " + name);
            this.batchSizeTracker = batchSizeTracker;
        }
        
        public void start() {
            thread.submit(() -> {
                while (!stop.get()) {
                    try {
                        ByteBuffer header = ByteBuffer.allocate(8);
                        fillFromChannel(header, channel);

                        int t = header.getInt();
                        WireCommandType type = WireCommands.getType(t);
                        if (type == null) {
                            throw new InvalidMessageException("Unknown wire command: " + t);
                        }

                        int length = header.getInt();
                        if (length < 0 || length > WireCommands.MAX_WIRECOMMAND_SIZE) {
                            throw new InvalidMessageException("Event of invalid length: " + length);
                        }

                        ByteBuffer payload = ByteBuffer.allocate(length);
                        fillFromChannel(payload, channel);
                        WireCommand command = type.readFrom(new EnhancedByteBufInputStream(Unpooled.wrappedBuffer(payload)), length);
                        if (command instanceof WireCommands.DataAppended) {
                            WireCommands.DataAppended dataAppended = (WireCommands.DataAppended) command;
                            batchSizeTracker.recordAck(dataAppended.getEventNumber());
                        }
                        try {
                            callback.process((Reply) command);
                        } catch (Exception e) {
                            callback.processingFailure(e);
                        }
                    } catch (Exception e) {
                        log.error("Error processing data from from server " + name, e);
                        stop();
                    }
                }
            });
        }

        private void fillFromChannel(ByteBuffer buff, ReadableByteChannel chan) throws IOException {
            while (buff.hasRemaining()) {                
                chan.read(buff);
            }
            buff.flip();
        }
        
        public void stop() {
            stop.set(true);
            thread.shutdown();
            callback.connectionDropped();
        }
    }

    public TcpClientConnection(String host, int port, ClientConfig clientConfig,  ReplyProcessor callback) {
        this.host = host;
        socket = createClientSocket(host, port, clientConfig); //TODO: Switch to AsynchronousSocketChannel.connect
        socket.setTcpNoDelay(true);
        SocketChannel channel = socket.getChannel();
        AppendBatchSizeTrackerImpl batchSizeTracker = new AppendBatchSizeTrackerImpl();
        this.reader = new ConnectionReader(host, channel, callback, batchSizeTracker);
        this.encoder = new CommandEncoder(l -> batchSizeTracker, null, channel, ExecutorServiceHelpers.newScheduledThreadPool(1, "Timeouts for " + host));
        this.reader.start();
    }

    private TrustManagerFactory createFromCert(String trustStoreFilePath)
            throws CertificateException, IOException, NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory factory = null;
        if (!Strings.isNullOrEmpty(trustStoreFilePath)) {
            KeyStore trustStore = CertificateUtils.createTrustStore(trustStoreFilePath);

            factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(trustStore);
        }
        return factory;
    }

    @SneakyThrows
    private Socket createClientSocket(String host, int port, ClientConfig clientConfig) {
        if (clientConfig.isEnableTlsToSegmentStore()) {
            TrustManagerFactory trustMgrFactory = createFromCert(clientConfig.getTrustStore());

            // Prepare a TLS context that uses the trust manager
            SSLContext tlsContext = SSLContext.getInstance("TLS");
            tlsContext.init(null,
                    trustMgrFactory != null ? trustMgrFactory.getTrustManagers() : null,
                    null);

            SSLSocket result = (SSLSocket) tlsContext.getSocketFactory().createSocket(host, port);

            // SSLSocket does not perform hostname verification by default. So, we must explicitly enable it.
            if (clientConfig.isValidateHostName()) {
                SSLParameters tlsParams = new SSLParameters();
                tlsParams.setEndpointIdentificationAlgorithm("HTTPS");
                result.setSSLParameters(tlsParams);
            }
            return result;

        } else {
            return new Socket(host, port);
        }
    }

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        if (closed.get()) {
            throw new ConnectionFailedException("Connection is closed");
        }
        try {
            encoder.write(cmd);
        } catch (IOException e) {
            log.warn("Error writing to connection");
            close();
            throw new ConnectionFailedException(e);
        }
    }

    @Override
    public void send(Append append) throws ConnectionFailedException {
        if (closed.get()) {
            throw new ConnectionFailedException("Connection is closed");
        }
        try {
            encoder.write(append);
        } catch (IOException e) {
            log.warn("Error writing to connection");
            close();
            throw new ConnectionFailedException(e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            reader.stop();
            try {
                socket.close();
            } catch (IOException e) {
                log.warn("Error closing socket", e);
            }
        }
    }

    @Override
    public void sendAsync(List<Append> appends, CompletedCallback callback) {
        try {
            for (Append append : appends) {
                encoder.write(append);
            }
            callback.complete(null);
        } catch (IOException e) {
            log.warn("Error writing to connection");
            close();
            callback.complete(new ConnectionFailedException(e));
        }
    }
    
    @Override
    public String toString() {
        return "TcpClientConnection [host=" + host + ", isClosed=" + closed.get() + "]";
    }

}
