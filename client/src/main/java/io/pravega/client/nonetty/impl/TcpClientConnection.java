/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.nonetty.impl;

import io.netty.buffer.Unpooled;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.AppendBatchSizeTrackerImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
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

                        callback.process((Reply) command);
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
        }
    }

    @SneakyThrows(IOException.class)
    public TcpClientConnection(String host, int port, ClientConfig clientConfig, ReplyProcessor callback) {
        socket = instantiateClientSocket(host, port, clientConfig);
        this.socket.setTcpNoDelay(true);
        SocketChannel channel = this.socket.getChannel();
        AppendBatchSizeTrackerImpl batchSizeTracker = new AppendBatchSizeTrackerImpl();
        this.reader = new ConnectionReader(host, channel, callback, batchSizeTracker);
        this.encoder = new CommandEncoder(l -> batchSizeTracker, null, channel, ExecutorServiceHelpers.newScheduledThreadPool(1, "Timeouts for " + host));
        this.reader.start();
    }

    @SneakyThrows
    private Socket instantiateClientSocket(String host, int port, ClientConfig clientConfig) {
        if (clientConfig.isEnableTlsToSegmentStore()) {
            // TODO:
            //  - Need to be able to use pem encoded files, as Pravega client applications specify such files.
            //  - Handle the scenario where no truststore is specified.
            //

            KeyStore trustStore = KeyStore.getInstance("JKS");
            trustStore.load(new FileInputStream(clientConfig.getTrustStore()), "".toCharArray());

            // Prepare the trust manager factory
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            // Prepare a TLS context that uses the trust manager
            SSLContext tlsContext = SSLContext.getInstance("TLS");
            tlsContext.init(null, tmf.getTrustManagers(), null);
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

}
