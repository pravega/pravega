package io.pravega.client.connection.impl;

import io.pravega.common.util.ReusableLatch;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.test.common.TestUtils;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j 
class MockServer implements AutoCloseable {
    @Getter
    private final int port;
    private final Thread thread;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    @Getter
    private final LinkedBlockingQueue<WireCommand> readCommands = new LinkedBlockingQueue<>();
    private final ReusableLatch started = new ReusableLatch(false);
    
    MockServer() {
        this.port = TestUtils.getAvailableListenPort();
        this.thread = new Thread(() -> listen(), "Mock server");
        thread.setDaemon(true);
    }
    
    public void start() {
        thread.start();
        started.awaitUninterruptibly();
    }
    
    private void listen() {
        try {
            @Cleanup
            ServerSocket ss = new ServerSocket(port);
            started.release();
            @Cleanup
            Socket s = ss.accept();
            @Cleanup
            InputStream stream = s.getInputStream();
            IoBuffer buffer = new IoBuffer();
            while (!stop.get()) {
                WireCommand command = TcpClientConnection.ConnectionReader.readCommand(stream, buffer);
                readCommands.add(command);
            }
        } catch (Exception e) {
            stop.set(true);
        }
    }

    public PravegaNodeUri getUri() {
        return new PravegaNodeUri("localhost", port);
    }
    
    public boolean isStopped() {
        return stop.get();
    }
    
    @Override
    public void close() throws Exception {
        stop.set(true);
        thread.interrupt();
    }
}