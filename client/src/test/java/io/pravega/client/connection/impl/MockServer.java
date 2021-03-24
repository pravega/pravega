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
 
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.util.ReusableLatch;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.test.common.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j 
class MockServer implements AutoCloseable {
    private final int port;
    private final Thread thread;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    @Getter
    private final LinkedBlockingQueue<WireCommand> readCommands = new LinkedBlockingQueue<>();
    private final ReusableLatch started = new ReusableLatch(false);
    @Getter
    private final CompletableFuture<OutputStream> outputStream = new CompletableFuture<OutputStream>();
    
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
            outputStream.complete(s.getOutputStream());
            @Cleanup
            InputStream stream = s.getInputStream();
            IoBuffer buffer = new IoBuffer();
            while (!stop.get()) {
                WireCommand command = TcpClientConnection.ConnectionReader.readCommand(stream, buffer);
                readCommands.add(command);
            }
        } catch (IOException e) {
            stop.set(true);
        }
    }
    
    public void sendReply(WireCommand cmd) throws IOException {
        ByteBuf buffer = Unpooled.buffer(8);
        CommandEncoder.writeMessage(cmd, buffer);
        OutputStream os = outputStream.join();
        buffer.readBytes(os, buffer.readableBytes());
        os.flush();
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