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

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.pravega.common.io.filesystem.FileModificationEventWatcher;
import io.pravega.common.io.filesystem.FileModificationMonitor;
import io.pravega.common.io.filesystem.FileModificationPollingMonitor;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.test.common.NoOpScheduledExecutor;
import io.pravega.test.common.SecurityConfigDefaults;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PravegaConnectionListenerTest {

    @Test
    public void testCtorSetsTlsReloadFalseByDefault() {
        @Cleanup
        PravegaConnectionListener listener = new PravegaConnectionListener(false, 6222,
                mock(StreamSegmentStore.class), mock(TableStore.class), NoOpScheduledExecutor.get());
        assertFalse(listener.isEnableTlsReload());
    }

    @Test
    public void testCtorSetsTlsReloadFalseIfTlsIsDisabled() {
        @Cleanup
        PravegaConnectionListener listener = new PravegaConnectionListener(false, true,
                "localhost", 6222, mock(StreamSegmentStore.class), mock(TableStore.class),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                null, null, true, NoOpScheduledExecutor.get());
        assertFalse(listener.isEnableTlsReload());
    }

    @Test
    public void testCloseWithoutStartListeningThrowsNoException() {
        PravegaConnectionListener listener = new PravegaConnectionListener(true, true,
                "localhost", 6222, mock(StreamSegmentStore.class), mock(TableStore.class),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                null, null, true, NoOpScheduledExecutor.get());

        // Note that we do not invoke startListening() here, which among other things instantiates some of the object
        // state that is cleaned up upon invocation of close() in this line.
        listener.close();
    }

    @Test
    public void testUsesEventWatcherForNonSymbolicLinks() {
        String pathToCertificateFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME;
        String pathToKeyFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME;
        @Cleanup
        PravegaConnectionListener listener = new PravegaConnectionListener(true, true,
                "whatever", -1, mock(StreamSegmentStore.class), mock(TableStore.class),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                "dummy-tls-certificate-path", "dummy-tls-key-path", true,
                NoOpScheduledExecutor.get());

        AtomicReference<SslContext> dummySslCtx = new AtomicReference<>(null);

        FileModificationMonitor monitor = listener.prepareCertificateMonitor(pathToCertificateFile, pathToKeyFile,
                dummySslCtx);

        assertTrue("Unexpected type of FileModificationMonitor", monitor instanceof FileModificationEventWatcher);
    }

    @Test
    public void testUsesPollingMonitorForSymbolicLinks() {
        String pathToCertificateFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME;
        String pathToKeyFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME;
        @Cleanup
        PravegaConnectionListener listener = new PravegaConnectionListener(true, true,
                "whatever", -1, mock(StreamSegmentStore.class), mock(TableStore.class),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                "dummy-tls-certificate-path", "dummy-tls-key-path", true,
                NoOpScheduledExecutor.get());

        AtomicReference<SslContext> dummySslCtx = new AtomicReference<>(null);

        FileModificationMonitor monitor = listener.prepareCertificateMonitor(true,
                pathToCertificateFile, pathToKeyFile, dummySslCtx);

        assertTrue("Unexpected type of FileModificationMonitor", monitor instanceof FileModificationPollingMonitor);
    }

    @Test
    public void testPrepareCertificateMonitorThrowsExceptionWithNonExistentFile() {
        String pathToCertificateFile = SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME;
        String pathToKeyFile = SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME;
        @Cleanup
        PravegaConnectionListener listener = new PravegaConnectionListener(true, true,
                "whatever", -1, mock(StreamSegmentStore.class), mock(TableStore.class),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                "dummy-tls-certificate-path", "dummy-tls-key-path", true,
                NoOpScheduledExecutor.get());
        AtomicReference<SslContext> dummySslCtx = new AtomicReference<>(null);

        try {
            listener.prepareCertificateMonitor(false, pathToCertificateFile, pathToKeyFile,
                    dummySslCtx);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof FileNotFoundException) {
                // test succeeded
            } else {
                // test fails
                throw e;
            }
        }
    }

    @Test
    public void testEnableTlsContextReloadWhenStateIsValid() {
        String pathToCertificateFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME;
        String pathToKeyFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME;
        @Cleanup
        PravegaConnectionListener listener = new PravegaConnectionListener(true, true,
                "whatever", -1, mock(StreamSegmentStore.class), mock(TableStore.class),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                pathToCertificateFile, pathToKeyFile, true, NoOpScheduledExecutor.get());

        AtomicReference<SslContext> dummySslCtx = new AtomicReference<>(null);
        listener.enableTlsContextReload(dummySslCtx);
        // No exception indicates success.
    }

    @Test
    public void testStartListening() {
        int port = TestUtils.getAvailableListenPort();
        PravegaConnectionListener listener = new PravegaConnectionListener(false, port,
                mock(StreamSegmentStore.class), mock(TableStore.class), NoOpScheduledExecutor.get());
        listener.startListening();
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.close();
            throw new AssertionError("Port should not be available");
        } catch (IOException e) {
            // Fine, the port is being used
        }
        listener.close();
    }

    @Test
    public void testCreateEncodingStack() {
        @Cleanup
        PravegaConnectionListener listener = new PravegaConnectionListener(false, 6622,
                mock(StreamSegmentStore.class), mock(TableStore.class), NoOpScheduledExecutor.get());
        List<ChannelHandler> stack = listener.createEncodingStack("connection");
        // Check that the order of encoders is the right one.
        Assert.assertTrue(stack.get(0) instanceof ExceptionLoggingHandler);
        Assert.assertTrue(stack.get(1) instanceof CommandEncoder);
        Assert.assertTrue(stack.get(2) instanceof LengthFieldBasedFrameDecoder);
        Assert.assertTrue(stack.get(3) instanceof CommandDecoder);
        Assert.assertTrue(stack.get(4) instanceof AppendDecoder);
    }

    @Test
    public void testCreateRequestProcessor() {
        @Cleanup
        PravegaConnectionListener listener = new PravegaConnectionListener(false, 6622,
                mock(StreamSegmentStore.class), mock(TableStore.class), NoOpScheduledExecutor.get());
        Assert.assertTrue(listener.createRequestProcessor(new TrackedConnection(new ServerConnectionInboundHandler())) instanceof AppendProcessor);
    }
}
