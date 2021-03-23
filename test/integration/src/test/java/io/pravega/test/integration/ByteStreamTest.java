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
package io.pravega.test.integration;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.io.IOException;
import java.util.Arrays;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ByteStreamTest extends LeakDetectorTestSuite {

    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        super.before();
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        this.server = new PravegaConnectionListener(false, servicePort, store,  tableStore, serviceBuilder.getLowPriorityExecutor());
        this.server.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, controllerPort,
                                                       serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
    }

    @After
    public void tearDown() throws Exception {
        super.after();
        if (this.controllerWrapper != null) {
            this.controllerWrapper.close();
            this.controllerWrapper = null;
        }
        if (this.server != null) {
            this.server.close();
            this.server = null;
        }
        if (this.serviceBuilder != null) {
            this.serviceBuilder.close();
            this.serviceBuilder = null;
        }
        if (this.zkTestServer != null) {
            this.zkTestServer.close();
            this.zkTestServer = null;
        }
    }

    @Test(timeout = 30000)
    public void readWriteTest() throws IOException {
        String scope = "ByteStreamTest";
        String stream = "ReadWriteTest";

        StreamConfiguration config = StreamConfiguration.builder().build();
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(controller, null);
        // create a scope
        Boolean createScopeStatus = streamManager.createScope(scope);
        log.info("Create scope status {}", createScopeStatus);
        // create a stream
        Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
        log.info("Create stream status {}", createStreamStatus);
        @Cleanup
        ByteStreamClientFactory client = createClientFactory(scope);

        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        byte[] readBuffer = new byte[10];

        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(stream);
        @Cleanup
        ByteStreamReader reader = client.createByteStreamReader(stream);

        AssertExtensions.assertBlocks(() -> reader.read(readBuffer), () -> writer.write(payload));
        assertArrayEquals(payload, readBuffer);
        Arrays.fill(readBuffer, (byte) 0);
        writer.write(payload);
        writer.write(payload);
        writer.write(payload);
        writer.closeAndSeal();
        assertEquals(10, reader.read(readBuffer));
        assertArrayEquals(payload, readBuffer);
        for (int i = 0; i < 10; i++) {
            assertEquals(i, reader.read());
        }
        Arrays.fill(readBuffer, (byte) -1);
        assertEquals(5, reader.read(readBuffer, 0, 5));
        assertEquals(5, reader.read(readBuffer, 5, 5));
        assertArrayEquals(payload, readBuffer);
        assertEquals(-1, reader.read());
        assertEquals(-1, reader.read(readBuffer));
    }

    @Test(timeout = 30000)
    public void readWriteTestTruncate() throws IOException {
        String scope = "ByteStreamTest";
        String stream = "ReadWriteTest";

        StreamConfiguration config = StreamConfiguration.builder().build();
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(controller, null);
        // create a scope
        Boolean createScopeStatus = streamManager.createScope(scope);
        log.info("Create scope status {}", createScopeStatus);
        // create a stream
        Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
        log.info("Create stream status {}", createStreamStatus);
        @Cleanup
        ByteStreamClientFactory client = createClientFactory(scope);

        byte[] payload = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        byte[] readBuffer = new byte[10];

        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(stream);
        @Cleanup
        ByteStreamReader reader = client.createByteStreamReader(stream);
        // Verify reads and writes.
        AssertExtensions.assertBlocks(() -> reader.read(readBuffer), () -> writer.write(payload));
        assertArrayEquals(payload, readBuffer);

        //Truncate data before offset 5
        writer.truncateDataBefore(5);

        // seek to offset 4 and verify if truncation is successful.
        reader.seekToOffset(4);
        assertThrows(SegmentTruncatedException.class, reader::read);

        // seek to offset 5 and verify if we are able to read the data.
        byte[] data = new byte[]{5, 6, 7, 8, 9};
        reader.seekToOffset(5);
        byte[] readBuffer1 = new byte[5];
        int bytesRead = reader.read(readBuffer1);
        assertEquals(5, bytesRead);
        assertArrayEquals(readBuffer1, data);

        // create a new byteStream Reader.
        ByteStreamReader reader1 = client.createByteStreamReader(stream);
        // verify it is able to read
        readBuffer1 = new byte[5];
        bytesRead = reader1.read(readBuffer1);
        //verify if all the bytes are read.
        assertEquals(5, bytesRead);
        assertArrayEquals(readBuffer1, data);
    }

    @Test(timeout = 30000)
    public void readLargeWrite() throws IOException {
        String scope = "ByteStreamTest";
        String stream = "ReadWriteTest";

        StreamConfiguration config = StreamConfiguration.builder().build();
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(controller, null);
        // create a scope
        Boolean createScopeStatus = streamManager.createScope(scope);
        log.info("Create scope status {}", createScopeStatus);
        // create a stream
        Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
        log.info("Create stream status {}", createStreamStatus);
        @Cleanup
        ByteStreamClientFactory client = createClientFactory(scope);

        byte[] payload = new byte[2 * PendingEvent.MAX_WRITE_SIZE + 2];
        Arrays.fill(payload, (byte) 7);
        byte[] readBuffer = new byte[PendingEvent.MAX_WRITE_SIZE];
        Arrays.fill(readBuffer, (byte) 0);

        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(stream);
        @Cleanup
        ByteStreamReader reader = client.createByteStreamReader(stream);
        writer.write(payload);
        writer.closeAndSeal();
        assertEquals(PendingEvent.MAX_WRITE_SIZE, StreamHelpers.readAll(reader, readBuffer, 0, readBuffer.length));
        assertEquals(7, readBuffer[readBuffer.length - 1]);
        Arrays.fill(readBuffer, (byte) 0);
        assertEquals(PendingEvent.MAX_WRITE_SIZE, StreamHelpers.readAll(reader, readBuffer, 0, readBuffer.length));
        assertEquals(7, readBuffer[readBuffer.length - 1]);
        Arrays.fill(readBuffer, (byte) 0);
        assertEquals(2, reader.read(readBuffer));
        assertEquals(7, readBuffer[0]);
        assertEquals(7, readBuffer[1]);
        assertEquals(0, readBuffer[2]);
        assertEquals(-1, reader.read(readBuffer));
    }

    @Test(timeout = 30000)
    public void testBlockingRead() throws IOException {
        String scope = "ByteStreamTest";
        String stream = "ReadWriteTest";

        StreamConfiguration config = StreamConfiguration.builder().build();
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(controller, null);
        // create a scope
        Boolean createScopeStatus = streamManager.createScope(scope);
        log.info("Create scope status {}", createScopeStatus);
        // create a stream
        Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
        log.info("Create stream status {}", createStreamStatus);
        @Cleanup
        ByteStreamClientFactory client = createClientFactory(scope);

        byte[] payload = new byte[100];
        Arrays.fill(payload, (byte) 1);
        byte[] readBuffer = new byte[200];
        Arrays.fill(readBuffer, (byte) 0);

        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(stream);
        @Cleanup
        ByteStreamReader reader = client.createByteStreamReader(stream);
        AssertExtensions.assertBlocks(() -> {
            assertEquals(100, reader.read(readBuffer));
        }, () -> writer.write(payload));
        assertEquals(1, readBuffer[99]);
        assertEquals(0, readBuffer[100]);
        Arrays.fill(readBuffer, (byte) 0);
        writer.write(payload);
        assertEquals(100, reader.read(readBuffer));
        assertEquals(1, readBuffer[99]);
        assertEquals(0, readBuffer[100]);
        writer.write(payload);
        writer.write(payload);
        assertEquals(200, StreamHelpers.readAll(reader, readBuffer, 0, readBuffer.length));
        AssertExtensions.assertBlocks(() -> {
            assertEquals(100, reader.read(readBuffer));
        }, () -> writer.write(payload));
        writer.closeAndSeal();
        assertEquals(-1, reader.read());
    }

    @Test(timeout = 30000)
    public void testRecreateStream() {
        String scope = "ByteStreamTest";
        String stream = "stream";

        StreamConfiguration config = StreamConfiguration.builder().build();
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(controller, null);
        // create a scope
        assertTrue("Create scope failed", streamManager.createScope(scope));
        // create a stream
        assertTrue("Create stream failed", streamManager.createStream(scope, stream, config));
        // verify read and write.
        verifyByteClientReadWrite(scope, stream);
        // delete the stream and recreate
        assertTrue("Seal stream operation failed", streamManager.sealStream(scope, stream));
        assertTrue("Delete Stream operation failed", streamManager.deleteStream(scope, stream));
        assertTrue("Recreate stream failed", streamManager.createStream(scope, stream, config));
        // verify read and write.
        verifyByteClientReadWrite(scope, stream);
    }

    @SneakyThrows(IOException.class)
    private void verifyByteClientReadWrite(String scope, String stream) {
        @Cleanup
        ByteStreamClientFactory client = createClientFactory(scope);

        byte[] payload = new byte[100];
        Arrays.fill(payload, (byte) 1);
        byte[] readBuffer = new byte[200];
        Arrays.fill(readBuffer, (byte) 0);

        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(stream);
        @Cleanup
        ByteStreamReader reader = client.createByteStreamReader(stream);
        AssertExtensions.assertBlocks(() -> {
            assertEquals(100, reader.read(readBuffer));
        }, () -> writer.write(payload));
        assertEquals(1, readBuffer[99]);
        assertEquals(0, readBuffer[100]);
    }

    ByteStreamClientFactory createClientFactory(String scope) {
        ClientConfig config = ClientConfig.builder().build();
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(config);
        ConnectionPool pool = new ConnectionPoolImpl(config, connectionFactory);
        val inputStreamFactory = new SegmentInputStreamFactoryImpl(controller, pool);
        val outputStreamFactory = new SegmentOutputStreamFactoryImpl(controller, pool);
        val metaStreamFactory = new SegmentMetadataClientFactoryImpl(controller, pool);
        return new ByteStreamClientImpl(scope, controller, pool, inputStreamFactory, outputStreamFactory, metaStreamFactory);
    }

}
