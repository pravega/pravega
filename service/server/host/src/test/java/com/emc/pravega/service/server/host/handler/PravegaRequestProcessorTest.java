/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.service.server.host.handler;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.WireCommands.CreateSegment;
import com.emc.pravega.common.netty.WireCommands.DeleteSegment;
import com.emc.pravega.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SealSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.SegmentDeleted;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.SegmentSealed;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.mocks.SynchronousStreamSegmentStore;
import com.emc.pravega.service.server.reading.ReadResultEntryBase;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.server.store.StreamSegmentService;
import com.emc.pravega.testcommon.InlineExecutor;
import lombok.Cleanup;
import lombok.Data;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.emc.pravega.common.metrics.Counter;
import com.emc.pravega.common.metrics.OpStatsData;

public class PravegaRequestProcessorTest {

    @Data
    private static class TestReadResult implements ReadResult {
        final long streamSegmentStartOffset;
        final int maxResultLength;
        boolean closed = false;
        final List<ReadResultEntry> results;
        long currentOffset = 0;

        @Override
        public boolean hasNext() {
            return !results.isEmpty();
        }

        @Override
        public ReadResultEntry next() {
            ReadResultEntry result = results.remove(0);
            currentOffset = result.getStreamSegmentOffset();
            return result;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public int getConsumedLength() {
            return (int) (currentOffset - streamSegmentStartOffset);
        }
    }

    private static class TestReadResultEntry extends ReadResultEntryBase {
        TestReadResultEntry(ReadResultEntryType type, long streamSegmentOffset, int requestedReadLength) {
            super(type, streamSegmentOffset, requestedReadLength);
        }

        @Override
        protected void complete(ReadResultEntryContents readResultEntryContents) {
            super.complete(readResultEntryContents);
        }

        @Override
        protected void fail(Throwable exception) {
            super.fail(exception);
        }
    }

    @Test(timeout = 20000)
    public void testReadSegment() {
        // Set up PravegaRequestProcessor instance to execute read segment request against
        String streamSegmentName = "testReadSegment";
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};
        int readLength = 1000;

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        TestReadResultEntry entry1 = new TestReadResultEntry(ReadResultEntryType.Cache, 0, readLength);
        entry1.complete(new ReadResultEntryContents(new ByteArrayInputStream(data), data.length));
        TestReadResultEntry entry2 = new TestReadResultEntry(ReadResultEntryType.Future, data.length, readLength);

        List<ReadResultEntry> results = new ArrayList<>();
        results.add(entry1);
        results.add(entry2);
        CompletableFuture<ReadResult> readResult = new CompletableFuture<>();
        readResult.complete(new TestReadResult(0, readLength, results));
        when(store.read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT)).thenReturn(readResult);

        // Execute and Verify readSegment calling stack in connection and store is executed as design.
        processor.readSegment(new ReadSegment(streamSegmentName, 0, readLength));
        verify(store).read(streamSegmentName, 0, readLength, PravegaRequestProcessor.TIMEOUT);
        verify(connection).send(new SegmentRead(streamSegmentName, 0, true, false, ByteBuffer.wrap(data)));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        entry2.complete(new ReadResultEntryContents(new ByteArrayInputStream(data), data.length));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);

        // Have readSegment once, so here readSegmentStats and readBytesStats both succeeded once.
        OpStatsData readSegmentStats = PravegaRequestProcessor.Metrics.READ_STREAM_SEGMENT.toOpStatsData();
        assertEquals(1, readSegmentStats.getNumSuccessfulEvents());
        assertEquals(0, readSegmentStats.getNumFailedEvents());

        OpStatsData readBytesStats = PravegaRequestProcessor.Metrics.READ_BYTES_STATS.toOpStatsData();
        assertEquals(1, readBytesStats.getNumSuccessfulEvents());
        assertEquals(0, readBytesStats.getNumFailedEvents());

        // Have read all the bytes in data[], so readBytes count equals to data.length.
        Counter readBytes = PravegaRequestProcessor.Metrics.READ_BYTES;
        assertEquals(Long.valueOf(data.length), readBytes.get());
    }

    @Test(timeout = 20000)
    public void testCreateSegment() throws InterruptedException, ExecutionException {
        // Set up PravegaRequestProcessor instance to execute requests against
        String streamSegmentName = "testCreateSegment";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        // Execute and Verify createSegment/getStreamSegmentInfo calling stack is executed as design.
        processor.createSegment(new CreateSegment(streamSegmentName));
        assertTrue(append(streamSegmentName, 1, store));
        processor.getStreamSegmentInfo(new GetStreamSegmentInfo(streamSegmentName));
        assertTrue(append(streamSegmentName, 2, store));
        order.verify(connection).send(new SegmentCreated(streamSegmentName));
        order.verify(connection).send(Mockito.any(StreamSegmentInfo.class));

        // TestCreateSealDelete may executed before this test case,
        // so createSegmentStats may record 1 or 2 createSegment operation here.
        OpStatsData createSegmentStats = PravegaRequestProcessor.Metrics.CREATE_STREAM_SEGMENT.toOpStatsData();
        assertNotEquals(0, createSegmentStats.getNumSuccessfulEvents());
        assertEquals(0, createSegmentStats.getNumFailedEvents());
    }

    @Test(timeout = 20000)
    public void testCreateSealDelete() throws InterruptedException, ExecutionException {
        // Set up PravegaRequestProcessor instance to execute requests against.
        String streamSegmentName = "testCreateSealDelete";
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        PravegaRequestProcessor processor = new PravegaRequestProcessor(store, connection);

        // Execute create/seal/delete Segment command.
        processor.createSegment(new CreateSegment(streamSegmentName));
        assertTrue(append(streamSegmentName, 1, store));
        processor.sealSegment(new SealSegment(streamSegmentName));
        assertFalse(append(streamSegmentName, 2, store));
        processor.deleteSegment(new DeleteSegment(streamSegmentName));
        assertFalse(append(streamSegmentName, 3, store));

        // Verify connection response with same order.
        order.verify(connection).send(new SegmentCreated(streamSegmentName));
        order.verify(connection).send(new SegmentSealed(streamSegmentName));
        order.verify(connection).send(new SegmentDeleted(streamSegmentName));
    }

    private boolean append(String streamSegmentName, int number, StreamSegmentStore store) {
        return FutureHelpers.await(store.append(streamSegmentName,
                new byte[]{(byte) number},
                new AppendContext(UUID.randomUUID(), number),
                PravegaRequestProcessor.TIMEOUT));
    }

    private static ServiceBuilderConfig getBuilderConfig() {
        Properties p = new Properties();
        ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_CONTAINER_COUNT, "1");
        ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_THREAD_POOL_SIZE, "3");
        return new ServiceBuilderConfig(p);
    }

    private static ServiceBuilder newInlineExecutionInMemoryBuilder(ServiceBuilderConfig config) {
        return ServiceBuilder.newInMemoryBuilder(config, new InlineExecutor())
                             .withStreamSegmentStore(setup -> new SynchronousStreamSegmentStore(new StreamSegmentService(
                                     setup.getContainerRegistry(), setup.getSegmentToContainerMapper())));
    }
}
