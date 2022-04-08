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

import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.shared.protocol.netty.AdminRequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.SerializedClassRunner;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.pravega.segmentstore.server.host.handler.PravegaRequestProcessor.TIMEOUT;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@Slf4j
@RunWith(SerializedClassRunner.class)
public class AdminRequestProcessorImplTest extends PravegaRequestProcessorTest {

    @Test(timeout = 60000)
    public void testFlushToStorage() throws Exception {
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        AdminRequestProcessor processor = new AdminRequestProcessorImpl(store, mock(TableStore.class), connection);

        processor.flushToStorage(new WireCommands.FlushToStorage(0, "", 1));
        order.verify(connection).send(new WireCommands.StorageFlushed(1));
    }

    @Test(timeout = 60000)
    public void testListStorageChunks() {
        String segmentName = "dummy";
        ExtendedChunkInfo chunk = ExtendedChunkInfo.builder()
                .lengthInMetadata(10)
                .lengthInStorage(10)
                .startOffset(10)
                .chunkName("chunk")
                .existsInStorage(false)
                .build();
        WireCommands.ChunkInfo chunkInfo = new WireCommands.ChunkInfo(10, 10,
                10, "chunk", false);
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        when(store.getExtendedChunkInfo(segmentName, TIMEOUT)).thenReturn(CompletableFuture.completedFuture(List.of(chunk)));

        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        AdminRequestProcessor processor = new AdminRequestProcessorImpl(store, mock(TableStore.class), connection);

        processor.listStorageChunks(new WireCommands.ListStorageChunks("dummy", "", 1));
        order.verify(connection).send(new WireCommands.StorageChunksListed(1, List.of(chunkInfo)));
    }

    @Test(timeout = 60000)
    public void testCheckChunkSanity() {
        String chunkName = "testChunk";

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        when(store.checkChunkStorageSanity(anyInt(), anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        AdminRequestProcessor processor = new AdminRequestProcessorImpl(store, mock(TableStore.class), connection);

        processor.checkChunkSanity(new WireCommands.CheckChunkSanity(1, "testChunk", 1, null, 123));
        order.verify(connection).send(new WireCommands.ChunkSanityChecked(123));
    }

    @Test(timeout = 60000)
    public void testFailedArgumentCheckChunkSanity() {
        String chunkName = "testChunk";

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        when(store.checkChunkStorageSanity(anyInt(), anyString(), anyInt())).thenReturn(CompletableFuture.failedFuture(new IllegalArgumentException("test")));

        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        AdminRequestProcessor processor = new AdminRequestProcessorImpl(store, mock(TableStore.class), connection);

        processor.checkChunkSanity(new WireCommands.CheckChunkSanity(1, "testChunk", 1, null, 123));
        order.verify(connection).send(new WireCommands.ErrorMessage(123, "testChunk", "test", WireCommands.ErrorMessage.ErrorCode.ILLEGAL_ARGUMENT_EXCEPTION));
    }

    @Test(timeout = 60000)
    public void testFailedStateCheckChunkSanity() {
        String chunkName = "testChunk";

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("test"))).when(store).checkChunkStorageSanity(anyInt(), anyString(), anyInt());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        AdminRequestProcessor processor = new AdminRequestProcessorImpl(store, mock(TableStore.class), connection);

        processor.checkChunkSanity(new WireCommands.CheckChunkSanity(1, "testChunk", 1, null, 123));
        order.verify(connection).send(new WireCommands.ErrorMessage(123, "testChunk", "test", WireCommands.ErrorMessage.ErrorCode.ILLEGAL_STATE_EXCEPTION));
    }

}
