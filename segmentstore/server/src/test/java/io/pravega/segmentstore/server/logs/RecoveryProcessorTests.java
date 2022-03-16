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
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.reading.ContainerReadIndex;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RecoveryProcessorTests {

    private static final int FRAME_SIZE = 512;
    private final static int CONTAINER_ID = 1234567;
    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder()
            .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();
    private static final OperationSerializer SERIALIZER = OperationSerializer.DEFAULT;

    private UpdateableContainerMetadata metadata;
    private MemoryStateUpdater memoryStateUpdater;
    private Storage storage;
    private CacheStorage cacheStorage;
    private CacheManager cacheManager;
    private ReadIndex readIndex;
    private InMemoryLog inMemoryOperationLog;
    private ScheduledExecutorService executorService;

    @Before
    public void setUp() {
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        metadata = new MetadataBuilder(CONTAINER_ID).build();
        storage = InMemoryStorageFactory.newStorage(executorService);
        cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
        cacheManager = new CacheManager(CachePolicy.INFINITE, cacheStorage, executorService);
        readIndex = new ContainerReadIndex(DEFAULT_READ_INDEX_CONFIG, metadata, storage, cacheManager, executorService);
        inMemoryOperationLog = new InMemoryLog();
        memoryStateUpdater = new MemoryStateUpdater(inMemoryOperationLog, readIndex);
    }

    @After
    public void tearDown() {
        storage.close();
        cacheStorage.close();
        cacheManager.close();
        readIndex.close();
        inMemoryOperationLog.close();
        ExecutorServiceHelpers.shutdown(executorService);
    }

    @Test(expected = DataCorruptionException.class)
    public void testRecoveryWithCorruptData() throws Exception {
        MetadataCheckpointOperation checkpoint = new MetadataCheckpointOperation();
        checkpoint.setContents(new ByteArraySegment(new byte[100]));
        checkpoint.setSequenceNumber(99L);
        ByteBuffer checkpointOperationBuffer = SERIALIZER.serialize(checkpoint).asByteBuffer();
        byte[] checkpointOperationBytes = new byte[checkpointOperationBuffer.remaining()];
        checkpointOperationBuffer.get(checkpointOperationBytes);

        DeleteSegmentOperation deleteSegmentOperation = new DeleteSegmentOperation(123456);
        deleteSegmentOperation.setSequenceNumber(100L);
        ByteBuffer deleteSegmentOperationBuffer = SERIALIZER.serialize(deleteSegmentOperation).asByteBuffer();
        byte[] deleteSegmentOperationBytes = new byte[deleteSegmentOperationBuffer.remaining()];
        deleteSegmentOperationBuffer.get(deleteSegmentOperationBytes);

        ArrayList<byte[]> records = new ArrayList<>();
        records.add(checkpointOperationBytes);
        records.add(deleteSegmentOperationBytes);
        records.add(deleteSegmentOperationBytes);

        val testItems = new ArrayList<TestItem>(2);
        records.forEach(r -> testItems.add(new TestItem(r)));
        val logItems = toDataFrames(testItems);

        CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> iterator = new FrameIterator(logItems,
                0, logItems.size() - 1);
        DurableDataLog durableDataLog = spy(DurableDataLog.class);
        when(durableDataLog.getReader()).thenReturn(iterator);

        RecoveryProcessor p = new RecoveryProcessor(metadata, durableDataLog, memoryStateUpdater);
        p.performRecovery();
    }

    private ArrayList<LogItem> toDataFrames(ArrayList<TestItem> items) throws Exception {
        val result = new ArrayList<LogItem>();
        Consumer<DataFrame> addCallback = df -> result.add(new LogItem(df.getData().getReader(), df.getData().getLength(), new TestLogAddress(result.size())));
        try (val dos = new DataFrameOutputStream(FRAME_SIZE, addCallback)) {
            for (TestItem item : items) {
                dos.startNewRecord();
                long beginSequence = result.size();
                dos.write(item.data);
                dos.endRecord();
                item.address = new TestLogAddress(result.size());
                for (long s = beginSequence; s <= item.address.getSequence(); s++) {
                    item.dataFrames.add(s);
                }
            }
            dos.flush();
        }
        return result;
    }

    private static class FrameIterator implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
        private final ArrayList<LogItem> items;
        private final int endIndex;
        private int pos = 0;

        FrameIterator(ArrayList<LogItem> dataFrames, int startIndex, int endIndex) {
            this.items = Preconditions.checkNotNull(dataFrames, "dataFrames");
            Preconditions.checkElementIndex(startIndex, dataFrames.size());
            this.endIndex = Preconditions.checkElementIndex(endIndex, dataFrames.size());
            Preconditions.checkArgument(startIndex <= endIndex);
            this.pos = startIndex;
        }

        @Override
        public DurableDataLog.ReadItem getNext() {
            if (this.pos > this.endIndex) {
                return null;
            }

            return this.items.get(this.pos++);
        }

        @Override
        public void close() {
            this.pos = Integer.MAX_VALUE;
        }
    }

    @RequiredArgsConstructor
    private static class TestItem {
        private final HashSet<Long> dataFrames = new HashSet<>();
        private final byte[] data;
        private LogAddress address;
    }

    @Data
    private static class LogItem implements DurableDataLog.ReadItem {
        private final InputStream payload;
        private final int length;
        private final LogAddress address;
    }
}
