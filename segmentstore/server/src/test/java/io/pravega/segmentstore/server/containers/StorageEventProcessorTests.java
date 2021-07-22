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
package io.pravega.segmentstore.server.containers;


import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.GarbageCollector;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.val;
import org.junit.Test;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StorageEventProcessorTests  extends ThreadPooledTestSuite {
    final static int CONTAINER_ID = 42;
    @Test
    public void testInvalidArgs() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        @Cleanup val mockEventProcessor = mock(ContainerEventProcessor.class);
        AssertExtensions.assertThrows("Should not allow null eventProcessor",
                () -> {
                    @Cleanup val x = new StorageEventProcessor(CONTAINER_ID, null, v -> CompletableFuture.completedFuture(null), 10);
                },
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null callBack",
                () -> {
                    @Cleanup val x = new StorageEventProcessor(CONTAINER_ID, mockEventProcessor, null, 10);
                },
                ex -> ex instanceof NullPointerException);

        // Create valid instance
        @Cleanup val x = new StorageEventProcessor(CONTAINER_ID, mockEventProcessor, v -> CompletableFuture.completedFuture(null), 10);

        // Test the invalid parameters
        AssertExtensions.assertThrows("Should not allow null queueName",
                () -> {
                    x.addQueue(null, false);
                },
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null queueName",
                () -> {
                    x.addTask(null, GarbageCollector.TaskInfo.builder().name("test").build());
                },
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null task",
                () -> {
                    x.addTask("test", null);
                },
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertFutureThrows("Should not allow null task",
                x.addTask("nonExistent", GarbageCollector.TaskInfo.builder().name("test").build()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testForConsumerRegistration() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        @Cleanup val mockContainerEventProcessor = mock(ContainerEventProcessor.class);
        @Cleanup val mockEventProcessor = mock(ContainerEventProcessor.EventProcessor.class);
        doReturn(CompletableFuture.completedFuture(mockEventProcessor)).when(mockContainerEventProcessor).forConsumer(eq("test"), any(), any());
        doReturn(CompletableFuture.completedFuture(111L)).when(mockEventProcessor).add(any(), any());
        @Cleanup val x = new StorageEventProcessor(CONTAINER_ID, mockContainerEventProcessor, batch -> CompletableFuture.completedFuture(null), 10);
        // Test forConsumer
        x.addQueue("test", false).join();
        verify(mockContainerEventProcessor, times(1)).forConsumer(eq("test"), any(), any());

        x.addTask("test", GarbageCollector.TaskInfo.builder()
                .name("task1")
                .transactionId(1)
                .taskType(2)
                .scheduledTime(3)
                .build());
        verify(mockEventProcessor, times(1)).add(any(), any());

        // Test some tasks
        val serializer = new GarbageCollector.TaskInfo.Serializer();
        val data1 = new ArrayList();
        data1.add(serializer.serialize(GarbageCollector.TaskInfo.builder()
                .name("task1")
                .taskType(GarbageCollector.TaskInfo.DELETE_CHUNK)
                .transactionId(1)
                .build()));
        data1.add(serializer.serialize(GarbageCollector.TaskInfo.builder()
                .name("task2")
                .taskType(GarbageCollector.TaskInfo.DELETE_CHUNK)
                .transactionId(1)
                .build()));
        x.processEvents(data1).join();

        // Invalid data
        val data2 = new ArrayList<BufferView>();
        data2.add(new ByteArraySegment(new byte[0]));

        AssertExtensions.assertFutureThrows( "should throw parsing error",
            x.processEvents(data2),
            ex -> ex instanceof EOFException);
    }

    @Test
    public void testDurableQueueRegistration() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        @Cleanup val mockContainerEventProcessor = mock(ContainerEventProcessor.class);
        @Cleanup val mockEventProcessor = mock(ContainerEventProcessor.EventProcessor.class);
        doReturn(CompletableFuture.completedFuture(mockEventProcessor)).when(mockContainerEventProcessor).forDurableQueue(eq("durable"));
        doReturn(CompletableFuture.completedFuture(222L)).when(mockEventProcessor).add(any(), any());
        @Cleanup val x = new StorageEventProcessor(CONTAINER_ID, mockContainerEventProcessor, batch -> CompletableFuture.completedFuture(null), 10);

        // Test forDurableQueue
        x.addQueue("durable", true).join();
        verify(mockContainerEventProcessor, times(1)).forDurableQueue(eq("durable"));
        x.addTask("durable", GarbageCollector.TaskInfo.builder()
                .name("task1")
                .transactionId(1)
                .taskType(2)
                .scheduledTime(3)
                .build());
        verify(mockEventProcessor, times(1)).add(any(), any());
    }
}

