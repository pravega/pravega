/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.segmentstore.server.host.handler;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.InlineExecutor;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.NameUtils.getTransactionNameFromId;
import static io.pravega.shared.NameUtils.getTransientNameFromId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Unit test for the {@link IndexAppendProcessor} class.
 */
public class IndexAppendProcessorTest {
    private StreamSegmentStore store;
    private InlineExecutor inlineExecutor;
    private Duration timeout;

    @Before
    public void setUp() {
        store = mock(StreamSegmentStore.class);
        inlineExecutor = new InlineExecutor();
        timeout = Duration.ofMinutes(1);
    }

    @After
    public void tearDown() {
        inlineExecutor.shutdown();
    }

    @Test(timeout = 6000)
    public void processAppend() {
        String segmentName = "testScope/testStream/0";
        SegmentProperties segmentProperties = StreamSegmentInformation.builder()
                                                                     .name(segmentName)
                                                                     .length(1234)
                                                                     .startOffset(123)
                                                                     .attributes(Map.of(EVENT_COUNT, 30L))
                                                                     .build();
        doReturn(CompletableFuture.completedFuture(segmentProperties)).when(store).getStreamSegmentInfo(segmentName, timeout);
        CompletableFuture<Long> future = new CompletableFuture<>();
        future.completeExceptionally(new StreamSegmentNotExistsException("Segment does not exits"));

        Mockito.when(store.append(anyString(), any(),
                        any(), any()))
                .thenReturn(future)
                .thenReturn(CompletableFuture.completedFuture(10L));
        @Cleanup
        IndexAppendProcessor appendProcessor = new IndexAppendProcessor(inlineExecutor, store);
        appendProcessor.processAppend(segmentName, NameUtils.INDEX_APPEND_EVENT_SIZE);
        appendProcessor.processAppend(segmentName, NameUtils.INDEX_APPEND_EVENT_SIZE);
        appendProcessor.runRemainingAndClose();
        verify(store, atLeast(1)).getStreamSegmentInfo(segmentName, timeout);
        verify(store, atLeast(1)).append(eq(NameUtils.getIndexSegmentName(segmentName)), any(), any(), any());
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 6000)
    public void processAppendDifferentEventSizeTest() {
        String segmentName = "testScope/testStream/0";
        SegmentProperties segmentProperties = StreamSegmentInformation.builder()
                .name(segmentName)
                .length(1234)
                .startOffset(123)
                .attributes(Map.of(EVENT_COUNT, 30L))
                .build();
        doReturn(CompletableFuture.completedFuture(segmentProperties)).when(store).getStreamSegmentInfo(segmentName, timeout);
        CompletableFuture<Long> future = new CompletableFuture<>();

        Mockito.when(store.append(anyString(), any(),
                        any(), any()))
                .thenReturn(CompletableFuture.completedFuture(10L));
        @Cleanup
        IndexAppendProcessor appendProcessor = new IndexAppendProcessor(inlineExecutor, store);
        appendProcessor.processAppend(segmentName, 32L);
        appendProcessor.processAppend(segmentName, NameUtils.INDEX_APPEND_EVENT_SIZE);
        appendProcessor.runRemainingAndClose();
        verify(store, times(1)).getStreamSegmentInfo(segmentName, timeout);
        verify(store, times(1)).append(eq(NameUtils.getIndexSegmentName(segmentName)), any(), any(), any());
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 6000)
    public void processAppendExceptionTest() {
        String segmentName = "testScope/testStream/0";
        CompletableFuture<SegmentProperties> future = new CompletableFuture<>();
        future.completeExceptionally(new StreamSegmentNotExistsException("Segment does not exits"));

        Mockito.when(store.getStreamSegmentInfo(anyString(), any()))
                .thenReturn(future);
        @Cleanup
        IndexAppendProcessor appendProcessor = new IndexAppendProcessor(inlineExecutor, store);
        appendProcessor.processAppend(segmentName, NameUtils.INDEX_APPEND_EVENT_SIZE);
        appendProcessor.runRemainingAndClose();
        verify(store, times(1)).getStreamSegmentInfo(segmentName, timeout);
        verify(store, times(0)).append(anyString(), any(), any(), any());
        verifyNoMoreInteractions(store);
    }

    @Test(timeout = 5000)
    public void processAppendForTransientSegmentTest() {
        String nonUserStreamSegment = "_system/_testStream/0";
        String transactionalSegment = getTransactionNameFromId("test/test/0", UUID.randomUUID());
        String transientSegment = getTransientNameFromId("test/test/0", UUID.randomUUID());
        @Cleanup
        IndexAppendProcessor appendProcessor = new IndexAppendProcessor(inlineExecutor, store);
        appendProcessor.processAppend(nonUserStreamSegment, NameUtils.INDEX_APPEND_EVENT_SIZE);
        appendProcessor.processAppend(transientSegment, NameUtils.INDEX_APPEND_EVENT_SIZE);
        appendProcessor.processAppend(transactionalSegment, NameUtils.INDEX_APPEND_EVENT_SIZE);

        verifyNoInteractions(store);
    }

}