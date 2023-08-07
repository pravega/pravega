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
import io.pravega.test.common.InlineExecutor;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Unit test for the {@link IndexAppendProcessor} class.
 */
public class IndexAppendProcessorTest {
    private StreamSegmentStore store;
    private InlineExecutor inlineExecutor;
    private IndexAppendProcessor indexAppendProcessor;
    private Duration timeout;

    @Before
    public void setUp() {
        store = mock(StreamSegmentStore.class);
        inlineExecutor = new InlineExecutor();
        indexAppendProcessor = new IndexAppendProcessor(inlineExecutor, store);
        timeout = Duration.ofMinutes(1);
    }

    @After
    public void tearDown() {
        inlineExecutor.shutdown();
    }

    @Test(timeout = 5000)
    public void processAppend() {
        String segmentName = "test";
        SegmentProperties segmentProperties = StreamSegmentInformation.builder()
                                                                     .name(segmentName)
                                                                     .length(1234)
                                                                     .startOffset(123)
                                                                     .attributes(Map.of(EVENT_COUNT, 30L))
                                                                     .build();
        doReturn(CompletableFuture.completedFuture(segmentProperties)).when(store).getStreamSegmentInfo(segmentName, timeout);
        doThrow(new CompletionException(new StreamSegmentNotExistsException("Segment does not exits")))
                .doReturn(CompletableFuture.completedFuture(10L))
                .when(store).append(anyString(), any(),
                any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(store).createStreamSegment(any(), any(), any(), any());

        IndexAppendProcessor appendProcessor = new IndexAppendProcessor(inlineExecutor, store);
        appendProcessor.processAppend(segmentName);
        appendProcessor.processAppend(segmentName);

        verify(store, times(2)).getStreamSegmentInfo(segmentName, timeout);
        verify(store, times(2)).append(anyString(), any(), any(), any());
        verify(store).createStreamSegment(any(), any(), any(), any());
        verifyNoMoreInteractions(store);
    }
}