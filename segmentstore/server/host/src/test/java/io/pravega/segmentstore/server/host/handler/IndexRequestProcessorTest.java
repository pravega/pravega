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

import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.NameUtils.getIndexSegmentName;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Unit test for the {@link IndexRequestProcessor} class.
 */

public class IndexRequestProcessorTest {
    private StreamSegmentStore store;
    private Duration timeout;

    @Before
    public void setUp() {
        store = mock(StreamSegmentStore.class);
        timeout = Duration.ofMinutes(1);
    }

    @After
    public void tearDown() {
    }

    @Test(timeout = 5000)
    public void locateOffsetForSegment() {
        String segmentName = "test";
        String indexSegmentName = getIndexSegmentName(segmentName);
        SegmentProperties segmentProperties = StreamSegmentInformation.builder()
                                                                      .name(segmentName)
                                                                      .length(1234)
                                                                      .startOffset(123)
                                                                      .attributes(Map.of(EVENT_COUNT, 30L))
                                                                      .build();
        SegmentProperties indexSegmentProperties = StreamSegmentInformation.builder()
                                                                      .name(indexSegmentName)
                                                                      .length(0)
                                                                      .startOffset(0)
                                                                      .attributes(Map.of(EVENT_COUNT, 0L))
                                                                      .build();

        doReturn(CompletableFuture.completedFuture(indexSegmentProperties)).when(store).getStreamSegmentInfo(indexSegmentName, timeout);
        doReturn(CompletableFuture.completedFuture(segmentProperties)).when(store).getStreamSegmentInfo(segmentName, timeout);

        long offset = IndexRequestProcessor.findNearestIndexedOffset(store, segmentName, 10L, true).join();
        assertEquals(1234, offset);
    }

    @Test(timeout = 5000)
    public void locateOffsetForSegmentWithFuture() throws ExecutionException, InterruptedException {
        String segmentName = "test";
        String indexSegmentName = getIndexSegmentName(segmentName);
        SegmentProperties segmentProperties = StreamSegmentInformation.builder()
                                                                      .name(indexSegmentName)
                                                                      .length(12)
                                                                      .startOffset(0)
                                                                      .attributes(Map.of(EVENT_COUNT, 1L))
                                                                      .build();
        SegmentProperties indexSegmentProperties = StreamSegmentInformation.builder()
                                                                           .name(indexSegmentName)
                                                                           .length(24)
                                                                           .startOffset(0)
                                                                           .attributes(Map.of(EVENT_COUNT, 0L))
                                                                           .build();
        ReadResultEntry readResultEntry = mock(ReadResultEntry.class);
        doNothing().when(readResultEntry).requestContent(any());
        doReturn(ReadResultEntryType.Future).when(readResultEntry).getType();
        doReturn(CompletableFuture.completedFuture(new IndexEntry(12, 1, 1234).toBytes())).when(readResultEntry).getContent();
        ReadResult result = mock(ReadResult.class);
        doReturn(true).when(result).hasNext();
        doReturn(readResultEntry).when(result).next();
        doReturn(CompletableFuture.completedFuture(segmentProperties)).when(store).getStreamSegmentInfo(eq(segmentName), any());
        doReturn(CompletableFuture.completedFuture(indexSegmentProperties)).when(store).getStreamSegmentInfo(eq(indexSegmentName), any());
        doReturn(CompletableFuture.completedFuture(result)).when(store).read(anyString(), anyLong(), anyInt(), any());
        assertEquals(0, IndexRequestProcessor.findNearestIndexedOffset(store, segmentName, 0L, false).join().longValue());
        assertEquals(12, IndexRequestProcessor.findNearestIndexedOffset(store, segmentName, 10L, true).join().longValue());
        assertEquals(12, IndexRequestProcessor.findNearestIndexedOffset(store, segmentName, 20L, true).join().longValue());
    }

    @Test(timeout = 5000)
    public void locateTruncateOffsetForSegment() {
        String segmentName = "test";
        String indexSegmentName = getIndexSegmentName(segmentName);
        SegmentProperties segmentProperties = StreamSegmentInformation.builder()
                                                                      .name(segmentName)
                                                                      .length(1234)
                                                                      .startOffset(123)
                                                                      .attributes(Map.of(EVENT_COUNT, 30L))
                                                                      .build();
        SegmentProperties indexSegmentProperties = StreamSegmentInformation.builder()
                                                                      .name(indexSegmentName)
                                                                      .length(0)
                                                                      .startOffset(0)
                                                                      .attributes(Map.of(EVENT_COUNT, 0L))
                                                                      .build();

        doReturn(CompletableFuture.completedFuture(indexSegmentProperties)).when(store).getStreamSegmentInfo(indexSegmentName, timeout);
        doReturn(CompletableFuture.completedFuture(segmentProperties)).when(store).getStreamSegmentInfo(segmentName, timeout);

        assertEquals(0, IndexRequestProcessor.locateTruncateOffsetInIndexSegment(store, segmentName, 10L).join().longValue());
    }
    
    @Test(timeout = 5000)
    public void locateTruncateOffsetForSegmentWithFuture() {
        String segmentName = "test";
        String indexSegmentName = getIndexSegmentName(segmentName);

        SegmentProperties indexSegmentProperties = StreamSegmentInformation.builder()
                                                                           .name(indexSegmentName)
                                                                           .length(24)
                                                                           .startOffset(0)
                                                                           .attributes(Map.of(EVENT_COUNT, 0L))
                                                                           .build();
        ReadResultEntry readResultEntry = mock(ReadResultEntry.class);
        doNothing().when(readResultEntry).requestContent(any());
        doReturn(ReadResultEntryType.Future).when(readResultEntry).getType();
        doReturn(CompletableFuture.completedFuture(new IndexEntry(12, 1, 1234).toBytes())).when(readResultEntry).getContent();
        ReadResult result = mock(ReadResult.class);
        doReturn(true).when(result).hasNext();
        doReturn(readResultEntry).when(result).next();
        doReturn(CompletableFuture.completedFuture(indexSegmentProperties)).when(store).getStreamSegmentInfo(eq(indexSegmentName), any());
        doReturn(CompletableFuture.completedFuture(result)).when(store).read(anyString(), anyLong(), anyInt(), any());
        assertEquals(0, IndexRequestProcessor.locateTruncateOffsetInIndexSegment(store, segmentName, 10L).join().longValue());
        assertEquals(24, IndexRequestProcessor.locateTruncateOffsetInIndexSegment(store, segmentName, 12L).join().longValue());
    }
    
}