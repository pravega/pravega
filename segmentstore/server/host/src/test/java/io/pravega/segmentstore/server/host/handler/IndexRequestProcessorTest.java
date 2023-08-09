/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.segmentstore.server.host.handler;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.NameUtils.getIndexSegmentName;
import static org.junit.Assert.assertEquals;
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

        long offset = IndexRequestProcessor.locateOffsetForSegment(store,segmentName, 10L, true);
        assertEquals(1234, offset);
    }


}