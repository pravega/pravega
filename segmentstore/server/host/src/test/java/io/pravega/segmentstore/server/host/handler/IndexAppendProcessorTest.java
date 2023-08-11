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

import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.test.common.InlineExecutor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
        String segmentName = "test";
        CompletableFuture<Long> future = new CompletableFuture<>();
        future.completeExceptionally(new StreamSegmentNotExistsException("Segment does not exits"));

        Mockito.when(store.append(anyString(), any(),
                        any(), any()))
                .thenReturn(future)
                .thenReturn(CompletableFuture.completedFuture(10L));

        IndexAppendProcessor appendProcessor = new IndexAppendProcessor(inlineExecutor, store);
        IndexAppend iAppend = new IndexAppend(segmentName, 1234L, 30L, 24L);
        IndexAppend iAppend2 = new IndexAppend(segmentName, 1234L, 30L, 24L);
        appendProcessor.processAppend(iAppend);
        appendProcessor.processAppend(iAppend2);

        verify(store, times(2)).append(anyString(), any(), any(), any());
        verifyNoMoreInteractions(store);
    }
}