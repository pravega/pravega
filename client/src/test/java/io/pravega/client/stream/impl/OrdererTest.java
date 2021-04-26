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
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OrdererTest {

    @Data
    private class StubEventSegmentReader implements EventSegmentReader {
        final int number;
        boolean canReadWithoutBlocking = true;
        long offset = 0;

        @Override
        public Segment getSegmentId() {
            return null;
        }

        @Override
        public void setOffset(long offset, boolean resendRequest) {
        }

        @Override
        public ByteBuffer read(long firstByteTimeout) throws EndOfSegmentException {
            return null;
        }

        @Override
        public CompletableFuture<?> fillBuffer() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isSegmentReady() {
            return canReadWithoutBlocking;
        }
    }

    @Test
    public void testChangingLogs() {
        List<StubEventSegmentReader> streams = createInputStreams(10);
        Orderer o = new Orderer();
        int[] totals = new int[streams.size()];
        for (int i = 0; i < streams.size() * 10; i++) {
            StubEventSegmentReader chosen = o.nextSegment(streams);
            totals[chosen.getNumber()]++;
        }
        for (int i = 0; i < 10; i++) {
            o.nextSegment(createInputStreams(1));
        }
        for (int i = 0; i < streams.size() * 10; i++) {
            StubEventSegmentReader chosen = o.nextSegment(streams);
            totals[chosen.getNumber()]++;
        }
        for (int value : totals) {
            assertEquals(20, value);
        }
    }

    @Test
    public void testFair() {
        List<StubEventSegmentReader> streams = createInputStreams(7);
        Orderer o = new Orderer();
        int[] totals = new int[streams.size()];
        for (int i = 0; i < streams.size() * 100; i++) {
            StubEventSegmentReader chosen = o.nextSegment(streams);
            totals[chosen.getNumber()]++;
        }
        for (int value : totals) {
            assertEquals(100, value);
        }
    }

    @Test
    public void testFindsNonblocking() {
        List<StubEventSegmentReader> streams = createInputStreams(13);
        for (StubEventSegmentReader stream : streams) {
            if (stream.getNumber() != 7) {
                stream.setCanReadWithoutBlocking(false);
            }
        }
        Orderer o = new Orderer();
        StubEventSegmentReader chosen = o.nextSegment(streams);
        assertEquals(7, chosen.getNumber());
    }

    @Test
    public void testIntWrap() {
        List<StubEventSegmentReader> streams = createInputStreams(10);
        Orderer o = new Orderer(Integer.MAX_VALUE - 5);
        int[] totals = new int[streams.size()];
        for (int i = 0; i < streams.size() * 2; i++) {
            StubEventSegmentReader chosen = o.nextSegment(streams);
            assertNotNull(chosen);
            totals[chosen.getNumber()]++;
        }
        for (int value : totals) {
            assertTrue(value >= 1);
        }
    }

    private List<StubEventSegmentReader> createInputStreams(int num) {
        Builder<StubEventSegmentReader> builder = ImmutableList.builder();
        for (int i = 0; i < num; i++) {
            builder.add(new StubEventSegmentReader(i));
        }
        return builder.build();
    }

}
