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
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.test.common.AssertExtensions;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link SegmentSelector} class.
 */
public class SegmentSelectorTests {
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo("Scope", "Stream");

    /**
     * Tests the {@link SegmentSelector#getTableSegment(String)} method.
     */
    @Test
    public void testGetSegmentByKeyFamily() {
        @Cleanup
        val context = new TestContext(10, (s, segments) -> {
            String kf = Integer.toString((int) s.getSegmentId());
            segments.byKeyFamily.put(kf, s);
        });

        for (val e : context.segments.byKeyFamily.entrySet()) {
            TableSegment s = context.selector.getTableSegment(e.getKey());
            Assert.assertEquals("Unexpected segment returned for KF " + e.getKey(), e.getValue().getSegmentId(), s.getSegmentId());
        }

        // This verifies that we cannot possibly get away with a null result from SegmentsByRange.getSegmentForKey().
        when(context.tsFactory.forSegment(isNull())).thenThrow(NullPointerException.class);
        AssertExtensions.assertThrows(
                "",
                () -> context.selector.getTableSegment("abc"),
                ex -> ex instanceof NullPointerException);
    }

    /**
     * Tests the {@link SegmentSelector#getTableSegment(String, ByteBuf)} method.
     */
    @Test
    public void testGetSegmentByKeyOrKeyFamily() {
        Function<Integer, ByteBuf> getKey = i -> Unpooled.wrappedBuffer(new byte[Integer.BYTES]).setInt(0, i);
        @Cleanup
        val context = new TestContext(10, (s, segments) -> {
            if (s.getSegmentId() % 2 == 0) {
                // Even segments are by key family.
                String kf = Integer.toString((int) s.getSegmentId());
                segments.byKeyFamily.put(kf, s);
            } else {
                // Odd segments are by key.
                segments.byKey.put(getKey.apply((int) s.getSegmentId()), s);
            }
        });

        for (val e : context.segments.byKeyFamily.entrySet()) {
            TableSegment s = context.selector.getTableSegment(e.getKey(), getKey.apply(123456));
            Assert.assertEquals("Unexpected segment returned for KF " + e.getKey(), e.getValue().getSegmentId(), s.getSegmentId());
        }

        for (val e : context.segments.byKey.entrySet()) {
            TableSegment s = context.selector.getTableSegment(null, e.getKey());
            Assert.assertEquals("Unexpected segment returned for Key " + e.getKey(), e.getValue().getSegmentId(), s.getSegmentId());
        }
    }

    /**
     * Tests the {@link SegmentSelector#close()} method.
     */
    @Test
    public void testClose() {
        @Cleanup
        val context = new TestContext(10, (s, segments) -> {
            String kf = Integer.toString((int) s.getSegmentId());
            segments.byKeyFamily.put(kf, s);
        });

        for (val e : context.segments.byKeyFamily.entrySet()) {
            TableSegment s = context.selector.getTableSegment(e.getKey());
            Assert.assertEquals("Unexpected segment returned for KF " + e.getKey(), e.getValue().getSegmentId(), s.getSegmentId());
        }

        int expectedClosed = context.segments.getSegmentCount();
        context.selector.close();
        Assert.assertEquals("Unexpected number of segments closed.", expectedClosed, context.closedCount.get());
    }

    private static class TestContext implements AutoCloseable {
        final Controller controller;
        final TestKeyValueTableSegments segments;
        final TableSegmentFactory tsFactory;
        final SegmentSelector selector;
        final AtomicInteger closedCount;

        TestContext(int segmentCount, BiConsumer<Segment, TestKeyValueTableSegments> segmentCreated) {
            this.controller = mock(Controller.class);
            this.segments = new TestKeyValueTableSegments();
            when(this.controller.getCurrentSegmentsForKeyValueTable(KVT.getScope(), KVT.getKeyValueTableName()))
                    .thenReturn(CompletableFuture.completedFuture(segments));
            this.tsFactory = mock(TableSegmentFactory.class);

            this.closedCount = new AtomicInteger();
            for (int i = 0; i < segmentCount; i++) {
                Segment s = new Segment(KVT.getScope(), KVT.getKeyValueTableName(), i);
                val ts = mock(TableSegment.class);
                when(ts.getSegmentId()).thenReturn(s.getSegmentId());
                when(this.tsFactory.forSegment(eq(s))).thenReturn(ts);

                Mockito.doAnswer(v -> {
                    this.closedCount.incrementAndGet();
                    return null;
                }).when(ts).close();

                segmentCreated.accept(s, this.segments);
            }

            this.selector = new SegmentSelector(KVT, this.controller, this.tsFactory);
            Assert.assertEquals("Unexpected result from getSegmentCount().", segmentCount, this.selector.getSegmentCount());
        }

        @Override
        public void close() {
            this.selector.close();
        }
    }

    @EqualsAndHashCode(callSuper = true)
    private static class TestKeyValueTableSegments extends KeyValueTableSegments {
        private final HashMap<String, Segment> byKeyFamily = new HashMap<>();
        private final HashMap<ByteBuf, Segment> byKey = new HashMap<>();

        public TestKeyValueTableSegments() {
            super(new TreeMap<>());
        }

        @Override
        Segment getSegmentForKey(ByteBuf keySerialization) {
            return this.byKey.get(keySerialization);
        }

        @Override
        public Segment getSegmentForKey(String keyFamily) {
            return this.byKeyFamily.get(keyFamily);
        }

        @Override
        int getSegmentCount() {
            return this.byKey.size() + this.byKeyFamily.size();
        }
    }
}
