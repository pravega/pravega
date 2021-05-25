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
package io.pravega.segmentstore.server.writer;

import io.pravega.segmentstore.server.ManualTimer;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import java.util.LinkedList;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link WriterState} class.
 */
public class WriterStateTests {
    /**
     * Tests basic functionality (except Forced Flushes).
     */
    @Test
    public void testBasicFeatures() {
        val s = new WriterState();
        val timer = new ManualTimer();
        timer.setElapsedMillis(0);

        s.recordIterationStarted(timer);
        timer.setElapsedMillis(1);
        Assert.assertEquals(1, s.getElapsedSinceIterationStart(timer).toMillis());
        Assert.assertEquals(1, s.getIterationId());

        s.setLastTruncatedSequenceNumber(10);
        Assert.assertEquals(10, s.getLastTruncatedSequenceNumber());

        Assert.assertFalse(s.getLastIterationError());
        s.recordIterationError();
        Assert.assertTrue(s.getLastIterationError());
        s.recordIterationStarted(timer);
        Assert.assertFalse(s.getLastIterationError());
        Assert.assertEquals(2, s.getIterationId());

        s.setLastReadSequenceNumber(123);
        Assert.assertEquals(123, s.getLastReadSequenceNumber());
    }

    /**
     * Tests {@link WriterState#getLastRead()} and {@link WriterState#setLastRead}.
     */
    @Test
    public void testLastRead() {
        val s = new WriterState();
        Assert.assertNull(s.getLastRead());

        val q = new LinkedList<Operation>();
        q.add(new MetadataCheckpointOperation());
        q.add(new StorageMetadataCheckpointOperation());

        s.setLastRead(q);
        Assert.assertSame(q, s.getLastRead());

        q.removeFirst();
        Assert.assertEquals(1, s.getLastRead().size());

        q.removeFirst();
        Assert.assertNull(s.getLastRead());
    }

    /**
     * Tests {@link WriterState#setForceFlush}.
     */
    @Test
    public void testForceFlush() throws Exception {
        val s = new WriterState();
        s.setLastReadSequenceNumber(10);

        // 1. When the desired sequence number has already been acknowledged.
        s.setLastTruncatedSequenceNumber(8);
        val r1 = s.setForceFlush(8);
        Assert.assertTrue("Expected an already completed result.", r1.isDone());
        Assert.assertFalse("Expected a result with isAnythingFlushed == false.", r1.join());
        Assert.assertFalse("Not expecting force flush flag to be set.", s.isForceFlush());

        // 2. With something flushed.
        val r2 = s.setForceFlush(100);
        Assert.assertFalse(r2.isDone());
        Assert.assertTrue("Expecting force flush flag to be set.", s.isForceFlush());
        AssertExtensions.assertThrows("", () -> s.setForceFlush(101), ex -> ex instanceof IllegalStateException);

        s.setLastReadSequenceNumber(90);
        s.recordFlushComplete(new WriterFlushResult());
        Assert.assertFalse(r2.isDone());
        s.setLastReadSequenceNumber(100);
        Assert.assertFalse(r2.isDone());
        s.recordFlushComplete(new WriterFlushResult().withFlushedBytes(1));
        TestUtils.await(r2::isDone, 5, 30000);
        Assert.assertTrue("Unexpected result when something was flushed.", r2.join());
        Assert.assertFalse("Expecting force flush flag to be cleared.", s.isForceFlush());

        // 3. Nothing flushed.
        val r3 = s.setForceFlush(200);
        Assert.assertFalse(r3.isDone());
        Assert.assertTrue("Expecting force flush flag to be set.", s.isForceFlush());
        s.setLastReadSequenceNumber(201);
        s.recordFlushComplete(new WriterFlushResult());
        TestUtils.await(r3::isDone, 5, 30000);
        Assert.assertFalse("Unexpected result when nothing was flushed.", r3.join());
        Assert.assertFalse("Expecting force flush flag to be cleared.", s.isForceFlush());
    }
}
