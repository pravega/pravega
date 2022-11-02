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

import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link AggregatedAppendIntegrityChecker} class.
 */
public class AggregatedAppendIntegrityCheckerTests {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Test
    public void testAddAppendInfo() {
        int containerId = 0, segmentId = 0;
        AggregatedAppendIntegrityChecker integrityChecker = new AggregatedAppendIntegrityChecker(containerId, segmentId);
        // Check that we cannot insert info from a wrong Segment in an integrity checker.
        Assert.assertThrows(IllegalArgumentException.class, () -> integrityChecker.addAppendIntegrityInfo(1, 0, 0, 0));
        Assert.assertTrue(integrityChecker.getAppendIntegrityInfo().isEmpty());
        // Add an append without hash information. This should result in the queue being empty.
        integrityChecker.addAppendIntegrityInfo(segmentId, 0, 100, StreamSegmentAppendOperation.NO_HASH);
        Assert.assertTrue(integrityChecker.getAppendIntegrityInfo().isEmpty());
        // Add some append info in the queue.
        integrityChecker.addAppendIntegrityInfo(segmentId, 0, 100, 12345);
        Assert.assertEquals(integrityChecker.getAppendIntegrityInfo().size(), 1);
        integrityChecker.close();
        Assert.assertTrue(integrityChecker.getAppendIntegrityInfo().isEmpty());
    }

    @Test
    public void testCheckAppendIntegrity() throws DataCorruptionException {
        int containerId = 0, segmentId = 0;
        AggregatedAppendIntegrityChecker integrityChecker = new AggregatedAppendIntegrityChecker(containerId, segmentId);
        // Check that we cannot check integrity for a wrong Segment in an integrity checker.
        Assert.assertThrows(IllegalArgumentException.class, () -> integrityChecker.checkAppendIntegrity(1, 0, BufferView.empty()));
        // Even though the Segment id is correct, if we have no elements in the queue, do nothing.
        integrityChecker.checkAppendIntegrity(0, 0, BufferView.empty());

        // Add some appends and generate data accordingly.
        int offset = 0;
        BufferView append1 = new ByteArraySegment(new byte[]{0, 1, 2, 3});
        BufferView append2 = new ByteArraySegment(new byte[]{4, 5, 6, 7});
        BufferView append3 = new ByteArraySegment(new byte[]{8, 9, 10, 11});
        integrityChecker.addAppendIntegrityInfo(segmentId, offset, append1.getLength(), append1.hash());
        offset += append1.getLength();
        integrityChecker.addAppendIntegrityInfo(segmentId, offset, append2.getLength(), append2.hash());
        offset += append2.getLength();
        integrityChecker.addAppendIntegrityInfo(segmentId, offset, append3.getLength(), append3.hash());
        offset += append3.getLength();
        Assert.assertEquals(integrityChecker.getAppendIntegrityInfo().size(), 3);

        // We simulate that the 3 appends are grouped into 2 aggregated appends, and append2 is split across them.
        BufferView aggregatedAppend1 = BufferView.builder().add(append1).add(append2.slice(0, 2)).build();
        BufferView aggregatedAppend2 = BufferView.builder().add(append2.slice(2, 2)).add(append3).build();
        integrityChecker.checkAppendIntegrity(segmentId, 0, aggregatedAppend1);
        integrityChecker.checkAppendIntegrity(segmentId, aggregatedAppend1.getLength(), aggregatedAppend2);
        // The 2 append has been entirely processed and removed, so there should be still 1 in the queue.
        Assert.assertEquals(integrityChecker.getAppendIntegrityInfo().size(), 2);

        // But now, let's imagine that we do have the append integrity info from the point at which the first aggregated
        // append starts, but from some further offset. We can simulate that not adding the integrity infos for the
        // first 2 appends.
        offset += append1.getLength() + append2.getLength();
        integrityChecker.addAppendIntegrityInfo(segmentId, offset, append3.getLength(), append3.hash());
        // We should still have the 2 remaining append integrity infos from previous iteration, plus the new one.
        Assert.assertEquals(integrityChecker.getAppendIntegrityInfo().size(), 3);

        // The data corresponding to the first 2 appends cannot be checks, but the data for the third one should.
        aggregatedAppend1 = BufferView.builder().add(append1).add(append2.slice(0, 2)).build();
        aggregatedAppend2 = BufferView.builder().add(append2.slice(2, 2)).add(append3).build();
        integrityChecker.checkAppendIntegrity(segmentId, 0, aggregatedAppend1);
        integrityChecker.checkAppendIntegrity(segmentId, aggregatedAppend1.getLength(), aggregatedAppend2);
    }

    @Test
    public void testCheckAppendIntegrityWithDataCorruption() throws DataCorruptionException {
        int containerId = 0, segmentId = 0;
        AggregatedAppendIntegrityChecker integrityChecker = new AggregatedAppendIntegrityChecker(containerId, segmentId);
        // Check that we cannot check integrity for a wrong Segment in an integrity checker.
        Assert.assertThrows(IllegalArgumentException.class, () -> integrityChecker.checkAppendIntegrity(1, 0, BufferView.empty()));
        // Even though the Segment id is correct, if we have no elements in the queue, do nothing.
        integrityChecker.checkAppendIntegrity(0, 0, BufferView.empty());

        // Add some appends and generate data accordingly.
        int offset = 0;
        BufferView append1 = new ByteArraySegment(new byte[]{0, 1, 2, 3});
        BufferView append2 = new ByteArraySegment(new byte[]{4, 5, 6, 7});
        BufferView append3 = new ByteArraySegment(new byte[]{8, 9, 10, 11});
        integrityChecker.addAppendIntegrityInfo(segmentId, offset, append1.getLength(), append1.hash());
        offset += append1.getLength();
        integrityChecker.addAppendIntegrityInfo(segmentId, offset, append2.getLength(), append2.hash());
        offset += append2.getLength();
        integrityChecker.addAppendIntegrityInfo(segmentId, offset, append3.getLength(), append3.hash());
        Assert.assertEquals(integrityChecker.getAppendIntegrityInfo().size(), 3);

        // Now, aggregated appends are corrupted. In this case, the data of original appends is aggregated out of order.
        BufferView aggregatedAppend1 = BufferView.builder().add(append2).add(append1.slice(0, 2)).build();
        // Expect a DataCorruptionException in this case.
        Assert.assertThrows(DataCorruptionException.class, () -> integrityChecker.checkAppendIntegrity(segmentId, 0, aggregatedAppend1));
    }
}
