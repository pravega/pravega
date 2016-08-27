/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.controller.store.stream;

import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Stream metadata test
 */
public class StreamMetadataStoreTest {

    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100l, 2, 3);
    private final StreamConfiguration configuration1 = new StreamConfigurationImpl(stream1, policy);
    private final StreamConfiguration configuration2 = new StreamConfigurationImpl(stream2, policy);

    private final StreamMetadataStore store =
            StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, null);

    @Test
    public void testStreamMetadataStore() {

        // region createStream
        store.createStream(stream1, configuration1);
        store.createStream(stream2, configuration2);

        Assert.assertEquals(stream1, store.getConfiguration(stream1).getName());

        SegmentFutures segmentFutures = store.getActiveSegments(stream1);
        Assert.assertEquals(0, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream1, 10);
        Assert.assertEquals(0, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        // endregion

        // region createSegments
        store.addActiveSegment(stream1, 0, 0.0, 0.5, new ArrayList<>());
        store.addActiveSegment(stream1, 0, 0.5, 1.0, new ArrayList<>());

        store.addActiveSegment(stream2, 5, 0.0, 0.3, new ArrayList<>());
        store.addActiveSegment(stream2, 5, 0.3, 0.6, new ArrayList<>());
        store.addActiveSegment(stream2, 5, 0.6, 1.0, new ArrayList<>());

        segmentFutures = store.getActiveSegments(stream1);
        Assert.assertEquals(2, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream1, 10);
        Assert.assertEquals(2, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream2);
        Assert.assertEquals(3, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream2, 10);
        Assert.assertEquals(3, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        // endregion

        // region scaleSegments
        Segment segment1 = new Segment(2, 20, 30, 0.5, 0.75);
        Segment segment2 = new Segment(3, 20, 40, 0.75, 1.0);
        store.scale(stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20);

        segmentFutures = store.getActiveSegments(stream1);
        Assert.assertEquals(3, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream1, 30);
        Assert.assertEquals(3, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream1, 10);
        Assert.assertEquals(2, segmentFutures.getCurrent().size());
        Assert.assertEquals(2, segmentFutures.getFutures().size());

        Segment segment3 = new Segment(4, 20, 40, 0.0, 0.5);
        Segment segment4 = new Segment(2, 20, 30, 0.5, 0.75);
        Segment segment5 = new Segment(3, 20, 40, 0.75, 1.0);
        store.scale(stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20);

        segmentFutures = store.getActiveSegments(stream1);
        Assert.assertEquals(3, segmentFutures.getCurrent().size());
        Assert.assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream2, 10);
        Assert.assertEquals(3, segmentFutures.getCurrent().size());
        Assert.assertEquals(1, segmentFutures.getFutures().size());

        // endregion

        // region getNextPosition

        SegmentFutures updatedPosition = new SegmentFutures(Arrays.asList(0, 5), Collections.EMPTY_MAP);
        List<SegmentFutures> futuresList = store.getNextSegments(stream2, Arrays.asList(1, 2), Collections.singletonList(updatedPosition));
        Assert.assertEquals(1, futuresList.size());
        Assert.assertEquals(3, futuresList.get(0).getCurrent().size());
        Assert.assertEquals(1, futuresList.get(0).getFutures().size());
        Assert.assertTrue(futuresList.get(0).getCurrent().contains(4));

        updatedPosition = new SegmentFutures(Arrays.asList(0, 1, 5), Collections.EMPTY_MAP);
        futuresList = store.getNextSegments(stream2, Collections.singletonList(2), Collections.singletonList(updatedPosition));
        Assert.assertEquals(1, futuresList.size());
        Assert.assertEquals(3, futuresList.get(0).getCurrent().size());
        Assert.assertEquals(1, futuresList.get(0).getFutures().size());

        updatedPosition = new SegmentFutures(Arrays.asList(0, 4, 5), Collections.EMPTY_MAP);
        futuresList = store.getNextSegments(stream2, Collections.singletonList(1), Collections.singletonList(updatedPosition));
        Assert.assertEquals(1, futuresList.size());
        Assert.assertEquals(3, futuresList.get(0).getCurrent().size());
        Assert.assertEquals(1, futuresList.get(0).getFutures().size());

        // endregion
    }

}
