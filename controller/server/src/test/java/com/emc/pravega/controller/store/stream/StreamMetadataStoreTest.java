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
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Stream metadata test
 */
public class StreamMetadataStoreTest {

    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
    private final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
    private final StreamConfiguration configuration1 = new StreamConfigurationImpl(stream1, policy1);
    private final StreamConfiguration configuration2 = new StreamConfigurationImpl(stream2, policy2);

    private final StreamMetadataStore store =
            StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, null);

    @Test
    public void testStreamMetadataStore() {

        // region createStream
        store.createStream(stream1, configuration1);
        store.createStream(stream2, configuration2);

        assertEquals(stream1, store.getConfiguration(stream1).getName());
        // endregion

        // region checkSegments
        SegmentFutures segmentFutures = store.getActiveSegments(stream1);
        assertEquals(2, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream1, 10);
        assertEquals(2, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream2);
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream2, 10);
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        // endregion

        // region scaleSegments
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        store.scale(stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20);

        segmentFutures = store.getActiveSegments(stream1);
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream1, 30);
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream1, 10);
        assertEquals(2, segmentFutures.getCurrent().size());
        assertEquals(2, segmentFutures.getFutures().size());

        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        store.scale(stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20);

        segmentFutures = store.getActiveSegments(stream1);
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(0, segmentFutures.getFutures().size());

        segmentFutures = store.getActiveSegments(stream2, 10);
        assertEquals(3, segmentFutures.getCurrent().size());
        assertEquals(1, segmentFutures.getFutures().size());

        // endregion

        // region getNextPosition

        SegmentFutures updatedPosition = new SegmentFutures(Arrays.asList(0, 5), Collections.EMPTY_MAP);
        List<SegmentFutures> futuresList = store.getNextSegments(stream2, new HashSet<>(Arrays.asList(1, 2)), Collections.singletonList(updatedPosition));
        assertEquals(1, futuresList.size());
        assertEquals(3, futuresList.get(0).getCurrent().size());
        assertEquals(1, futuresList.get(0).getFutures().size());
        assertTrue(futuresList.get(0).getCurrent().contains(4));

        updatedPosition = new SegmentFutures(Arrays.asList(0, 1, 5), Collections.EMPTY_MAP);
        futuresList = store.getNextSegments(stream2, new HashSet<>(Collections.singletonList(2)), Collections.singletonList(updatedPosition));
        assertEquals(1, futuresList.size());
        assertEquals(3, futuresList.get(0).getCurrent().size());
        assertEquals(1, futuresList.get(0).getFutures().size());

        updatedPosition = new SegmentFutures(Arrays.asList(0, 4, 5), Collections.EMPTY_MAP);
        futuresList = store.getNextSegments(stream2, new HashSet<>(Collections.singletonList(1)), Collections.singletonList(updatedPosition));
        assertEquals(1, futuresList.size());
        assertEquals(3, futuresList.get(0).getCurrent().size());
        assertEquals(1, futuresList.get(0).getFutures().size());

        // endregion
    }

}
