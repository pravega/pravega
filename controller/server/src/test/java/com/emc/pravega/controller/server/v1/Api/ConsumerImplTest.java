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

package com.emc.pravega.controller.server.v1.Api;

import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.InMemoryHostControllerStoreConfig;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * ConsumerImpl test
 */
public class ConsumerImplTest {

    private final String stream1 = "stream1";
    private final String stream2 = "stream2";

    private final StreamMetadataStore streamStore =
            StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, null);

    private Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();

    private final HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory,
            new InMemoryHostControllerStoreConfig().setHostContainers(hostContainerMap));

    private final ConsumerImpl consumer = new ConsumerImpl(streamStore, hostStore);

    @Before
    public void prepareStreamStore() {

        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
        final StreamConfiguration configuration1 = new StreamConfigurationImpl(stream1, policy);
        final StreamConfiguration configuration2 = new StreamConfigurationImpl(stream2, policy);

        // region createStream
        streamStore.createStream(stream1, configuration1);
        streamStore.createStream(stream2, configuration2);
        // endregion

        // region createSegments
        streamStore.addActiveSegment(stream1, 0, 0.0, 0.5, new ArrayList<>());
        streamStore.addActiveSegment(stream1, 0, 0.5, 1.0, new ArrayList<>());

        streamStore.addActiveSegment(stream2, 5, 0.0, 0.3, new ArrayList<>());
        streamStore.addActiveSegment(stream2, 5, 0.3, 0.6, new ArrayList<>());
        streamStore.addActiveSegment(stream2, 5, 0.6, 1.0, new ArrayList<>());
        // endregion

        // region scaleSegments
        Segment segment1 = new Segment(2, 20, 30, 0.5, 0.75);
        Segment segment2 = new Segment(3, 20, 40, 0.75, 1.0);
        streamStore.scale(stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20);

        Segment segment3 = new Segment(4, 20, 40, 0.0, 0.5);
        Segment segment4 = new Segment(2, 20, 30, 0.5, 0.75);
        Segment segment5 = new Segment(3, 20, 40, 0.75, 1.0);
        streamStore.scale(stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20);
        // endregion
    }

    @Before
    public void prepareHostStore() {
        Host host = new Host("localhost", 9090);
        hostContainerMap.put(host, new HashSet<>(Collections.singletonList(0)));
    }

    @Test
    public void testMethods() throws InterruptedException, ExecutionException {
        List<Position> positions;

        positions = consumer.getPositions(stream1, 10, 3).get();
        Assert.assertEquals(2, positions.size());
        Assert.assertEquals(1, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getOwnedSegments().size());
        Assert.assertEquals(2, positions.get(1).getFutureOwnedSegments().size());

        positions = consumer.getPositions(stream1, 10, 1).get();
        Assert.assertEquals(1, positions.size());
        Assert.assertEquals(2, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(2, positions.get(0).getFutureOwnedSegments().size());

        positions = consumer.getPositions(stream2, 10, 3).get();
        Assert.assertEquals(3, positions.size());
        Assert.assertEquals(1, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(1).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(2).getOwnedSegments().size());
        Assert.assertEquals(1, positions.get(2).getFutureOwnedSegments().size());


        Position newPosition = new PositionImpl(
                Collections.singletonMap(new SegmentId(stream2, stream2 + 5, 5, 2, "localhost", 9090), 0L),
                Collections.EMPTY_MAP);
        positions.set(2, newPosition);
        positions = consumer.updatePositions(stream2, positions).get();
        Assert.assertEquals(3, positions.size());
        Assert.assertEquals(1, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(2).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(2).getFutureOwnedSegments().size());

        positions = consumer.getPositions(stream2, 10, 2).get();
        Assert.assertEquals(2, positions.size());
        Assert.assertEquals(2, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getFutureOwnedSegments().size());

        positions = consumer.getPositions(stream1, 25, 3).get();
        Assert.assertEquals(3, positions.size());
        Assert.assertEquals(1, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(1).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(2).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(2).getFutureOwnedSegments().size());

        positions = consumer.getPositions(stream1, 25, 1).get();
        Assert.assertEquals(1, positions.size());
        Assert.assertEquals(3, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(0).getFutureOwnedSegments().size());

        positions = consumer.getPositions(stream2, 25, 3).get();
        Assert.assertEquals(3, positions.size());
        Assert.assertEquals(1, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(1).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(2).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(2).getFutureOwnedSegments().size());

        positions = consumer.getPositions(stream2, 25, 2).get();
        Assert.assertEquals(2, positions.size());
        Assert.assertEquals(2, positions.get(0).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        Assert.assertEquals(1, positions.get(1).getOwnedSegments().size());
        Assert.assertEquals(0, positions.get(1).getFutureOwnedSegments().size());

    }
}
