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

package com.emc.pravega.controller.server.v1;

import static org.junit.Assert.assertEquals;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.emc.pravega.controller.server.rpc.v1.ControllerServiceImpl;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.InMemoryHostControllerStoreConfig;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;

/**
 * ConsumerApiImpl test
 */
public class ConsumerApiImplTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";

    private final StreamMetadataStore streamStore =
            StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, null);

    private Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();

    private final HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory,
            new InMemoryHostControllerStoreConfig(hostContainerMap));

    private final ControllerServiceImpl consumer = new ControllerServiceImpl(streamStore, hostStore);

    @Before
    public void prepareStreamStore() {

        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
        final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
        final StreamConfiguration configuration1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);
        final StreamConfiguration configuration2 = new StreamConfigurationImpl(SCOPE, stream2, policy2);

        // region createStream
        streamStore.createStream(stream1, configuration1);
        streamStore.createStream(stream2, configuration2);
        // endregion

        // region scaleSegments

        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        streamStore.scale(stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20);

        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        streamStore.scale(stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20);
        // endregion
    }

    @Before
    public void prepareHostStore() {
        Host host = new Host("localhost", 9090);
        hostContainerMap.put(host, new HashSet<>(Collections.singletonList(0)));
    }

    @Test
    public void testMethods() throws InterruptedException, ExecutionException, TException {
        List<Position> positions;

        positions = consumer.getPositions(SCOPE, stream1, 10, 3);
        assertEquals(2, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegments().size());
        assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(1).getOwnedSegments().size());
        assertEquals(2, positions.get(1).getFutureOwnedSegments().size());

        positions = consumer.getPositions(SCOPE, stream1, 10, 1);
        assertEquals(1, positions.size());
        assertEquals(2, positions.get(0).getOwnedSegments().size());
        assertEquals(2, positions.get(0).getFutureOwnedSegments().size());

        positions = consumer.getPositions(SCOPE, stream2, 10, 3);
        assertEquals(3, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegments().size());
        assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(1).getOwnedSegments().size());
        assertEquals(0, positions.get(1).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(2).getOwnedSegments().size());
        assertEquals(1, positions.get(2).getFutureOwnedSegments().size());


        Position newPosition = new Position(
                Collections.singletonMap(new SegmentId(SCOPE, stream2, 5), 0L),
                Collections.emptyMap());
        positions.set(2, newPosition);
        positions = consumer.updatePositions(SCOPE, stream2, positions);
        assertEquals(3, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegments().size());
        assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(1).getOwnedSegments().size());
        assertEquals(1, positions.get(1).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(2).getOwnedSegments().size());
        assertEquals(0, positions.get(2).getFutureOwnedSegments().size());

        positions = consumer.getPositions(SCOPE, stream2, 10, 2);
        assertEquals(2, positions.size());
        assertEquals(2, positions.get(0).getOwnedSegments().size());
        assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(1).getOwnedSegments().size());
        assertEquals(1, positions.get(1).getFutureOwnedSegments().size());

        positions = consumer.getPositions(SCOPE, stream1, 25, 3);
        assertEquals(3, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegments().size());
        assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(1).getOwnedSegments().size());
        assertEquals(0, positions.get(1).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(2).getOwnedSegments().size());
        assertEquals(0, positions.get(2).getFutureOwnedSegments().size());

        positions = consumer.getPositions(SCOPE, stream1, 25, 1);
        assertEquals(1, positions.size());
        assertEquals(3, positions.get(0).getOwnedSegments().size());
        assertEquals(0, positions.get(0).getFutureOwnedSegments().size());

        positions = consumer.getPositions(SCOPE, stream2, 25, 3);
        assertEquals(3, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegments().size());
        assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(1).getOwnedSegments().size());
        assertEquals(0, positions.get(1).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(2).getOwnedSegments().size());
        assertEquals(0, positions.get(2).getFutureOwnedSegments().size());

        positions = consumer.getPositions(SCOPE, stream2, 25, 2);
        assertEquals(2, positions.size());
        assertEquals(2, positions.get(0).getOwnedSegments().size());
        assertEquals(0, positions.get(0).getFutureOwnedSegments().size());
        assertEquals(1, positions.get(1).getOwnedSegments().size());
        assertEquals(0, positions.get(1).getFutureOwnedSegments().size());

    }
}
