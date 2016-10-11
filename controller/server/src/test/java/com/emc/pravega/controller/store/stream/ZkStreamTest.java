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
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ZkStreamTest {

    private TestingServer zkTestServer;
    private CuratorFramework cli;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2181);
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
    }

    @After
    public void stopZookeeper() throws Exception {
        cli.close();
        zkTestServer.stop();
    }


    @Test
    public void TestZkStream() throws Exception {
        final ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 5);

        final StoreConfiguration config = new StoreConfiguration(zkTestServer.getConnectString());
        final StreamMetadataStore store = StreamStoreFactory.createStore(StreamStoreFactory.StoreType.Zookeeper, config);
        final String streamName = "test";

        StreamConfigurationImpl streamConfig = new StreamConfigurationImpl(streamName, streamName, policy);
        store.createStream(streamName, streamConfig).get();

        List<Segment> segments = store.getActiveSegments(streamName).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0, 1, 2, 3, 4).contains(x.getNumber())));

        assertEquals(store.getConfiguration(streamName).get(), streamConfig);

        // existing range 0 - .2, .2 - .4, .4 - .6, .6 - .8,, .8 - 1
        // 0, 1 -> 5.. 3 -> 6, 7
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = Lists.newArrayList(
                new AbstractMap.SimpleEntry<Double, Double>(0.0, 0.4),
                new AbstractMap.SimpleEntry<Double, Double>(0.6, 0.7),
                new AbstractMap.SimpleEntry<Double, Double>(0.7, 0.8));

        // 0 and 1 merged
        long timestamp = System.currentTimeMillis();
        store.scale(streamName, Lists.newArrayList(0, 1, 3), newRanges, timestamp).get();

        segments = store.getActiveSegments(streamName).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(2, 4, 5, 6, 7).contains(x.getNumber())));

        SegmentFutures segmentFutures = store.getActiveSegments(streamName, timestamp - 1).get();
        assertEquals(segmentFutures.getCurrent().size(), 5);
        assertTrue(segmentFutures.getCurrent().containsAll(Lists.newArrayList(0, 1, 2, 3, 4)));
    }
}
