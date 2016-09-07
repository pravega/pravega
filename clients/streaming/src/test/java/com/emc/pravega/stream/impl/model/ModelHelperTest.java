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
package com.emc.pravega.stream.impl.model;

import com.emc.pravega.controller.stream.api.v1.ScalingPolicyType;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.PositionImpl;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ModelHelperTest {

    private static SegmentId createSegmentId(String name) {
        return new SegmentId("scope", name, 2, 1, "", 0);
    }

    private static ScalingPolicy createScalingPolicy() {
        ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
        return policy;
    }

    private static StreamConfiguration createStreamConfig(String name) {
        return new StreamConfiguration() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public ScalingPolicy getScalingingPolicy() {
                return createScalingPolicy();
            }
        };
    }

    private static Position createPosition() {
        Map<SegmentId, Long> ownedLogs = new HashMap<>();
        ownedLogs.put(createSegmentId("seg1"), 1L);

        Map<SegmentId, Long> futureOwnedLogs = new HashMap<>();
        futureOwnedLogs.put(createSegmentId("seg2"), 2L);

        return new PositionImpl(ownedLogs, futureOwnedLogs);
    }

    @Test(expected = NullPointerException.class)
    public void decodeSegmentId() throws Exception {
        final String segName = "seg1";

        com.emc.pravega.controller.stream.api.v1.SegmentId segmentID = ModelHelper.decode(createSegmentId("seg1"));
        assertEquals(segName, segmentID.getName());
        assertEquals("scope", segmentID.getScope());
        assertEquals(2, segmentID.getNumber());
        assertEquals(1, segmentID.getPrevious());

        ModelHelper.decode((SegmentId) null);
    }

    @Test(expected = NullPointerException.class)
    public void encodeSegmentId() {
        SegmentId segment = ModelHelper.encode(ModelHelper.decode(createSegmentId("seg1")));
        assertEquals("seg1", segment.getName());
        assertEquals("scope", segment.getScope());
        assertEquals(2, segment.getNumber());
        assertEquals(1, segment.getPrevious());

        ModelHelper.encode((com.emc.pravega.controller.stream.api.v1.SegmentId) null);
    }

    @Test(expected = NullPointerException.class)
    public void decodeScalingPolicy() throws Exception {
        com.emc.pravega.controller.stream.api.v1.ScalingPolicy policy = ModelHelper.decode(createScalingPolicy());
        assertEquals(ScalingPolicyType.FIXED_NUM_SEGMENTS, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());

        ModelHelper.decode((ScalingPolicy) null);
    }

    @Test(expected = NullPointerException.class)
    public void encodeScalingPolicy() throws Exception {
        ScalingPolicy policy = ModelHelper.encode(ModelHelper.decode(createScalingPolicy()));
        assertEquals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());

        ModelHelper.encode((com.emc.pravega.controller.stream.api.v1.ScalingPolicy) null);
    }

    @Test(expected = NullPointerException.class)
    public void decodeStreamConfig() throws Exception {
        StreamConfig config = ModelHelper.decode(createStreamConfig("test"));
        assertEquals("test", config.getName());
        com.emc.pravega.controller.stream.api.v1.ScalingPolicy policy = config.getPolicy();
        assertEquals(ScalingPolicyType.FIXED_NUM_SEGMENTS, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());

        ModelHelper.decode((StreamConfiguration) null);


    }

    @Test(expected = NullPointerException.class)
    public void encodeStreamConfig() throws Exception {
        StreamConfiguration config = ModelHelper.encode(ModelHelper.decode(createStreamConfig("test")));
        assertEquals("test", config.getName());
        ScalingPolicy policy = config.getScalingingPolicy();
        assertEquals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());

        ModelHelper.encode((StreamConfig) null);
    }

    @Test(expected = NullPointerException.class)
    public void decodePosition() throws Exception {
        com.emc.pravega.controller.stream.api.v1.Position position = ModelHelper.decode(createPosition());
        assertEquals(1, position.getOwnedLogs().size());
        assertEquals(1, position.getFutureOwnedLogs().size());
        assertEquals(1L, position.getOwnedLogs().get(ModelHelper.decode(createSegmentId("seg1"))).longValue());
        assertEquals(2L, position.getFutureOwnedLogs().get(ModelHelper.decode(createSegmentId("seg2"))).longValue());

        ModelHelper.decode((Position) null);
    }

    @Test(expected = NullPointerException.class)
    public void encodePosition() throws Exception {
        Position position = ModelHelper.encode(ModelHelper.decode(createPosition()));
        assertEquals(1, position.asImpl().getOwnedLogs().size());
        assertEquals(1, position.asImpl().getFutureOwnedLogs().size());
        Map<SegmentId, Long> owndedLogs = position.asImpl().getOwnedLogs();
        assertEquals(1L, position.asImpl().getOwnedLogs().get(createSegmentId("seg1")).longValue());
        assertEquals(2L, position.asImpl().getFutureOwnedLogs().get(createSegmentId("seg2")).longValue());

        ModelHelper.encode((com.emc.pravega.controller.stream.api.v1.Position) null);
    }

}