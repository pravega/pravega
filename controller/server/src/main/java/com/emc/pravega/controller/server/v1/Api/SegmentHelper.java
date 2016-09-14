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

import com.emc.pravega.common.hash.ConsistentHash;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.SegmentUri;

public class SegmentHelper {
    public static SegmentId getSegment(String stream, Segment segment) {
        return new SegmentId(stream, stream + segment.getNumber(), segment.getNumber(), -1);
    }

    public static SegmentId getSegment(String stream, int segmentNumber, int previous) {
        return new SegmentId(stream, stream + segmentNumber, segmentNumber, previous);
    }

    public static SegmentUri getSegmentUri(String stream, SegmentId id, HostControllerStore hostStore) {
        int container = ConsistentHash.hash(stream + id.getNumber(), hostStore.getContainerCount());
        Host host = hostStore.getHostForContainer(container);
        return new SegmentUri(host.getIpAddr(), host.getPort());
    }
}
