package com.emc.pravega.controller.server.v1.Api;

import com.emc.pravega.common.hash.ConsistentHash;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.stream.SegmentId;

public class SegmentHelper {
    public static SegmentId getSegmentId(String stream, Segment segment, HostControllerStore hostStore){
        int container = ConsistentHash.hash(stream + segment.getNumber(), hostStore.getContainerCount());
        Host host = hostStore.getHostForContainer(container);
        return new SegmentId(stream, stream + segment.getNumber(), segment.getNumber(), 0, host.getIpAddr(), host.getPort());

    }
}
