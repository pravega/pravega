/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.host;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.segment.SegmentToContainerMapper;
import com.emc.pravega.stream.Segment;
import com.google.common.base.Preconditions;

import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class InMemoryHostStore implements HostControllerStore {
    private Map<Host, Set<Integer>> hostContainerMap;
    private final SegmentToContainerMapper segmentMapper;

    /**
     * Creates an in memory based host store. The data is not persisted across restarts. Useful for dev and single node
     * deployment purposes.
     *
     * @param hostContainerMap      The initial Host to container ownership information.
     */
    InMemoryHostStore(Map<Host, Set<Integer>> hostContainerMap, int containerCount) {
        Preconditions.checkNotNull(hostContainerMap, "hostContainerMap");
        this.hostContainerMap = hostContainerMap;
        segmentMapper = new SegmentToContainerMapper(containerCount);
    }

    @Override
    @Synchronized
    public Map<Host, Set<Integer>> getHostContainersMap() {
        return new HashMap<>(hostContainerMap);
    }

    @Override
    @Synchronized
    public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {
        Preconditions.checkNotNull(newMapping, "newMapping");
        hostContainerMap = new HashMap<>(newMapping);
    }

    private Host getHostForContainer(int containerId) {
        Optional<Host> host = hostContainerMap.entrySet().stream()
                .filter(x -> x.getValue().contains(containerId)).map(x -> x.getKey()).findAny();
        if (host.isPresent()) {
            log.debug("Found owning host: {} for containerId: {}", host.get(), containerId);
            return host.get();
        } else {
            throw new HostStoreException("Could not find host for container id: " + String.valueOf(containerId));
        }
    }

    @Override
    public int getContainerCount() {
        return segmentMapper.getTotalContainerCount();
    }

    @Override
    @Synchronized
    public Host getHostForSegment(String scope, String stream, int segmentNumber) {
        String qualifiedName = Segment.getScopedName(scope, stream, segmentNumber);
        return getHostForContainer(segmentMapper.getContainerId(qualifiedName));
    }
}
