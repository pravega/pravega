/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.host;

import com.google.common.base.Preconditions;
import io.pravega.common.cluster.Host;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.pravega.shared.segment.StreamSegmentNameUtils;
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
    public Host getHostForSegment(String scope, String stream, long segmentId) {
        String qualifiedName = StreamSegmentNameUtils.getQualifiedStreamSegmentName(scope, stream, segmentId);
        return getHostForContainer(segmentMapper.getContainerId(qualifiedName));
    }
}
