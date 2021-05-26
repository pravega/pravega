/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.host;

import com.google.common.base.Preconditions;
import io.pravega.common.cluster.Host;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
        segmentMapper = new SegmentToContainerMapper(containerCount, true);
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
                .filter(x -> x.getValue().contains(containerId)).map(Map.Entry::getKey).findAny();
        if (host.isPresent()) {
            log.debug("Found owning host: {} for containerId: {}", host.get(), containerId);
            return host.get();
        } else {
            throw new HostStoreException("Could not find host for container id: " + containerId);
        }
    }

    @Override
    public int getContainerCount() {
        return segmentMapper.getTotalContainerCount();
    }

    @Override
    @Synchronized
    public Host getHostForSegment(String scope, String stream, long segmentId) {
        String qualifiedName = NameUtils.getQualifiedStreamSegmentName(scope, stream, segmentId);
        return getHostForContainer(segmentMapper.getContainerId(qualifiedName));
    }

    @Override
    @Synchronized
    public Host getHostForTableSegment(String tableName) {
        return getHostForContainer(segmentMapper.getContainerId(tableName));
    }
}
