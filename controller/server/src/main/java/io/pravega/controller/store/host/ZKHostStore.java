/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.host;

import io.pravega.common.cluster.Host;
import io.pravega.common.segment.SegmentToContainerMapper;
import io.pravega.controller.util.ZKUtils;
import io.pravega.stream.Segment;
import com.google.common.base.Preconditions;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Zookeeper based implementation of the HostControllerStore.
 */
@Slf4j
public class ZKHostStore implements HostControllerStore {

    //The path used to store the segment container mapping.
    private final String zkPath;

    //The supplied curator framework instance.
    private final CuratorFramework zkClient;

    //To bootstrap zookeeper on first use.
    private volatile boolean zkInit = false;

    private final SegmentToContainerMapper segmentMapper;

    /**
     * Zookeeper based host store implementation.
     *
     * @param client                    The curator client instance.
     */
    ZKHostStore(CuratorFramework client, int containerCount) {
        Preconditions.checkNotNull(client, "client");

        zkClient = client;
        zkPath = ZKPaths.makePath("cluster", "segmentContainerHostMapping");
        segmentMapper = new SegmentToContainerMapper(containerCount);
    }

    //Ensure required zk node is present in zookeeper.
    @Synchronized
    private void tryInit() {
        if (!zkInit) {
            ZKUtils.createPathIfNotExists(zkClient, zkPath, SerializationUtils.serialize(new HashMap<Host,
                    Set<Integer>>()));
            zkInit = true;
        }
    }

    @Override
    public Map<Host, Set<Integer>> getHostContainersMap() {
        tryInit();

        return getCurrentHostMap();
    }

    @SuppressWarnings("unchecked")
    private Map<Host, Set<Integer>> getCurrentHostMap() {
        try {
            return (Map<Host, Set<Integer>>) SerializationUtils.deserialize(zkClient.getData().forPath(zkPath));
        } catch (Exception e) {
            throw new HostStoreException("Failed to fetch segment container map from zookeeper", e);
        }
    }

    @Override
    public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {
        Preconditions.checkNotNull(newMapping, "newMapping");
        tryInit();
        byte[] serializedMap;
        if (newMapping instanceof Serializable) {
            serializedMap = SerializationUtils.serialize((Serializable) newMapping);
        } else {
            serializedMap = SerializationUtils.serialize(new HashMap<>(newMapping));
        }
        try {
            zkClient.setData().forPath(zkPath, serializedMap);
            log.info("Successfully updated segment container map");
        } catch (Exception e) {
            throw new HostStoreException("Failed to persist segment container map to zookeeper", e);
        }
    }

    private Host getHostForContainer(int containerId) {
        tryInit();

        Map<Host, Set<Integer>> mapping = getCurrentHostMap();
        Optional<Host> host = mapping.entrySet().stream()
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
    public Host getHostForSegment(String scope, String stream, int segmentNumber) {
        String qualifiedName = Segment.getScopedName(scope, stream, segmentNumber);
        return getHostForContainer(segmentMapper.getContainerId(qualifiedName));
    }
}
