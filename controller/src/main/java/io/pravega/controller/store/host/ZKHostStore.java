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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.cluster.Host;
import io.pravega.controller.util.ZKUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;

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

    private final NodeCache hostContainerMapNode;

    private AtomicReference<ImmutableMap<Host, Set<Integer>>> hostContainerMap;
    /**
     * The tests can add listeners to get notification when the update has happed in the store. 
     */
    private final AtomicReference<Listener> listenerRef;
    /**
     * Zookeeper based host store implementation.
     *
     * @param client                    The curator client instance.
     */
    ZKHostStore(CuratorFramework client, int containerCount) {
        Preconditions.checkNotNull(client, "client");

        zkClient = client;
        zkPath = ZKPaths.makePath("cluster", "segmentContainerHostMapping");
        hostContainerMapNode = new NodeCache(zkClient, zkPath);
        hostContainerMap = new AtomicReference<>(ImmutableMap.of());
        segmentMapper = new SegmentToContainerMapper(containerCount);
        listenerRef = new AtomicReference<>();
    }

    //Ensure required zk node is present in zookeeper.
    @Synchronized
    @SneakyThrows(Exception.class)
    private void tryInit() {
        if (!zkInit) {
            ZKUtils.createPathIfNotExists(zkClient, zkPath, SerializationUtils.serialize(new HashMap<Host,
                    Set<Integer>>()));
            this.hostContainerMapNode.getListenable().addListener(this::updateMap);
            hostContainerMapNode.start(true);

            zkInit = true;
        }
    }

    @Synchronized
    private void updateMap() {
        hostContainerMap.set(ImmutableMap.copyOf(getCurrentHostMap()));
        Listener consumer = listenerRef.get();
        if (consumer != null) {
            consumer.signal();
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
            return (Map<Host, Set<Integer>>) SerializationUtils.deserialize(hostContainerMapNode.getCurrentData().getData());
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

        Optional<Host> host = hostContainerMap.get().entrySet().stream()
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
    public Host getHostForSegment(String scope, String stream, long segmentId) {
        String qualifiedName = StreamSegmentNameUtils.getQualifiedStreamSegmentName(scope, stream, segmentId);
        return getHostForContainer(segmentMapper.getContainerId(qualifiedName));
    }
    
    @VisibleForTesting
    public void addListener(Listener listener) {
        this.listenerRef.set(listener);
    }
    
    @FunctionalInterface
    public interface Listener {
        void signal();
    }
}
