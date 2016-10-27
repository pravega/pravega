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

package com.emc.pravega.service.server;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.Closeable;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * ZK based implementation for SegmentContainerManager. The controller updates the segmentContainer ownership in zk.
 * The SegmentContainerManager watches the zk entry and starts or stop appropriate segment containers.
 * <p>
 * <p>
 * The SegmentName -> ContainerId mapping is done by taking the hash of the StreamSegment name and then modulo the
 * number of containers (result is in hex).
 */
@Slf4j
public class ZKSegmentContainerManager implements SegmentContainerManager {

    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30); //TODO: config?
    private final SegmentContainerRegistry registry;
    private final SegmentToContainerMapper segmentToContainerMapper;
    private final HashMap<Integer, ContainerHandle> handles;
    private boolean closed;

    private final NodeCache segContainerHostMapping;
    private final CuratorFramework client;
    private final String path = ZKPaths.makePath("cluster", "segmentContainerHostMapping");

    /**
     * Creates a new instance of the LocalSegmentContainerManager class.
     *
     * @param containerRegistry        The SegmentContainerRegistry to manage.
     * @param segmentToContainerMapper A SegmentToContainerMapper that is used to determine the configuration of the cluster
     *                                 (i.e., number of containers).
     * @param zkClient                 ZooKeeper client.
     * @throws NullPointerException If containerRegistry is null.
     * @throws NullPointerException If segmentToContainerMapper is null.
     * @throws NullPointerException If logger is null.
     * @throws Exception            Error while communicating with Zookeeper.
     */
    public ZKSegmentContainerManager(SegmentContainerRegistry containerRegistry, SegmentToContainerMapper segmentToContainerMapper,
                                     CuratorFramework zkClient) throws Exception {
        Preconditions.checkNotNull(containerRegistry, "containerRegistry");
        Preconditions.checkNotNull(segmentToContainerMapper, "segmentToContainerMapper");
        Preconditions.checkNotNull(zkClient, "zookeeperClient");

        this.registry = containerRegistry;
        this.segmentToContainerMapper = segmentToContainerMapper;
        this.handles = new HashMap<>();

        this.client = zkClient;
        segContainerHostMapping = new NodeCache(zkClient, path);
        segContainerHostMapping.start(true);
    }

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "initialize");
        ensureNotClosed();

        String hostAddress = getHostAddress(); //TODO: find a better implementation to fetch the host address

        List<CompletableFuture<Void>> futures = initializeFromZK(hostAddress, timeout);
        CompletableFuture<Void> initResult = FutureHelpers.allOf(futures)
                .thenRun(() -> LoggerHelpers.traceLeave(log, "initialize", traceId));

        //Add the node cache listener which watches ZK for changes in segment container mapping.
        addListenerSegContainerMapping(timeout, hostAddress);

        return initResult;
    }

    @Override
    public void close() {
        this.closed = true;
        //close Node cache and its listeners
        close(segContainerHostMapping);
        // Close all containers that are still open.
        ArrayList<CompletableFuture<Void>> results = new ArrayList<>();
        synchronized (this.handles) {
            ArrayList<ContainerHandle> toClose = new ArrayList<>(this.handles.values());
            for (ContainerHandle handle : toClose) {
                results.add(this.registry.stopContainer(handle, CLOSE_TIMEOUT)
                        .thenAccept(v -> unregisterHandle(handle.getContainerId())));
            }
        }

        // Wait for all the containers to be closed.
        FutureHelpers.allOf(results).join();
    }

    private void close(Closeable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (IOException e) {
            log.error("Error while closing resource", e);
        }
    }

    @VisibleForTesting
    public Map<Integer, ContainerHandle> getHandles() {
        return Collections.unmodifiableMap(this.handles);
    }

    private void unregisterHandle(int containerId) {
        synchronized (this.handles) {
            assert this.handles.containsKey(containerId) : "found unregistered handle " + containerId;
            this.handles.remove(containerId);
        }
        log.info("Container {} has been unregistered.", containerId);
    }

    private void registerHandle(ContainerHandle handle) {
        assert handle != null : "handle is null.";
        synchronized (this.handles) {
            assert !this.handles.containsKey(handle.getContainerId()) : "handle is already registered " + handle.getContainerId();
            this.handles.put(handle.getContainerId(), handle);

            handle.setContainerStoppedListener(id -> {
                unregisterHandle(handle.getContainerId());
                //TODO: need to restart container. BUT ONLY IF WE HAVE A FLAG SET. In benchmark mode, we rely on not auto-restarting containers.
            });
        }
        log.info("Container {} has been registered.", handle.getContainerId());
    }

    private void ensureNotClosed() {
        Exceptions.checkNotClosed(this.closed, this);
    }

    private void addListenerSegContainerMapping(Duration timeout, String hostName) {
        try {
            segContainerHostMapping.getListenable().addListener(getListenerNodeCache(timeout, hostName));
        } catch (Exception e) {
            throw new RuntimeException("Unable to start zk based cache which has the segContainer to Host mapping", e);
        }
    }

    private String getHostAddress() {
        try {
            return Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to get the Host Address", e);
        }
    }

    private NodeCacheListener getListenerNodeCache(final Duration timeout, final String hostName) {
        return () -> {
            log.debug("Listener for SegmentContainer mapping invoked.");
            List<CompletableFuture<Void>> futures = initializeFromZK(hostName, timeout);
            FutureHelpers.allOf(futures).get();
            log.debug("Completed execution of SegmentContainer listener.");
        };
    }

    /**
     * Initialize the segment containers from ZK. This function performs the following for a given host.
     * a. Fetch the assigned segment containers from zookeeper.
     * b. Get a list of segment containers that are currently running.
     * c. Start and stop the appropriate containers.
     *
     * @param hostId  - Identifier of host
     * @param timeout - timeout value to be passed to SegmentContainerRegistry.
     * @return - List of CompletableFuture for the start and stop operations performed.
     */
    private List<CompletableFuture<Void>> initializeFromZK(String hostId, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        Map<Host, Set<Integer>> controlMapping = getSegmentContainerMapping();

        Set<Integer> desiredContainerList = controlMapping.entrySet().stream()
                .filter(ep -> ep.getKey().getIpAddr().equals(hostId))
                .map(Map.Entry::getValue)
                .findFirst().orElse(Collections.EMPTY_SET);

        Collection<Integer> runningContainers = this.registry.getRegisteredContainerIds();

        Collection<Integer> containersToBeStarted = getComplement(desiredContainerList, runningContainers);
        Collection<Integer> containersToBeStopped = getComplement(runningContainers, desiredContainerList);

        List<CompletableFuture<Void>> futures = containersToBeStarted.stream()
                .map(containerId -> this.registry.startContainer(containerId, timer.getRemaining()).thenAccept(this::registerHandle))
                .collect(Collectors.toList());

        futures.addAll(containersToBeStopped.stream()
                .map(containerId -> handles.get(containerId))
                .map(containerHandle -> registry.stopContainer(containerHandle, timer.getRemaining())
                        .thenAccept(v -> unregisterHandle(containerHandle.getContainerId())))
                .collect(Collectors.toList()));
        return futures;
    }

    // This method will get the complement of a given parentSet form a parent parentSet.
    private <T> Collection<T> getComplement(Collection<T> parentSet, Collection<T> complementOf) {
        Collection<T> complement = new ArrayList<T>();
        complement.addAll(parentSet);
        complement.removeAll(complementOf);
        return complement;
    }

    private Map<Host, Set<Integer>> getSegmentContainerMapping() {
        Optional<ChildData> containerToHostMapSer = Optional.of(segContainerHostMapping.getCurrentData());
        if (containerToHostMapSer.isPresent()) {
            return (Map<Host, Set<Integer>>) SerializationUtils.deserialize(containerToHostMapSer.get().getData());
        } else {
            return Collections.EMPTY_MAP;
        }
    }
}
