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

package com.emc.pravega.service.server.host;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.segment.SegmentToContainerMapper;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.server.ContainerHandle;
import com.emc.pravega.service.server.SegmentContainerManager;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.ZKPaths;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * ZK based implementation for SegmentContainerManager. The controller updates the segmentContainer ownership in zk.
 * The SegmentContainerManager watches the zk entry and starts or stop appropriate segment containers.
 * <p>
 * The SegmentName -> ContainerId mapping is done by taking the hash of the StreamSegment name and then modulo the
 * number of containers (result is in hex).
 */
@Slf4j
public class ZKSegmentContainerManager implements SegmentContainerManager {

    private static final Duration INIT_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L);
    private static final Duration CLOSE_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L);
    private final SegmentContainerRegistry registry;
    private final SegmentToContainerMapper segmentToContainerMapper;

    @GuardedBy("handles")
    private final HashMap<Integer, ContainerHandle> handles;
    private final Host host;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final NodeCache segContainerHostMapping;
    private final CuratorFramework client;
    private final String clusterPath;
    private final Cluster cluster;

    /**
     * Creates a new instance of the ZKSegmentContainerManager class.
     *
     * @param containerRegistry        The SegmentContainerRegistry to manage.
     * @param segmentToContainerMapper A SegmentToContainerMapper that is used to determine the configuration of the
     *                                 cluster (i.e., number of containers).
     * @param zkClient                 ZooKeeper client.
     * @param pravegaServiceEndpoint   Pravega service endpoint details.
     * @param clusterName              Cluster Name.
     * @throws NullPointerException If containerRegistry is null.
     * @throws NullPointerException If segmentToContainerMapper is null.
     * @throws NullPointerException If logger is null.
     */
    public ZKSegmentContainerManager(SegmentContainerRegistry containerRegistry,
                                     SegmentToContainerMapper segmentToContainerMapper,
                                     CuratorFramework zkClient, Host pravegaServiceEndpoint, String clusterName) {
        Preconditions.checkNotNull(containerRegistry, "containerRegistry");
        Preconditions.checkNotNull(segmentToContainerMapper, "segmentToContainerMapper");
        Preconditions.checkNotNull(zkClient, "zkClient");
        Preconditions.checkNotNull(pravegaServiceEndpoint, "pravegaServiceEndpoint");
        Exceptions.checkNotNullOrEmpty(clusterName, "clusterName");

        this.registry = containerRegistry;
        this.segmentToContainerMapper = segmentToContainerMapper;
        this.handles = new HashMap<>();

        this.client = zkClient;
        this.clusterPath = ZKPaths.makePath("cluster", clusterName, "segmentContainerHostMapping");
        this.segContainerHostMapping = new NodeCache(zkClient, this.clusterPath);
        this.cluster = new ClusterZKImpl(zkClient, clusterName);

        this.host = pravegaServiceEndpoint;
    }

    @Override
    public CompletableFuture<Void> initialize() {
        long traceId = LoggerHelpers.traceEnter(log, "initialize");
        ensureNotClosed();
        CompletableFuture<Void> initResult = CompletableFuture.runAsync(() -> {
            // Add the node cache listener which watches ZK for changes in segment container mapping.
            addListenerSegContainerMapping(INIT_TIMEOUT_PER_CONTAINER, host);
            cluster.registerHost(host);
        }).thenRun(() -> LoggerHelpers.traceLeave(log, "initialize", traceId));

        return initResult;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Close all containers that are still open.
            ArrayList<CompletableFuture<Void>> results = new ArrayList<>();
            synchronized (this.handles) {
                close(segContainerHostMapping); // Close Node cache and its listeners.
                ArrayList<ContainerHandle> toClose = new ArrayList<>(this.handles.values());
                for (ContainerHandle handle : toClose) {
                    results.add(this.registry.stopContainer(handle, CLOSE_TIMEOUT_PER_CONTAINER)
                            .thenAccept(v -> unregisterHandle(handle.getContainerId())));
                }
            }
            // Wait for all the containers to be closed.
            FutureHelpers.allOf(results).join();
        }
    }

    private void close(final AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception e) {
            log.error("Error while closing resource", e);
        }
    }

    @VisibleForTesting
    Map<Integer, ContainerHandle> getHandles() {
        return Collections.unmodifiableMap(this.handles);
    }

    private void unregisterHandle(int containerId) {
        synchronized (this.handles) {
            Preconditions.checkState(handles.containsKey(containerId), "found unregistered handle %s", containerId);
            this.handles.remove(containerId);
        }
        log.info("Container {} has been unregistered.", containerId);
    }

    private void registerHandle(ContainerHandle handle) {
        Preconditions.checkNotNull(handle, "handle");
        synchronized (this.handles) {
            Preconditions.checkState(!this.handles.containsKey(handle.getContainerId()),
                    "handle is already registered %s", handle);
            this.handles.put(handle.getContainerId(), handle);

            handle.setContainerStoppedListener(id -> {
                unregisterHandle(handle.getContainerId());
                //TODO: Handle container failures. https://github.com/emccode/pravega/issues/154
            });
        }
        log.info("Container {} has been registered.", handle.getContainerId());
    }

    private void ensureNotClosed() {
        Exceptions.checkNotClosed(closed.get(), this);
    }

    private void addListenerSegContainerMapping(Duration timeout, Host host) {
        try {
            segContainerHostMapping.start(); //NodeCache recipe is used listen to events on the mapping data.
            segContainerHostMapping.getListenable().addListener(getSegmentContainerListener(timeout, host));
        } catch (Exception e) {
            throw new RuntimeStreamingException(
                    "Unable to start zk based cache which has the segContainer to Host mapping", e);
        }
    }

    private NodeCacheListener getSegmentContainerListener(final Duration timeout, final Host host) {
        return () -> {
            long traceId = LoggerHelpers.traceEnter(log, "segmentContainerListener");
            List<CompletableFuture<Void>> futures = initializeFromZK(host, timeout);
            FutureHelpers.allOf(futures).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            LoggerHelpers.traceLeave(log, "segmentContainerListener", traceId);
        };
    }

    /**
     * Initialize the segment containers from ZK. This function performs the following for a given host.
     * a. Fetch the assigned segment containers from zookeeper.
     * b. Get a list of segment containers that are currently running.
     * c. Start and stop the appropriate containers.
     *
     * @param hostId  Identifier of host
     * @param timeout timeout value to be passed to SegmentContainerRegistry.
     * @return List of CompletableFuture for the start and stop operations performed.
     */
    private List<CompletableFuture<Void>> initializeFromZK(Host hostId, Duration timeout) throws Exception {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        Map<Host, Set<Integer>> controlMapping = getSegmentContainerMapping();

        Set<Integer> desiredContainerList = controlMapping.entrySet().stream()
                .filter(ep -> ep.getKey().equals(hostId))
                .map(Map.Entry::getValue)
                .findFirst().orElse(Collections.<Integer>emptySet());
        log.debug("Desired list of running containers {}.", desiredContainerList);

        Collection<Integer> runningContainers = this.registry.getRegisteredContainerIds();
        log.debug("Current list of running containers {}.", runningContainers);

        Collection<Integer> containersToBeStarted = getComplement(desiredContainerList, runningContainers);
        log.debug("Containers that need to be started {}.", containersToBeStarted);

        Collection<Integer> containersToBeStopped = getComplement(runningContainers, desiredContainerList);
        log.debug("Containers that need to be stopped {}.", containersToBeStopped);

        List<CompletableFuture<Void>> futures = containersToBeStarted.stream()
                .peek(containerId -> log.info("Container to be started is {}", containerId))
                .map(containerId ->
                        this.registry.startContainer(containerId, timer.getRemaining())
                                .thenAccept(this::registerHandle))
                .collect(Collectors.toList());

        futures.addAll(containersToBeStopped.stream()
                .peek(containerId -> log.info("Container to be stopped is {}", containerId))
                .map(handles::get)
                .peek(handle -> {
                    if (handle == null) {
                        log.error("Container handle is null, container has been already unregistered");
                    }
                })
                .filter(Objects::nonNull)
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

    @SuppressWarnings("unchecked")
    private Map<Host, Set<Integer>> getSegmentContainerMapping() throws Exception {
        Map<Host, Set<Integer>> segContainerMapping = Collections.<Host, Set<Integer>>emptyMap();
        if (client.checkExists().forPath(clusterPath) != null) { //Check if path exists.
            //read data from zk.
            Optional<byte[]> containerToHostMapSer = Optional.ofNullable(client.getData().forPath(clusterPath));
            if (containerToHostMapSer.isPresent()) {
                segContainerMapping = (Map<Host, Set<Integer>>)
                        SerializationUtils.deserialize(containerToHostMapSer.get());
            }
        }
        return segContainerMapping;
    }
}
