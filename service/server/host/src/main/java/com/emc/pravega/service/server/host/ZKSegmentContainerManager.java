/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.ClusterType;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.CollectionHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.service.server.ContainerHandle;
import com.emc.pravega.service.server.SegmentContainerManager;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;

/**
 * ZK based implementation for SegmentContainerManager.
 * The SegmentContainerManager watches the shared zk entry that contains the segment container ownership information
 * and starts or stops appropriate segment containers locally.
 * <p>
 * The SegmentName -> ContainerId mapping is done by taking the hash of the StreamSegment name and then modulo the
 * number of containers (result is in hex).
 */
@Slf4j
class ZKSegmentContainerManager implements SegmentContainerManager {

    private static final Duration INIT_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L);
    private static final Duration CLOSE_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L);
    private final SegmentContainerRegistry registry;
    private final Map<Integer, ContainerHandle> handles;
    private final Map<Integer, CompletableFuture<ContainerHandle>> pendingStartups;

    private final Host host;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final NodeCache hostContainerMapNode;
    private final CuratorFramework client;
    private final String clusterPath;
    private final Cluster cluster;
    private final ScheduledExecutorService executor;
    private final ExecutorService curatorListenerExecutor;

    /**
     * Creates a new instance of the ZKSegmentContainerManager class.
     *
     * @param containerRegistry      The SegmentContainerRegistry to manage.
     * @param zkClient               ZooKeeper client.
     * @param pravegaServiceEndpoint Pravega service endpoint details.
     */
    ZKSegmentContainerManager(SegmentContainerRegistry containerRegistry, CuratorFramework zkClient,
                              Host pravegaServiceEndpoint, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(containerRegistry, "containerRegistry");
        Preconditions.checkNotNull(zkClient, "zkClient");
        Preconditions.checkNotNull(pravegaServiceEndpoint, "pravegaServiceEndpoint");
        Preconditions.checkNotNull(executor, "executor");

        this.registry = containerRegistry;
        this.handles = new ConcurrentHashMap<>();
        this.pendingStartups = new ConcurrentHashMap<>();
        this.client = zkClient;
        this.clusterPath = ZKPaths.makePath("cluster", "segmentContainerHostMapping");
        this.hostContainerMapNode = new NodeCache(zkClient, this.clusterPath);
        this.cluster = new ClusterZKImpl(zkClient, ClusterType.HOST);
        this.host = pravegaServiceEndpoint;
        this.executor = executor;

        // We will use a single threaded listener to serialize start/stop events.
        this.curatorListenerExecutor = Executors.newSingleThreadExecutor();
    }

    @Override
    public CompletableFuture<Void> initialize() {
        long traceId = LoggerHelpers.traceEnter(log, "initialize");
        ensureNotClosed();
        return CompletableFuture
                .runAsync(() -> {
                    // Add the node cache listener which watches ZK for changes in segment container mapping.
                    registerMappingChangeListener();
                    cluster.registerHost(host);
                    log.info("Initialized.");
                }, this.executor)
                .thenRun(() -> LoggerHelpers.traceLeave(log, "initialize", traceId));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Close all containers that are still open.
            close(hostContainerMapNode); // Close Node cache and its listeners.
            ArrayList<ContainerHandle> toClose = new ArrayList<>(this.handles.values());
            ArrayList<CompletableFuture<Void>> results = new ArrayList<>();
            for (ContainerHandle handle : toClose) {
                results.add(this.registry.stopContainer(handle, CLOSE_TIMEOUT_PER_CONTAINER)
                                         .thenAccept(v -> unregisterHandle(handle.getContainerId(), false)));
            }

            // Wait for all the containers to be closed.
            FutureHelpers.await(FutureHelpers.allOf(results), CLOSE_TIMEOUT_PER_CONTAINER.toMillis());
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
    Collection<Integer> getRegisteredContainers() {
        return this.handles.keySet();
    }

    private void unregisterHandle(int containerId, boolean enforceExistence) {
        if (this.handles.remove(containerId) == null && enforceExistence) {
            throw new IllegalStateException("found unregistered handle " + containerId);
        }

        log.info("Container {} has been unregistered.", containerId);
    }

    private ContainerHandle registerHandle(ContainerHandle handle) {
        Preconditions.checkNotNull(handle, "handle");
        Preconditions.checkState(this.handles.putIfAbsent(handle.getContainerId(), handle) == null, "handle is already registered %s", handle);
        this.pendingStartups.remove(handle.getContainerId());

        handle.setContainerStoppedListener(id -> {
            unregisterHandle(handle.getContainerId(), false);
            //TODO: Handle container failures. https://github.com/emccode/pravega/issues/154
        });
        log.info("Container {} has been registered.", handle.getContainerId());
        return handle;
    }

    private void ensureNotClosed() {
        Exceptions.checkNotClosed(closed.get(), this);
    }

    private void registerMappingChangeListener() {
        try {
            hostContainerMapNode.start(); //NodeCache recipe is used listen to events on the mapping data.
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to start zk based cache which has the segContainer to Host mapping", e);
        }

        hostContainerMapNode.getListenable().addListener(
                () -> FutureHelpers.await(initializeFromZK(), INIT_TIMEOUT_PER_CONTAINER.toMillis()),
                this.curatorListenerExecutor);
    }

    /**
     * Initialize the segment containers from ZK. This function performs the following for a given host.
     * a. Fetch the assigned segment containers from zookeeper.
     * b. Get a list of segment containers that are currently running.
     * c. Start and stop the appropriate containers.
     *
     * @return A CompletableFuture that will complete when the operation completes.
     */
    private CompletableFuture<Void> initializeFromZK() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, "initializeFromZK");
        TimeoutTimer timer = new TimeoutTimer(INIT_TIMEOUT_PER_CONTAINER);
        Map<Host, Set<Integer>> controlMapping = getHostContainerMapping();

        Set<Integer> desiredContainerList = controlMapping.entrySet().stream()
                                                          .filter(ep -> ep.getKey().equals(this.host))
                                                          .map(Map.Entry::getValue)
                                                          .findFirst().orElse(Collections.emptySet());

        Collection<Integer> runningContainers = new HashSet<>(this.handles.keySet());
        runningContainers.addAll(this.pendingStartups.keySet());
        Collection<Integer> containersToBeStarted = CollectionHelpers.filterOut(desiredContainerList, runningContainers);
        Collection<Integer> containersToBeStopped = CollectionHelpers.filterOut(runningContainers, desiredContainerList);

        log.info("Container Changes: Desired = {}, Current = {}, ToStart = {}, ToStop = {}.",
                desiredContainerList, runningContainers, containersToBeStarted, containersToBeStopped);

        List<CompletableFuture<Void>> futures = containersToBeStarted
                .stream()
                .map(containerId -> {
                    log.info("Starting Container {}.", containerId);

                    // We will retry a few times before giving up.
                    CompletableFuture<ContainerHandle> startFuture = Retry.withExpBackoff(1000, 2, 20, 5000)
                            .retryingOn(Exception.class)
                            .throwingOn(IllegalAccessException.class)
                            .runAsync(() -> this.registry.startContainer(containerId, INIT_TIMEOUT_PER_CONTAINER),
                                    this.executor)
                            .thenApply(this::registerHandle);

                    // This doesn't really handle the case of attempting to start an already started container, but
                    // that would be caught up by the call to startContainer() above.
                    this.pendingStartups.put(containerId, startFuture);
                    FutureHelpers.exceptionListener(startFuture, v -> {
                        log.warn("Start container failed: {}", v);
                        this.pendingStartups.remove(containerId);
                    });
                    return FutureHelpers.toVoid(startFuture);
                })
                .collect(Collectors.toList());

        containersToBeStopped
                .stream()
                .map(containerId -> {
                    log.info("Stopping Container {}.", containerId);
                    ContainerHandle handle = handles.get(containerId);
                    if (handle == null) {
                        // Container is not registered. Check to see if it's still starting.
                        CompletableFuture<ContainerHandle> pendingStartup = this.pendingStartups.get(containerId);
                        if (pendingStartup == null) {
                            log.warn("Container %d handle is null and no pending startup; container has been already unregistered.", containerId);
                            return null;
                        } else {
                            // Container is still starting. We cannot interrupt that, so we need to wait for a successful
                            // startup until we shut it down (if it fails to start, then we don't need to shut it down).
                            log.warn("Container %d is still starting; queuing shutdown after successful startup.", containerId);
                            return pendingStartup.thenComposeAsync(pendingHandle -> stopContainer(pendingHandle, timer.getRemaining()), this.executor);
                        }
                    } else {
                        // Plain old container shutdown.
                        return stopContainer(handle, timer.getRemaining());
                    }
                })
                .filter(Objects::nonNull)
                .forEach(futures::add);

        val result = FutureHelpers.allOf(futures);

        // Attach an exception listener, as this will be our only way of recording that an exception occurred.
        FutureHelpers.exceptionListener(result,
                ex -> log.error("Unable to update containers in local Segment Store.", ex));
        return result.thenRun(() -> LoggerHelpers.traceLeave(log, "initializeFromZK", traceId));
    }

    private CompletableFuture<Void> stopContainer(ContainerHandle handle, Duration timeout) {
        return registry.stopContainer(handle, timeout)
                       .thenRun(() -> unregisterHandle(handle.getContainerId(), true));
    }

    @SuppressWarnings("unchecked")
    private Map<Host, Set<Integer>> getHostContainerMapping() throws Exception {
        if (client.checkExists().forPath(clusterPath) != null) { //Check if path exists.
            //read data from zk.
            byte[] containerToHostMapSer = client.getData().forPath(clusterPath);
            if (containerToHostMapSer != null) {
                return (Map<Host, Set<Integer>>) SerializationUtils.deserialize(containerToHostMapSer);
            }
        }
        return Collections.emptyMap();
    }
}
