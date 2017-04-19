/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.CollectionHelpers;
import com.emc.pravega.service.server.ContainerHandle;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Monitors the current set of running segment containers and ensure it matches the ownership assigment for this host.
 * This monitor watches the shared zk entry that contains the segment container ownership information
 * and starts or stops appropriate segment containers locally. Any start failures are periodically retried until
 * the desired ownership state is achieved.
 */
@Slf4j
public class ZKSegmentContainerMonitor implements AutoCloseable {

    private static final Duration INIT_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L);
    private static final Duration CLOSE_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L);
    private static final Duration MONITOR_INTERVAL = Duration.ofSeconds(10);

    // The host entry for which we are monitoring the container assignments.
    private final Host host;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // The zkNode which contains the segment container to host assignment.
    private final NodeCache hostContainerMapNode;
    private final SegmentContainerRegistry registry;

    // The list of container handles which are currently running in this node.
    private final Map<Integer, ContainerHandle> handles;

    // The list of containers which have ongoing start/stop tasks pending. This list is needed to ensure
    // we don't initiate conflicting tasks for the same containerId.
    private final Set<Integer> pendingTasks;

    // The single threaded executor used to monitor the shared segment container assignment info.
    private ScheduledExecutorService scheduledExecutor = null;

    /**
     * Creates an instance of ZKSegmentContainerMonitor.
     *
     * @param containerRegistry         The registry used to control the container state.
     * @param zkClient                  The curator client.
     * @param pravegaServiceEndpoint    The pravega endpoint for which we need to fetch the container assignment.
     */
    ZKSegmentContainerMonitor(SegmentContainerRegistry containerRegistry, CuratorFramework zkClient,
                              Host pravegaServiceEndpoint) {
        Preconditions.checkNotNull(zkClient, "zkClient");

        this.registry = Preconditions.checkNotNull(containerRegistry, "containerRegistry");
        this.host = Preconditions.checkNotNull(pravegaServiceEndpoint, "pravegaServiceEndpoint");
        this.handles = new ConcurrentHashMap<>();
        this.pendingTasks = new ConcurrentSkipListSet<>();
        String clusterPath = ZKPaths.makePath("cluster", "segmentContainerHostMapping");
        this.hostContainerMapNode = new NodeCache(zkClient, clusterPath);
    }

    /**
     * Initialize the monitor. This will start the monitor thread which will process the start/stop container events.
     */
    public void initialize() {
        initialize(MONITOR_INTERVAL);
    }

    @VisibleForTesting
    @SneakyThrows(Exception.class)
    public void initialize(Duration monitorInterval) {
        Exceptions.checkNotClosed(closed.get(), this);

        this.hostContainerMapNode.start();
        this.scheduledExecutor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("monitor-segment-containers-%d").setDaemon(true).build());
        this.scheduledExecutor.scheduleWithFixedDelay(
                this::checkAssignment, 0L, monitorInterval.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        Preconditions.checkState(closed.compareAndSet(false, true));
        try {
            this.hostContainerMapNode.close();
        } catch (IOException e) {
            // Ignoring exception on shutdown.
            log.warn("Failed to close hostContainerMapNode {}", e);
        }
        if (this.scheduledExecutor != null) {
            this.scheduledExecutor.shutdownNow();
        }

        ArrayList<ContainerHandle> toClose = new ArrayList<>(this.handles.values());
        ArrayList<CompletableFuture<Void>> results = new ArrayList<>();
        for (ContainerHandle handle : toClose) {
            results.add(this.registry.stopContainer(handle, CLOSE_TIMEOUT_PER_CONTAINER)
                    .thenAccept(v -> unregisterHandle(handle.getContainerId())));
        }

        // Wait for all the containers to be closed.
        FutureHelpers.await(FutureHelpers.allOf(results), CLOSE_TIMEOUT_PER_CONTAINER.toMillis());
    }

    @VisibleForTesting
    Collection<Integer> getRegisteredContainers() {
        return this.handles.keySet();
    }

    // The container assignment monitor. This is executed in a single threaded executor and hence there will never be
    // parallel invocations of this method.
    // This method will fetch the current owned containers for this host and ensures that the local containers' state
    // reflects this.
    private void checkAssignment() {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "checkAssignment");

        // Fetch the list of containers that is supposed to be owned by this host.
        Set<Integer> desiredList = getDesiredContainerList();
        if (desiredList != null) {
            Collection<Integer> runningContainers = new HashSet<>(this.handles.keySet());
            Collection<Integer> containersPendingTasks = new HashSet<>(this.pendingTasks);

            // Filter out containers which have pending tasks so we don't initiate conflicting events on the same
            // containers. Events for these containers will be tried on subsequent runs of this executor.
            Collection<Integer> containersToBeStarted = CollectionHelpers.filterOut(desiredList, runningContainers);
            containersToBeStarted = CollectionHelpers.filterOut(containersToBeStarted, containersPendingTasks);

            Collection<Integer> containersToBeStopped = CollectionHelpers.filterOut(runningContainers, desiredList);
            containersToBeStopped = CollectionHelpers.filterOut(containersToBeStopped, containersPendingTasks);

            log.debug("Container Changes: Desired = {}, Current = {}, ToStart = {}, ToStop = {}.",
                    desiredList, runningContainers, containersToBeStarted, containersToBeStopped);
            if (!containersPendingTasks.isEmpty()) {
                log.info("Containers with pending start/stop tasks = {}", containersPendingTasks);
            }

            // Initiate the start and stop tasks asynchronously.
            containersToBeStarted.stream().forEach(this::startContainer);
            containersToBeStopped.stream().forEach(this::stopContainer);
        } else {
            log.warn("No segment container assignments found");
        }
        LoggerHelpers.traceLeave(log, "checkAssignment", traceId);
    }

    // Stop the container given its id.
    private CompletableFuture<Void> stopContainer(int containerId) {
        log.info("Stopping Container {}.", containerId);
        ContainerHandle handle = handles.get(containerId);
        if (handle == null) {
            log.warn("Container {} handle is null, container is pending start or already unregistered.", containerId);
            return null;
        } else {
            this.pendingTasks.add(containerId);
            return registry.stopContainer(handle, CLOSE_TIMEOUT_PER_CONTAINER)
                    .whenComplete((aVoid, throwable) -> {
                        if (throwable != null) {
                            log.warn("Stopping container {} failed: {}", containerId, throwable);
                        }

                        // We remove the handle and don't attempt retry on stop container failures.
                        unregisterHandle(containerId);
                        this.pendingTasks.remove(containerId);
                    });
        }
    }

    private CompletableFuture<Void> startContainer(int containerId) {
        log.info("Starting Container {}.", containerId);
        this.pendingTasks.add(containerId);
        return this.registry.startContainer(containerId, INIT_TIMEOUT_PER_CONTAINER)
                .thenAccept(this::registerHandle)
                .whenComplete((u, throwable) -> {
                    if (throwable != null) {
                        log.warn("Starting container {} failed: {}", containerId, throwable);
                    }
                    this.pendingTasks.remove(containerId);
                });
    }

    private void unregisterHandle(int containerId) {
        if (this.handles.remove(containerId) == null) {
            log.warn("found unregistered handle {}", containerId);
        }
        log.info("Container {} has been unregistered", containerId);
    }

    private void registerHandle(ContainerHandle handle) {
        Preconditions.checkNotNull(handle, "handle");
        Preconditions.checkState(this.handles.putIfAbsent(handle.getContainerId(), handle) == null,
                "handle for container {} is already registered {}", handle.getContainerId(), handle);

        handle.setContainerStoppedListener(this::unregisterHandle);
        log.info("Container {} has been registered", handle.getContainerId());
    }

    @SuppressWarnings("unchecked")
    private Set<Integer> getDesiredContainerList() {
        log.debug("Fetching the latest container assignment from zookeeper");
        if (hostContainerMapNode.getCurrentData() != null) { //Check if path exists.
            //read data from zk.
            byte[] containerToHostMapSer = hostContainerMapNode.getCurrentData().getData();
            if (containerToHostMapSer != null) {
                val controlMapping = (Map<Host, Set<Integer>>) SerializationUtils.deserialize(containerToHostMapSer);
                return controlMapping.entrySet().stream()
                        .filter(ep -> ep.getKey().equals(this.host))
                        .map(Map.Entry::getValue)
                        .findFirst().orElse(Collections.emptySet());
            }
        }
        return null;
    }
}
