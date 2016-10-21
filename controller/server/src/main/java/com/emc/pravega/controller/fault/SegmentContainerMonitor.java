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
package com.emc.pravega.controller.fault;

import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.ClusterListener;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.controller.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.emc.pravega.controller.util.Config.LOCK_TIMEOUT_SECS;
import static com.emc.pravega.controller.util.ZKUtils.createPathIfNotExists;

/**
 * Class used to monitor the Data nodes for failures and ensure the segment containers owned by them is assigned
 * to the other Data nodes.
 */
@Slf4j
public class SegmentContainerMonitor implements AutoCloseable {
    private static final String LOCK_PATH = ZKPaths.makePath("cluster", "data", "faulthandler-lock");

    private final ContainerBalancer<Integer, Host> segBalancer;
    private final InterProcessMutex mutex;
    private final LinkedBlockingQueue<Host> hostRemoved = new LinkedBlockingQueue<>();
    private final Cluster pravegaServiceCluster;

    //Executor used to trigger fault handling operations.
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    //Custom executor for acquiring a mutex, since Curator expects the same thread to acquire and release the lock.
    private final ExecutorService mutexExecutor = Executors.newSingleThreadExecutor();

    public SegmentContainerMonitor(CuratorFramework client) {
        pravegaServiceCluster = new ClusterZKImpl(client, Config.CLUSTER_NAME);
        createPathIfNotExists(client, LOCK_PATH);
        mutex = new InterProcessMutex(client, LOCK_PATH);
        segBalancer = new RandomContainerBalancer(client);
    }

    public void startMonitor() {
        try {
            pravegaServiceCluster.addListener(getClusterListener());
        } catch (Exception e) {
            throw new RuntimeException("Not able to start monitor the Pravega service cluster", e);
        }
    }

    private ClusterListener getClusterListener() {
        ClusterListener listener = (type, host) -> {
            if (type.equals(ClusterListener.EventType.HOST_ADDED)) {
                log.info("DataNode:{} added to cluster", host);
            } else {
                hostRemoved.add(host);
                log.info("DataNode:{} removed from cluster", host);
            }
            triggerFaultHandling(pravegaServiceCluster.getClusterMembers(), getHostsRemoved());
        };
        return listener;
    }

    private void triggerFaultHandling(List<Host> clusterMembers, List<Host> hostsRemoved) {
        if (executor.getQueue().size() == 0) {
            //submit only if there are no tasks in queue. Each each fault handling takes care of all events
            executor.submit(() -> performFaultHandling(clusterMembers, hostsRemoved).get());
        }
    }

    /**
     * Following operations are performed
     * 1. Acquire re-entrant lock to do SegmentContainer recovery
     * 2. ContainerBalancer - based on the configured strategy
     * 3. Intimate the Data nodes of the assignment. (or the data nodes could watch the entry in zk)
     * 4. Update the table
     * 5. release lock
     *
     * @return
     */
    private CompletableFuture<Void> performFaultHandling(List<Host> clusterMembers, List<Host> hostsRemoved) {
        return acquireDistributedLock(mutexExecutor)
                .thenApply(success -> {
                    if (success) {
                        return segBalancer.rebalance(clusterMembers, hostsRemoved);
                    } else {
                        throw new CompletionException(new TimeoutException("Timeout while acquiring lock" + LOCK_PATH));
                    }
                })
                .thenAccept(mapOpt -> mapOpt.ifPresent(segBalancer::persistSegmentContainerHostMapping))
                .whenComplete((r, t) -> {
                    releaseDistributedLock(); // finally release the lock, it is executed in the same thread as acquireLock
                    if (t != null) {
                        if (TimeoutException.class.isInstance(t.getCause())) {
                            log.info("Timeout while acquiring a lock for fault handling as a different controller has" +
                                    "performed the fault handling operations");
                        } else {
                            log.error("Error during fault handling", t);
                        }
                    }
                });
    }

    private List<Host> getHostsRemoved() {
        List<Host> removedHosts = new ArrayList<>();
        hostRemoved.drainTo(removedHosts);
        return removedHosts;
    }

    private CompletableFuture<Boolean> acquireDistributedLock(final Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
                    Boolean result = false;
                    try {
                        result = mutex.acquire(LOCK_TIMEOUT_SECS, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        log.error("Exception while acquiring a lock", e);
                        throw new CompletionException(e);
                    }
                    return result;
                }, executor
        );
    }

    private void releaseDistributedLock() {
        try {
            mutex.release();
        } catch (Exception e) {
            log.error("Exception while releasing distributed lock", e);
        }
    }

    @Override
    public void close() throws Exception {
        pravegaServiceCluster.close();
    }
}
