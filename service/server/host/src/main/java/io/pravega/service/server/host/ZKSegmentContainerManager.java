/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.host;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.service.server.SegmentContainerManager;
import io.pravega.service.server.SegmentContainerRegistry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

/**
 * ZK based implementation for SegmentContainerManager.
 */
@Slf4j
class ZKSegmentContainerManager implements SegmentContainerManager {

    private final Host host;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Cluster cluster;
    private final ZKSegmentContainerMonitor containerMonitor;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the ZKSegmentContainerManager class.
     *
     * @param containerRegistry      The SegmentContainerRegistry to manage.
     * @param zkClient               ZooKeeper client.
     * @param pravegaServiceEndpoint Pravega service endpoint details.
     * @param executor               Executor service for running async operations.
     */
    ZKSegmentContainerManager(SegmentContainerRegistry containerRegistry, CuratorFramework zkClient,
                              Host pravegaServiceEndpoint, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(containerRegistry, "containerRegistry");
        Preconditions.checkNotNull(zkClient, "zkClient");
        this.host = Preconditions.checkNotNull(pravegaServiceEndpoint, "pravegaServiceEndpoint");
        this.executor = Preconditions.checkNotNull(executor, "executor");

        this.cluster = new ClusterZKImpl(zkClient, ClusterType.HOST);
        this.containerMonitor = new ZKSegmentContainerMonitor(containerRegistry, zkClient, pravegaServiceEndpoint, this.executor);
    }

    @Override
    public void initialize() {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "initialize");

        try {
            // Initialize the container monitor.
            this.containerMonitor.initialize();

            // Advertise this segment store to the cluster.
            this.cluster.registerHost(this.host);
            log.info("Initialized.");
            LoggerHelpers.traceLeave(log, "initialize", traceId);
        } catch (Exception ex) {
            // Need to make sure we clean up resources if we failed to initialize.
            log.error("Initialization error. Cleaning up.", ex);
            close();
            throw ex;
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            close(this.containerMonitor);
            close(this.cluster);
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
}
