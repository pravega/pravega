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
package com.emc.pravega.controller.server;

import static com.emc.pravega.controller.util.Config.ASYNC_TASK_POOL_SIZE;
import static com.emc.pravega.controller.util.Config.HOST_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STREAM_STORE_CONNECTION_STRING;
import static com.emc.pravega.controller.util.Config.STREAM_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STORE_CONNECTION_STRING;
import static com.emc.pravega.controller.util.Config.STORE_TYPE;

import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.StoreClientFactory;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.InMemoryHostControllerStoreConfig;
import com.emc.pravega.controller.store.stream.StoreConfiguration;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.task.TaskSweeper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import com.google.common.collect.Sets;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {

        // TODO: Will use hard-coded host to container mapping for this sprint
        // Read from a config file. This same information will be present on pravega hosts
        // TODO: remove temporary hard coding for the cluster and segment
        Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();
        hostContainerMap.put(new Host("localhost", 12345), Sets.newHashSet(0));

        String hostId;
        try {
            // On each controller process restart, it gets a fresh hostId,
            // which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            log.debug("Failed to get host address.", e);
            hostId = UUID.randomUUID().toString();
        }

        // initialize the executor service
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        //1) LOAD configuration.

        log.info("Creating store client");
        StoreClient storeClient = StoreClientFactory.createStoreClient(
                StoreClientFactory.StoreType.valueOf(STORE_TYPE),
                new StoreConfiguration(STORE_CONNECTION_STRING));

        log.info("Creating in-memory stream store");
        StreamMetadataStore streamStore = StreamStoreFactory.createStore(
                StreamStoreFactory.StoreType.valueOf(STREAM_STORE_TYPE),
                new StoreConfiguration(STREAM_STORE_CONNECTION_STRING),
                executor);
        log.info("Creating in-memory host store");
        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.valueOf(HOST_STORE_TYPE),
                new InMemoryHostControllerStoreConfig(hostContainerMap));

        log.info("Creating zk based task store");
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        //2) start RPC server with v1 implementation. Enable other versions if required.
        log.info("Starting RPC server");
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, hostId);
        RPCServer.start(new ControllerServiceAsyncImpl(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks));

        //3. hook up TaskSweeper.sweepOrphanedTasks as a callback on detecting some controller node failure
        // todo: hook up TaskSweeper.sweepOrphanedTasks with Failover support feature
        // Controller has a mechanism to track the currently active controller host instances. On detecting a failure of
        // any controller instance, the failure detector stores the failed HostId in a failed hosts directory (FH), and
        // invokes the taskSweeper.sweepOrphanedTasks for each failed host. When all resources under the failed hostId
        // are processed and deleted, that failed HostId is removed from FH folder.
        // Moreover, on controller process startup, it detects any hostIds not in the currently active set of controllers
        // and starts sweeping tasks orphaned by those hostIds.
        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, hostId, streamMetadataTasks, streamTransactionMetadataTasks);
    }
}
