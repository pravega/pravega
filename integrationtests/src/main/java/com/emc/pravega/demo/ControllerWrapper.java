/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.demo;

import com.emc.pravega.controller.embedded.EmbeddedControllerImpl;
import com.emc.pravega.controller.requesthandler.RequestHandlersInit;
import com.emc.pravega.controller.requesthandler.TxTimeoutStreamScheduler;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ControllerWrapper extends EmbeddedControllerImpl {

    private ControllerWrapper(ControllerService controller) {
        super(controller);
    }

    public static ControllerWrapper getControllerWrapper(String connectionString) {
        String hostId;
        try {
            // On each controller process restart, it gets a fresh hostId,
            // which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            hostId = UUID.randomUUID().toString();
        }

        // initialize the executor service
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, new RetryOneTime(2000));
        client.start();

        StoreClient storeClient = new ZKStoreClient(client);

        StreamMetadataStore streamStore = new ZKStreamMetadataStore(client, executor);

        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        //2) start RPC server with v1 implementation. Enable other versions if required.
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);
        TxTimeoutStreamScheduler txTimeOutScheduler = new TxTimeoutStreamScheduler();
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId, txTimeOutScheduler);

        ControllerService controller = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);

        ControllerWrapper controllerWrapper = new ControllerWrapper(controller);
        RequestHandlersInit.coldStart(controllerWrapper, Executors.newScheduledThreadPool(100));

        txTimeOutScheduler.setController(controllerWrapper);

        return controllerWrapper;
    }
}

