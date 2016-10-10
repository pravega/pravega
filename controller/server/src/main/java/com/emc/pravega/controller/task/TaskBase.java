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
package com.emc.pravega.controller.task;

import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * TaskBase contains several environment variables for use by batch functions.
 * Class containing batch function may inherit from TaskBase for accessing these environment variables.
 */
public class TaskBase {

    private StreamMetadataStore streamMetadataStore;
    private HostControllerStore hostControllerStore;
    private String streamName;
    private CuratorFramework client;
    private static final long lockWaitTime = 10;

    public void initialize(StreamMetadataStore streamMetadataStore,
                           HostControllerStore hostControllerStore,
                           String streamName,
                           CuratorFramework client) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.streamName = streamName;
        this.client = client;
    }

    public CompletableFuture<Boolean> lock() {
        String path = String.format(Paths.streamLocks, streamName);
        InterProcessMutex mutex = new InterProcessMutex(client, path);
        try {
            boolean success = mutex.acquire(lockWaitTime, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(success);
        } catch (Exception ex) {
            // log exception
            return CompletableFuture.completedFuture(false);
        }
    }

    public void unlock() {
        String path = String.format(Paths.streamLocks, streamName);
        InterProcessMutex mutex = new InterProcessMutex(client, path);
        try {
            mutex.release();
        } catch (Exception e) {
            // log exception
        }
    }
}
