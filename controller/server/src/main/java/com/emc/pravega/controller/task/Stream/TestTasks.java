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
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * Set of tasks for test purposes.
 */
public class TestTasks extends TaskBase {

    public TestTasks(StreamMetadataStore streamMetadataStore, HostControllerStore hostControllerStore, TaskMetadataStore taskMetadataStore) {
        super(streamMetadataStore, hostControllerStore, taskMetadataStore);
    }

    @Task(name = "test", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<Void> testStreamLock(String scope, String stream) {
        return execute(
                getResource(scope, stream),
                new Serializable[]{scope, stream},
                () -> {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return  CompletableFuture.completedFuture(null);
                },
                null);
    }

    private String getResource(String scope, String stream) {
        return scope + "/" + stream;
    }
}
