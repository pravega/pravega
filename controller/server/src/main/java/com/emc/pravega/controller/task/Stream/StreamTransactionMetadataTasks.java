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
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.CompletableFuture;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 *
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
public class StreamTransactionMetadataTasks extends TaskBase {

    public StreamTransactionMetadataTasks(StreamMetadataStore streamMetadataStore, HostControllerStore hostControllerStore, TaskMetadataStore taskMetadataStore) {
        super(streamMetadataStore, hostControllerStore, taskMetadataStore);
    }

    /**
     * Create transaction.
     * @param scope stream scope.
     * @param stream stream name.
     * @return transaction id.
     */
    @Task(name = "createTransaction", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<String> createTx(String scope, String stream) {
        throw new NotImplementedException();
    }

    /**
     * Drop transaction.
     * @param scope stream scope.
     * @param stream stream name.
     * @param txId transaction id.
     * @return true/false.
     */
    @Task(name = "dropTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<Boolean> dropTx(String scope, String stream, String txId) {
        throw new NotImplementedException();
    }

    /**
     * Commit transaction.
     * @param scope stream scope.
     * @param stream stream name.
     * @param txId transaction id.
     * @return true/false.
     */
    @Task(name = "commitTransaction", version = "1.0", resource = "{scope}/{stream}/{txId}")
    public CompletableFuture<Boolean> commitTx(String scope, String stream, String txId) {
        throw new NotImplementedException();
    }
}
