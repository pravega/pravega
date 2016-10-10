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

import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.CompletableFuture;

/**
 * Collection of transaction related batch operations on stream.
 */
public class StreamTransactionTasks extends TaskBase {
    @Task(name = "createTransaction")
    public CompletableFuture<String> createTx(String scope, String stream) {
        throw new NotImplementedException();
    }

    @Task(name = "dropTransaction")
    public CompletableFuture<Boolean> dropTx(String scope, String stream, String txId) {
        throw new NotImplementedException();
    }

    @Task(name = "commitTransaction")
    public CompletableFuture<Boolean> commitTx(String scope, String stream, String txId) {
        throw new NotImplementedException();
    }
}
