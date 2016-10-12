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
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * TaskBase contains several environment variables for use by batch functions.
 * Class containing batch function may inherit from TaskBase for accessing these environment variables.
 */
public class TaskBase {

    public interface FutureOperation<T> {
        CompletableFuture<T> apply();
    }

    private static final long LOCK_WAIT_TIME = 2;
    protected final StreamMetadataStore streamMetadataStore;
    protected final HostControllerStore hostControllerStore;
    protected ConnectionFactoryImpl connectionFactory;
    private final CuratorFramework client;

    public TaskBase(StreamMetadataStore streamMetadataStore,
                    HostControllerStore hostControllerStore,
                    ConnectionFactoryImpl connectionFactory,
                    CuratorFramework client) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.connectionFactory = connectionFactory;
        this.client = client;
    }

    public CompletableFuture<Boolean> lock(String path) {
        InterProcessMutex mutex = new InterProcessMutex(client, path);
        try {
            boolean success = mutex.acquire(LOCK_WAIT_TIME, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(success);
        } catch (Exception ex) {
            // log exception
            return CompletableFuture.completedFuture(false);
        }
    }

    public void unlock(String path) {
        InterProcessMutex mutex = new InterProcessMutex(client, path);
        try {
            mutex.release();
        } catch (Exception e) {
            // log exception
        }
    }

    /**
     * Wrapper method that initially obtains lock, then executes the passed method, and finally releases the lock
     * @param operation operation to execute
     * @return returns the value returned by operation, if lock is obtained successfully
     */
    public <T> CompletableFuture<T> wrapper(String scope, String stream, List<Serializable> parameters, FutureOperation<T> operation) {
        String streamName = scope + "_" + stream;
        String lockPath = String.format(Paths.STREAM_LOCKS, scope, stream);
        try {
            CompletableFuture<Boolean> lock = this.lock(lockPath);
            return lock.thenCompose(
                    success -> {
                        if (success) {
                            TaskData taskData = new TaskData();
                            StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
                            StackTraceElement e = stacktrace[1];
                            taskData.setMethodName(e.getMethodName());
                            taskData.setParameters(parameters);


                            // 1. persist taks data
                            String path = String.format(Paths.STREAM_TASKS, scope, stream);
                            try {
                                client.setData().forPath(path, taskData.serialize());
                            } catch (Exception ex) {
                                throw new WriteFailedException(streamName, ex);
                            }
                            CompletableFuture<T> o = operation.apply();
                            // 1. remove task data
                            try {
                                client.delete().forPath(path);
                            } catch (Exception ex) {
                                throw new WriteFailedException(streamName, ex);
                            }
                            return o;
                        } else {
                            throw new LockFailedException(streamName);
                        }
                    }
            );
        } finally {
            unlock(lockPath);
        }
    }
}
