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
import org.apache.zookeeper.CreateMode;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * TaskBase contains the following.
 * 1. Environment variables used by tasks.
 * 2. Wrapper method that has boilerplate code for locking, persisting task data and executing the task
 *
 * Actual tasks are implemented in sub-classes of TaskBase and annotated with @Task annotation.
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
                    CuratorFramework client) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.client = client;
    }

    /**
     * Wrapper method that initially obtains lock, persists the task data then executes the passed method,
     * finally deletes task data and releases lock
     *
     * @param lockPath ZK path for lock
     * @param taskDataPath ZK path for persisting task data
     * @param parameters method parameters
     * @param operation lambda operation that is the actual task
     * @param <T> type parameter
     * @return return value of task execution
     */
    public <T> CompletableFuture<T> execute(String lockPath, String taskDataPath, Serializable[] parameters, FutureOperation<T> operation) {
        TaskData taskData = getTaskData(parameters);
        try {
            CompletableFuture<Boolean> lock = this.lock(lockPath);
            return lock.thenCompose(success -> {
                        if (success) {
                            // todo: handle lock glitches possibly due to connectivity issues
                            // todo: duplicate task data already exists
                            // solution: perform the old task first and then execute the current one
                            createTaskData(taskDataPath, taskData);
                            CompletableFuture<T> o = operation.apply();
                            deleteTaskData(taskDataPath);
                            return o;
                        } else {
                            throw new LockFailedException(lockPath);
                        }
                    }
            );
        } finally {
            unlock(lockPath);
        }
    }

    private CompletableFuture<Boolean> lock(String path) {
        InterProcessMutex mutex = new InterProcessMutex(client, path);
        try {
            boolean success = mutex.acquire(LOCK_WAIT_TIME, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(success);
        } catch (Exception ex) {
            // log exception
            return CompletableFuture.completedFuture(false);
        }
    }

    private void unlock(String path) {
        InterProcessMutex mutex = new InterProcessMutex(client, path);
        try {
            mutex.release();
        } catch (Exception e) {
            // log exception
        }
    }

    private TaskData getTaskData(Serializable[] parameters) {
        TaskData taskData = new TaskData();
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement e = stacktrace[3];
        Task annotation = getTaskAnnotation(e.getMethodName());
        taskData.setMethodName(annotation.name());
        taskData.setMethodVersion(annotation.version());
        taskData.setParameters(parameters);
        return taskData;
    }

    private Task getTaskAnnotation(String method) {
        for (Method m : this.getClass().getMethods()) {
            if (m.getName().equals(method)) {
                for (Annotation annotation : m.getDeclaredAnnotations()) {
                    if (annotation instanceof Task) {
                        return (Task) annotation;
                    }
                }
                break;
            }
        }
        throw new TaskAnnotationMissingException(method);
    }

    private void createTaskData(String taskDataPath, TaskData taskData) {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(taskDataPath, taskData.serialize());
        } catch (Exception ex) {
            throw new TaskDataWriteFailedException(taskDataPath, ex);
        }
    }

    private void deleteTaskData(String taskDataPath) {
        try {
            client.delete().forPath(taskDataPath);
        } catch (Exception ex) {
            throw new TaskDataWriteFailedException(taskDataPath, ex);
        }
    }
}
