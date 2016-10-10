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
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;

/**
 * Initializes and executes a batch job
 */
@Slf4j
public class TaskExecutor {

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final CuratorFramework client;

    public TaskExecutor(StreamMetadataStore streamMetadataStore, HostControllerStore hostControllerStore, CuratorFramework client) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.client = client;
    }

    public Object execute(String stream, TaskData taskData) throws MalformedURLException, IllegalAccessException,
            InstantiationException, InvocationTargetException, ClassNotFoundException {
        return this.searchAndExecuteMethod(stream, taskData);
    }

    private Object searchAndExecuteMethod(String stream, TaskData taskData) throws MalformedURLException,
            IllegalAccessException, InstantiationException, InvocationTargetException, ClassNotFoundException {

        log.debug("Trying to execute {}", taskData.getClassName() + ":" + taskData.getMethodName());
        Class claz = Class.forName(taskData.getClassName());
        if (claz != null) {
            for (Method method : claz.getDeclaredMethods()) {
                if (method.getName().equals(taskData.getMethodName())) {
                    Object o = claz.newInstance();
                    if (o instanceof TaskBase) {
                        StreamMetadataTasks t = new StreamMetadataTasks();
                        log.debug("Initialising TaskBase");
                        ((TaskBase) o).initialize(streamMetadataStore, hostControllerStore, stream, client);
                    }
                    log.debug("Invoking method={}", method.getName());
                    Object returnValue = method.invoke(o, taskData.getParameters());
                    return returnValue;
                }
            }
        }
        throw new RuntimeException(String.format("Task %s not found", taskData.getMethodName()));
    }
}
