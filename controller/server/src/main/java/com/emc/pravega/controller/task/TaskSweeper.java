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

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.List;

/**
 * This class
 */
@Slf4j
public class TaskSweeper {

    private final CuratorFramework client;
    private final TaskBase[] taskClassObjects;

    public TaskSweeper(CuratorFramework client, TaskBase... classes) {
        this.client = client;

        // following arrays can alternatively be populated by dynamically finding all sub-classes of TaskBase using
        // reflection library org.reflections. However, this library is flagged by checkstyle as disallowed library.
        taskClassObjects = classes;
    }

    /**
     * This method is called whenever a node in the controller cluster dies. A ServerSet abstraction may be used as
     * a trigger to invoke this method.
     *
     * It sweeps through all stream tasks under path /tasks/stream/, identifies orphaned tasks and attempts to execute
     * them to completion.
     */
    public void sweepOrphanedTasks() {
        try {
            List<String> children = client.getChildren().forPath(Paths.STREAM_TASK_ROOT);
            for (String streamName: children) {
                // find the task details for this stream's update operation
                byte[] data = client.getData().forPath(Paths.STREAM_TASK_ROOT + streamName);
                if (data != null && data.length > 0) {
                    // if no one is holding a lock, try to lock the task and execute it
                    List<String> locks = client.getChildren().forPath(Paths.STREAM_LOCKS_ROOT + streamName);
                    if (locks != null && locks.size() == 0) {
                        // execute this task
                        try {
                            TaskData taskData = TaskData.deserialize(data);
                            execute(taskData);
                        } catch (Exception e) {
                            // log exception
                            log.error("Error processing task", e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            // log exception
            log.error("Error processing task", e);
        }
    }

    /**
     * This method identifies correct method to execute form among the task classes and executes it
     * @param taskData taks data
     * @return the object returned from task method
     * @throws MalformedURLException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     * @throws ClassNotFoundException
     */
    public Object execute(TaskData taskData) throws MalformedURLException, IllegalAccessException,
            InstantiationException, InvocationTargetException, ClassNotFoundException {

        log.debug("Trying to execute {}", taskData.getMethodName());
        for (int i = 0; i < taskClassObjects.length; i++) {
            Class claz = taskClassObjects[i].getClass();
            for (Method method : claz.getDeclaredMethods()) {
                if (matches(method, taskData)) {
                    Object o = taskClassObjects[i];
                    log.debug("Invoking method={}", method.getName());
                    return method.invoke(o, taskData.getParameters());
                }
            }
        }
        throw new RuntimeException(String.format("Task %s not found", taskData.getMethodName()));
    }

    private boolean matches(Method method, TaskData taskData) {
        for (Annotation annotation : method.getAnnotations()) {
            if ((annotation instanceof Task)
                    && ((Task) annotation).name().equals(taskData.getMethodName())
                    && ((Task) annotation).version().equals(taskData.getMethodVersion())) {
                return true;
            }
        }
        return false;
    }
}
