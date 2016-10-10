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

import java.util.List;

/**
 * This class
 */
public class TaskSweeper {

    private final CuratorFramework client;
    private final TaskExecutor taskExecutor;

    public TaskSweeper(StreamMetadataStore streamMetadataStore, HostControllerStore hostControllerStore, CuratorFramework client) {
        this.client = client;
        this.taskExecutor = new TaskExecutor(streamMetadataStore, hostControllerStore, client);
    }

    /**
     * This method is called whenever a node in the controller cluster dies. A ServerSet abstraction may be used as
     * a trigger to invoke this method.
     *
     * It sweeps through all stream tasks under path /stream/*, identifies orphaned tasks and tries to execute them
     * to completion.
     */
    public void sweepOrphanedTasks(String failedHost) {
        try {
            List<String> children = client.getChildren().forPath(String.format(Paths.hostTasks, failedHost));
            for (String streamName: children) {
                // find the task details for this stream's update operation
                byte[] data = client.getData().forPath(String.format(Paths.streamTasks, streamName));
                if (data == null || data.length == 0) {
                    // this task is already cleaned up, nothing to do, ignore
                    continue;
                } else {
                    // execute this task
                    try {
                        TaskData taskData = TaskData.deserialize(data);
                        taskExecutor.execute(streamName, taskData);
                    } catch (Exception ex) {
                        // log exception
                        // continue with next task
                    }
                }
            }
        } catch (Exception e) {
            // log exception
        }
    }
}
