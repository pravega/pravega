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
package com.emc.pravega.controller.autoscaling;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.autoscaling.util.BackgroundWorker;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Class that is responsible for processing actions per stream.
 * It implements background worker and polls the action queue asynchronously.
 * Every time an action is submitted in the queue, it picks the action and starts scale task and blocks
 * until the task finishes.
 */
public class ActionProcessor extends BackgroundWorker<Action> {

    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(100);
    private final ActionQueue actionQueue;
    private final StreamMetadataTasks streamMetadataTasks;
    private final String stream;
    private final String scope;

    public ActionProcessor(final ActionQueue actionQueue, final StreamMetadataTasks streamMetadataTasks, final String stream, final String scope) {
        super(EXECUTOR);
        this.actionQueue = actionQueue;
        this.streamMetadataTasks = streamMetadataTasks;
        this.stream = stream;
        this.scope = scope;
    }

    @Override
    public Action dequeue() {
        return actionQueue.getNextAction();
    }

    // Note: we will not consolidate multiple actions. We will run each action independently.
    // This is because upon failure it would be hard to identify action responsible for failure and discard and retry
    // with remainder of actions.
    // Also, we have ensured that multiple actions are not submitted for the same segment, whereby removing chances of
    // conflicts arising from auto-scaling produced actions. However, conflicts could still come because of manual scaling.
    // This will result in action being failed and ignored. If some segments from this failed action survive the scale, they
    // will continue to work on their history and produce newer actions to handle their load characteristics.
    @Override
    public void process(final Action action) {
        // Whenever a new action is generated we have to start a scale task.
        // If an action is being performed, other actions lay in wait.
        // An action can fail because of two reasons --
        // 1. another scale task ongoing.
        //      wait.
        // 2. failed because action is no longer valid.
        //      fail and ignore.

        // TODO: check its returned state and determine if this has to be retried or ignored
        ScaleResponse response = FutureHelpers.getAndHandleExceptions(streamMetadataTasks.scale(scope,
                        stream,
                        action.getSegments(),
                        action.getNewRanges(),
                        System.currentTimeMillis()),
                RuntimeException::new);
    }
}
