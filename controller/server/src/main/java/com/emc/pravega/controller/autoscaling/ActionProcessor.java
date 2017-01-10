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

import com.emc.pravega.controller.autoscaling.util.BackgroundWorker;
import com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Class that is responsible for processing actions per stream.
 * It implements background worker and polls the action queue asynchronously.
 * Every time an action is submitted in the queue, it picks the action and starts scaled task and blocks
 * until the task finishes.
 */
public class ActionProcessor extends BackgroundWorker<Action> {
    /**
     * Static executor shared by all action processors across streams
     */
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(100);
    private final ActionQueue actionQueue;
    private final StreamMetadataTasks streamMetadataTasks;
    private final String stream;
    private final String scope;
    private volatile boolean processing;

    public ActionProcessor(final ActionQueue actionQueue, final StreamMetadataTasks streamMetadataTasks, final String stream, final String scope) {
        super(EXECUTOR);
        this.actionQueue = actionQueue;
        this.streamMetadataTasks = streamMetadataTasks;
        this.stream = stream;
        this.scope = scope;
        this.processing = false;
    }

    /**
     * If no other processing is going on for this action processor then getNextWork.
     * Else return null
     *
     * @return
     */
    @Override
    public Action getNextWork() {
        if (!processing) {
            return actionQueue.getNextAction();
        } else {
            return null;
        }
    }

    // Note: we will not consolidate multiple actions. We will run each action independently.
    // This is because upon failure it would be hard to identify action responsible for failure and discard and retry
    // with remainder of actions.
    // Also, we have ensured that multiple actions are not submitted for the same segment, whereby removing chances of
    // conflicts arising from auto-scaling produced actions. However, conflicts could still come because of manual scaling.
    // This will result in action being failed and ignored. If some segments from this failed action survive the scaled, they
    // will continue to work on their history and produce newer actions to handle their load characteristics.
    @Override
    public void process(final Action action) {
        // Whenever a new action is generated we have to start a scaled task.
        // If an action is being performed, other actions lay in wait in the action queue.
        // If an action fails, its ok to ignore. If segments have not changed and the impacted segment
        // continue to show load characteristics that could trigger a scaled, the stream monitor will generate one.

        // every time process finishes, a new poll and getNextWork happens. this means process should finish the work and
        // only then do want to getNextWork. So process should block. The disadvantage is that the executor pool
        // where process is running is shared by all action processors. So if an action
        processing = true;

        streamMetadataTasks.scale(scope,
                stream,
                action.getSegments(),
                action.getNewRanges(),
                System.currentTimeMillis())
                .thenAccept(x -> {
                    processing = false;
                    actionQueue.done(action);

                    final ScaleStreamStatus status = x.getStatus();
                    if (status.equals(ScaleStreamStatus.SUCCESS)) {
                        // notify stream monitor about new active segments
                        StreamStoreChangeWorker.requestStreamUpdate(scope, stream);
                    }
                });
    }
}
