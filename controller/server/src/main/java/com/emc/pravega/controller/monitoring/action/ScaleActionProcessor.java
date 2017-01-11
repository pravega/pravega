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
package com.emc.pravega.controller.monitoring.action;

import com.emc.pravega.controller.monitoring.datasets.StreamStoreChangeWorker;
import com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.util.action.ActionProcessor;

public class ScaleActionProcessor extends ActionProcessor<Scale, ScaleActionQueue> {
    private final StreamMetadataTasks streamMetadataTasks;
    private final String stream;
    private final String scope;
    private volatile boolean processing;

    public ScaleActionProcessor(ScaleActionQueue scaleActionQueue, StreamMetadataTasks streamMetadataTasks, String stream, String scope) {
        super(scaleActionQueue);
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
    public Scale getNextWork() {
        if (!processing) {
            return queue.getNextAction();
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
    public void process(final Scale action) {
        // Whenever a new action is generated we have to start a scaled task.
        // If an action is being performed, other actions lay in wait in the action queue.
        // If an action fails, its ok to ignore. If segments have not changed and the impacted segment
        // continue to show load characteristics that could trigger a scaled, the stream monitor will generate one.

        // every time process finishes, a new poll and getNextWork happens. this means process should finish the work and
        // only then do want to getNextWork. So process should block. The disadvantage is that the executor pool
        // where process is running is shared by all action processors. So if an action
        processing = true;

        Scale scaleAction = (Scale) action;
        streamMetadataTasks.scale(scope,
                stream,
                scaleAction.getSegments(),
                scaleAction.getNewRanges(),
                System.currentTimeMillis())
                .thenAccept(x -> {
                    processing = false;
                    queue.done(action);

                    final ScaleStreamStatus status = x.getStatus();
                    if (status.equals(ScaleStreamStatus.SUCCESS)) {
                        // notify stream monitor about new active segments
                        StreamStoreChangeWorker.requestStreamUpdate(scope, stream);
                    }
                });
    }

}
