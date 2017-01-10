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
package com.emc.pravega.controller.util.action;

import com.emc.pravega.controller.util.BackgroundWorker;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Class that is responsible for processing actions in the queue.
 * It implements background worker and polls the action queue asynchronously.
 * Every time an action is submitted in the queue, it picks the action and calls process.
 */
public abstract class ActionProcessor<A extends Action, Q extends ActionQueue<A>> extends BackgroundWorker<A> {
    /**
     * Static executor shared by all action processors across streams
     */
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(100);
    protected final Q queue;

    public ActionProcessor(final Q queue) {
        super(EXECUTOR);
        this.queue = queue;
    }
}
