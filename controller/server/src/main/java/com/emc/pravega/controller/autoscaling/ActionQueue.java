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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ActionQueue {
    /**
     * Segment number to action mapping
     * Only one outstanding action per value
     * Outdated actions need to be outdated purged
     */
    private final ConcurrentHashMap<Integer, Action> table;

    private final ConcurrentLinkedQueue<Action> queue;

    public ActionQueue() {
        table = new ConcurrentHashMap<>();
        queue = new ConcurrentLinkedQueue<>();
    }

    /**
     * Method to add action to action queue.
     * Ignore if a pending action already exists for any segment impacted by the action.
     * If a segment is already a candidate for a previous action, let it complete. It will result in
     * some new segments being created (either through splits or merges) which will render
     * subsequent action irrelevant. Even if a scale action is performed after a conflict with an external manual
     * action, it will fail at precondition.
     * So we will ingest all actions that do not conflict with another in our action queue.
     * However, there is no guarantee that the action may succeed.
     *
     * @param action Action to be performed.
     */
    public void addAction(final Action action) {

        final Map<Integer, Action> toAdd = new HashMap<>();
        action.getSegments().stream().forEach(x -> {
            if (!table.containsKey(x)) {
                toAdd.put(x, action);
            }
        });

        if (toAdd.size() == action.getSegments().size()) {
            table.putAll(toAdd);
            queue.add(action);
        }
    }

    public Action getNextAction() {
        final Action action = queue.poll();
        table.keySet().removeAll(action.getSegments());
        return action;
    }
}
