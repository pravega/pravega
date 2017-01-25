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
package com.emc.pravega.controller.actor.impl;

import com.emc.pravega.controller.actor.StreamEvent;
import com.google.common.util.concurrent.Service;
import lombok.extern.log4j.Log4j;

@Log4j
public class ActorFailureListener<T extends StreamEvent> extends Service.Listener {

    private final Actor<T> actor;

    public ActorFailureListener(Actor<T> actor) {
        this.actor = actor;
    }

    public void failed(Service.State from, Throwable failure) {
        log.warn("Actor " + actor + " failed with exception from state " + from, failure);

        // Default policy: if the actor failed while processing messages, i.e., from running state, then restart it.
        if (from == Service.State.RUNNING) {
            actor.restartAsync();
        }
    }
}
