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

import com.emc.pravega.controller.actor.ActorGroupRef;
import com.emc.pravega.controller.actor.ActorSystem;
import org.apache.commons.lang.NotImplementedException;

public class ActorGroupRefImpl implements ActorGroupRef {

    private final ActorSystem actorSystem;
    private final String stream;

    // package local constructor
    ActorGroupRefImpl(ActorSystem actorSystem, String stream) {
        this.actorSystem = actorSystem;
        this.stream = stream;
    }

    public void sendEvent(byte[] event) {
        // send an event to the stream
        throw new NotImplementedException();
    }
}
