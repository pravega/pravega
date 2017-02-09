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
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.StreamEvent;
import com.emc.pravega.stream.EventStreamWriter;

/**
 * Actor interface.
 */
public abstract class EventProcessor<T extends StreamEvent> {

    private EventStreamWriter<T> self;

    void setup(EventStreamWriter<T> self) {
        this.self = self;
    }

    /**
     * AbstractActor initialization hook that is called before actor starts receiving events.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected void beforeStart() throws Exception { }

    /**
     * User defined event processing logic.
     * @param event Event received from Pravega Stream.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected abstract void receive(T event) throws Exception;

    /**
     * AbstractActor shutdown hook that is called on shut down.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected void afterStop() throws Exception { }

    /**
     * AbstractActor preRestart hook that is called before actor restarts
     * after recovering from a failure. After this method call, preStart is
     * called before the Actor starts again.
     * @param t Throwable error.
     * @param event Event being processed when error is thrown.
     * @throws Exception Exception thrown from user defined preStart method.
     */
    protected void beforeRestart(Throwable t, T event) throws Exception { }

    /**
     * Get a reference of the ActorGroup it is part of.
     * @return ActorGroupRef.
     */
    protected final EventStreamWriter<T> getSelf() {
        return this.self;
    }

}
