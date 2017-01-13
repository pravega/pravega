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
package com.emc.pravega.controller.actor;

import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

// TODO: exception handling in receive method
// TODO: fault tolerance

public abstract class Actor extends AbstractExecutionThreadService {

    private EventStreamReader<byte[]> reader;
    private static long DEFAULT_TIMEOUT = Long.MAX_VALUE;

    final void setReader(EventStreamReader<byte[]> reader) {
        this.reader = reader;
    }

    final EventStreamReader<byte[]> getReader() {
        return this.reader;
    }

    @Override
    public final void startUp() throws Exception {
        preStart();
    }

    @Override
    public final void run() throws Exception {
        while (isRunning()) {
            EventRead<byte[]> event = reader.readNextEvent(DEFAULT_TIMEOUT);
            receive(event.getEvent());
        }
    }

    @Override
    public final void shutDown() throws Exception {
        postStop();
    }

    @Override
    public final void triggerShutdown() {
        this.stopAsync();
    }

    public abstract void preStart() throws Exception;

    public abstract void receive(byte[] event) throws Exception;

    public abstract void postStop() throws Exception;
}
