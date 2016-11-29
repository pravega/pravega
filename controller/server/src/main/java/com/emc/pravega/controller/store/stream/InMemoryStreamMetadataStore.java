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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.NotImplementedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * In-memory stream store.
 */
public class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    private final Map<String, InMemoryStream> streams = new HashMap<>();

    @Override
    synchronized Stream newStream(String name) {
        if (streams.containsKey(name)) {
            return streams.get(name);
        } else {
            throw new StreamNotFoundException(name);
        }
    }

    @Override
    public synchronized CompletableFuture<Boolean> createStream(String name, StreamConfiguration configuration, long timeStamp) {
        if (!streams.containsKey(name)) {
            InMemoryStream stream = new InMemoryStream(name);
            stream.create(configuration, timeStamp);
            streams.put(name, stream);
            return CompletableFuture.completedFuture(true);
        } else {
            CompletableFuture<Boolean> result = new CompletableFuture<>();
            result.completeExceptionally(new StreamAlreadyExistsException(name));
            return result;
        }
    }

    @Override
    public CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx() {
        throw new NotImplementedException();
    }
}
