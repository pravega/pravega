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

import com.emc.pravega.stream.StreamConfiguration;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * In-memory stream store.
 */
@Deprecated
public class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    private final Map<String, InMemoryStream> streams = new HashMap<>();

    @Override
    synchronized Stream newStream(String scope, String name) {
        if (streams.containsKey(name)) {
            return streams.get(name);
        } else {
            throw new StreamNotFoundException(name);
        }
    }

    @Override
    public synchronized CompletableFuture<Boolean> createStream(String scope, String name, StreamConfiguration configuration, long timeStamp, OperationContext context) {
        if (!streams.containsKey(name)) {
            InMemoryStream stream = new InMemoryStream(name, scope);
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
    public CompletableFuture<Void> checkpoint(String id, String group, ByteBuffer serialize) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Optional<ByteBuffer>> readCheckpoint(String id, String group) {
        return null;
    }
}
