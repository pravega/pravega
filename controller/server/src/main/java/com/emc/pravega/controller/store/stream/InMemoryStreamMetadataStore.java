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
    private final Map<String, InMemoryScope> scopes = new HashMap<>();

    @Override
    synchronized Stream newStream(String streamName) {
        if (streams.containsKey(streamName)) {
            return streams.get(streamName);
        } else {
            throw new StreamNotFoundException(streamName);
        }
    }

    @Override
    synchronized Scope newScope(String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName);
        } else {
            throw new ScopeNotFoundException(scopeName);
        }
    }

    @Override
    public synchronized CompletableFuture<Boolean> createStream(String scopeName, String streamName,
                                                                StreamConfiguration configuration, long timeStamp) {

        if (scopes.containsKey(scopeName)) {

            if (!streams.containsKey(new StringBuffer(scopeName).append("/").append(streamName).toString())) {
                InMemoryStream stream = new InMemoryStream(scopeName, streamName);
                stream.create(configuration, timeStamp);
                streams.put(new StringBuffer(scopeName).append("/").append(streamName).toString(), stream);
                scopes.get(scopeName).addStreamToScope(stream);
                return CompletableFuture.completedFuture(true);
            } else {
                CompletableFuture<Boolean> result = new CompletableFuture<>();
                result.completeExceptionally(new StreamAlreadyExistsException(streamName));
                return result;
            }

        } else {
            CompletableFuture<Boolean> result = new CompletableFuture<>();
            result.completeExceptionally(new ScopeNotFoundException(scopeName));
            return result;
        }
    }

    @Override
    public synchronized CompletableFuture<Boolean> createScope(String scopeName) {

        if (!scopes.containsKey(scopeName)) {
            InMemoryScope scope = new InMemoryScope(scopeName);
            scope.createScope(scopeName);
            scopes.put(scopeName, scope);
            return CompletableFuture.completedFuture(true);
        } else {
            CompletableFuture<Boolean> result = new CompletableFuture<>();
            result.completeExceptionally(new ScopeAlreadyExistsException(scopeName));
            return result;
        }
    }

    @Override
    public CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx() {
        throw new NotImplementedException();
    }
}
