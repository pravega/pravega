/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.stream;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.client.stream.StreamConfiguration;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * In-memory stream store.
 */
@Slf4j
class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    @GuardedBy("$lock")
    private final Map<String, InMemoryStream> streams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, InMemoryScope> scopes = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, ByteBuffer> checkpoints = new HashMap<>();

    private final Executor executor;

    InMemoryStreamMetadataStore(Executor executor) {
        this.executor = executor;
    }

    @Override
    @Synchronized
    Stream newStream(String scope, String name) {
        Stream stream = streams.get(scopedStreamName(scope, name));
        if (stream != null) {
            return stream;
        } else {
            return new InMemoryStream.NonExistentStream(scope, name);
        }
    }

    @Override
    @Synchronized
    Scope newScope(final String scopeName) {
        return scopes.get(scopeName);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> createStream(final String scopeName, final String streamName,
                                                   final StreamConfiguration configuration,
                                                   final long timeStamp,
                                                   final OperationContext context,
                                                   final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            if (!streams.containsKey(scopedStreamName(scopeName, streamName))) {
                InMemoryStream stream = new InMemoryStream(scopeName, streamName);
                stream.create(configuration, timeStamp);
                streams.put(scopedStreamName(scopeName, streamName), stream);
                scopes.get(scopeName).addStreamToScope(streamName);
                return CompletableFuture.completedFuture(true);
            } else {
                return FutureHelpers.
                        failedFuture(new StoreException(StoreException.Type.NODE_EXISTS, "Stream already exists."));
            }
        } else {
            return FutureHelpers.
                    failedFuture(new StoreException(StoreException.Type.NODE_NOT_FOUND, "Scope not found."));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> deleteStream(final String scopeName, final String streamName,
                                                final OperationContext context,
                                                final Executor executor) {
        String scopedStreamName = scopedStreamName(scopeName, streamName);
        if (scopes.containsKey(scopeName) && streams.containsKey(scopedStreamName)) {
            streams.remove(scopedStreamName);
            scopes.get(scopeName).removeStreamFromScope(streamName);
            return CompletableFuture.completedFuture(null);
        } else {
            return FutureHelpers.
                    failedFuture(new StoreException(StoreException.Type.NODE_NOT_FOUND, "Stream not found."));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> updateConfiguration(final String scopeName,
                                                          final String streamName,
                                                          final StreamConfiguration configuration,
                                                          final OperationContext context,
                                                          final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            return streams.get(scopedStreamName(scopeName, streamName)).updateConfiguration(configuration);
        } else {
            return FutureHelpers.
                    failedFuture(new StoreException(StoreException.Type.NODE_NOT_FOUND, "Scope not found."));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        if (!scopes.containsKey(scopeName)) {
            InMemoryScope scope = new InMemoryScope(scopeName);
            scope.createScope();
            scopes.put(scopeName, scope);
            return CompletableFuture.completedFuture(CreateScopeStatus.newBuilder().setStatus(
                    CreateScopeStatus.Status.SUCCESS).build());
        } else {
            return CompletableFuture.completedFuture(CreateScopeStatus.newBuilder().setStatus(
                    CreateScopeStatus.Status.SCOPE_EXISTS).build());
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName).listStreamsInScope().thenApply((streams) -> {
                if (streams.isEmpty()) {
                    scopes.get(scopeName).deleteScope();
                    scopes.remove(scopeName);
                    return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SUCCESS).build();
                } else {
                    return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build();
                }
            });
        } else {
            return CompletableFuture.completedFuture(DeleteScopeStatus.newBuilder().setStatus(
                    DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build());
        }
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return CompletableFuture.completedFuture(scopeName);
        } else {
            return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.NODE_NOT_FOUND));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<List<String>> listScopes() {
        return CompletableFuture.completedFuture(new ArrayList<>(scopes.keySet()));
    }

    /**
     * List the streams in scope.
     *
     * @param scopeName Name of scope
     * @return List of streams in scope
     */
    @Override
    @Synchronized
    public CompletableFuture<List<StreamConfiguration>> listStreamsInScope(final String scopeName) {
        InMemoryScope inMemoryScope = scopes.get(scopeName);
        if (inMemoryScope != null) {
            return inMemoryScope.listStreamsInScope()
                    .thenApply(streams -> streams.stream().map(
                            stream -> this.getConfiguration(scopeName, stream, null, executor).join()).collect(Collectors.toList()));
        } else {
            return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.NODE_NOT_FOUND));
        }
    }

    private String scopedStreamName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }
}
