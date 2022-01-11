/**
 * Copyright Pravega Authors.
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
package io.pravega.controller.store.kvtable;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.InMemoryScope;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.index.InMemoryHostIndex;
import io.pravega.controller.store.stream.InMemoryStreamMetadataStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * In-memory stream store.
 */
@Slf4j
public class InMemoryKVTMetadataStore extends AbstractKVTableMetadataStore {

    @GuardedBy("$lock")
    private final Map<String, Integer> deletedKVTables = new HashMap<>();

    private final InMemoryStreamMetadataStore streamStore;

    public InMemoryKVTMetadataStore(StreamMetadataStore streamStore) {
        super(new InMemoryHostIndex());
        this.streamStore = (InMemoryStreamMetadataStore) streamStore;
    }

    @Override
    @Synchronized
    KeyValueTable newKeyValueTable(String scope, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scope newScope(String scopeName) {
        return getScope(scopeName, null);
    }

    @SneakyThrows
    @Override
    @Synchronized
    public KeyValueTable getKVTable(String scope, final String name, OperationContext context) {
        if (this.streamStore.scopeExists(scope)) {
            InMemoryScope kvtScope = (InMemoryScope) this.streamStore.getScope(scope, context);
            if (kvtScope.checkTableExists(name)) {
                return kvtScope.getKeyValueTable(name);
            }
        }
        return new InMemoryKVTable(scope, name);
    }

    @Override
    public CompletableFuture<Void> deleteFromScope(final String scope,
                                                   final String name,
                                                   final OperationContext context,
                                                   final Executor executor) {
        return Futures.completeOn(((InMemoryScope) getScope(scope, context)).removeKVTableFromScope(name),
                executor);
    }

    @Override
    CompletableFuture<Void> recordLastKVTableSegment(String scope, String kvtable, int lastActiveSegment, OperationContext context, Executor executor) {
        Integer oldLastActiveSegment = deletedKVTables.put(getScopedKVTName(scope, kvtable), lastActiveSegment);
        Preconditions.checkArgument(oldLastActiveSegment == null || lastActiveSegment >= oldLastActiveSegment);
        log.debug("Recording last segment {} for kvtable {}/{} on deletion.", lastActiveSegment, scope, kvtable);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkScopeExists(String scope, OperationContext context, Executor executor) {
        return Futures.completeOn(CompletableFuture.completedFuture(this.streamStore.scopeExists(scope)), executor);
    }

    @Override
    public CompletableFuture<Boolean> isScopeSealed(String scope, OperationContext context, Executor executor) {
        return Futures.completeOn(CompletableFuture.completedFuture(this.streamStore.scopeInDeletingTable(scope)), executor);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkTableExists(String scopeName, String kvt, OperationContext context, Executor executor) {
        return Futures.completeOn(checkScopeExists(scopeName, context, executor).thenCompose(exists -> {
            if (exists) {
                return CompletableFuture.completedFuture(((InMemoryScope) getScope(scopeName, context)).checkTableExists(kvt));
            }
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }), executor);
    }

    @Override
    @Synchronized
    public Scope getScope(final String scopeName, OperationContext context) {
        if (this.streamStore.scopeExists(scopeName)) {
            return this.streamStore.getScope(scopeName, context);
        } else {
            return new InMemoryScope(scopeName);
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName, 
                                                                      OperationContext context, Executor executor) {
        final Integer safeStartingSegmentNumber = deletedKVTables.get(scopedKVTName(scopeName, kvtName));
        return CompletableFuture.completedFuture((safeStartingSegmentNumber != null) ? safeStartingSegmentNumber + 1 : 0);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                         final String kvtName,
                                                         final UUID id,
                                                         OperationContext context, 
                                                         final Executor executor) {
        return Futures.completeOn(((InMemoryScope) this.streamStore.getScope(scopeName, context))
                                        .addKVTableToScope(kvtName, id), executor);
    }

    private String scopedKVTName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }
}
