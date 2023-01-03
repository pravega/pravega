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
package io.pravega.controller.store;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import io.pravega.controller.store.stream.OperationContext;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Properties of a Scope and operations that can be performed on it.
 * Identifier for a Scope is its name.
 */
public interface Scope {
    /**
     * Retrieve name of the scope.
     *
     * @return Name of the scope
     */
    String getName();

    /**
     * Create the scope.
     * @param context operation context
     * @return null on success and exception on failure.
     */
    CompletableFuture<Void> createScope(OperationContext context);

    /**
     * Delete the scope.
     *
     * @param context operation context
     * @return null on success and exception on failure.
     */
    CompletableFuture<Void> deleteScope(OperationContext context);

    /**
     * Delete the scope recursively.
     *
     * @param context operation context
     * @return null on success and exception on failure.
     */
    CompletableFuture<Void> deleteScopeRecursive(OperationContext context);
    
    /**
     * A paginated api on the scope to get requested number of streams from under the scope starting from the continuation token. 
     * 
     * @param limit maximum number of streams to return
     * @param continuationToken continuation token from where to start.
     * @param executor executor
     * @param context operation context
     * @return A future, which upon completion, will hold a pair of list of stream names and a new continuation token. 
     */
    CompletableFuture<Pair<List<String>, String>> listStreams(final int limit, final String continuationToken, Executor executor,
                                                              OperationContext context);

    /**
     * A paginated api on the scope to get streams with the specified tag starting from the continuation token.
     * @param tag Stream Tag.
     * @param continuationToken Continuation token from where to start.
     * @param executor executor.
     * @param context operation context.
     * @return A fture, when upon completion, will hold a pair of list of stream names and a new continuation token.
     */
    CompletableFuture<Pair<List<String>, String>> listStreamsForTag(final String tag, final String continuationToken, Executor executor,
                                                              OperationContext context);

    /**
     * List existing streams in scopes.
     * @param context operation context
     * @return List of streams in scope
     */
    CompletableFuture<List<String>> listStreamsInScope(OperationContext context);

    /**
     * Refresh the scope object. Typically to be used to invalidate any caches.
     * This allows us reuse of scope object without having to recreate a new scope object for each new operation
     */
    void refresh();

    /**
     * A paginated api on the scope to get requested number of KeyValueTables from under the scope
     * starting from the continuation token.
     *
     * @param limit maximum number of kvtables to return
     * @param continuationToken continuation token from where to start.
     * @param context operation context
     * @param executor executor
     * @return A future, which upon completion, will hold a pair of list of kvtable names and a new continuation token.
     */
    CompletableFuture<Pair<List<String>, String>> listKeyValueTables(final int limit, final String continuationToken, 
                                                                     final Executor executor, OperationContext context);

    CompletableFuture<UUID> getReaderGroupId(String rgName, OperationContext context);

    CompletableFuture<Boolean> isScopeSealed(String scopeName, OperationContext context);

    CompletableFuture<UUID> getScopeId(String scopeName, OperationContext context);

    default UUID newId() {
        return UUID.randomUUID();
    }

    default byte[] getIdInBytes(UUID id) {
        byte[] b = new byte[2 * Long.BYTES];
        BitConverter.writeUUID(new ByteArraySegment(b), id);
        return b;
    }
}