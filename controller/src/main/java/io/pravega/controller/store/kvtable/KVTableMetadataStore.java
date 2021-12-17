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

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.controller.store.stream.OperationContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * KeyValueTable Metadata Store.
 */
public interface KVTableMetadataStore extends AutoCloseable {

    /**
     * Method to create an operation context. A context ensures that multiple calls to store for the same data are avoided
     * within the same operation. All api signatures are changed to accept context. If context is supplied, the data will be
     * looked up within the context and, upon a cache miss, will be fetched from the external store and cached within the context.
     * Once an operation completes, the context is discarded.
     *
     * @param scope Stream scope.
     * @param name  Stream name.
     * @param requestId request id.
     * @return Return a streamContext
     */
    KVTOperationContext createContext(final String scope, final String name, final long requestId);

    /**
     * Checks if scope exists or not. 
     * @param scope Scope.
     * @param context operation context.
     * @param executor executor
     * @return Completable Future which when completed will hold a boolan which will indicate if scope exists or not. 
     */
    CompletableFuture<Boolean> checkScopeExists(String scope, OperationContext context, Executor executor);

    /**
     * Api to check if a scope exists in the deleting scope table or not.
     * @param scopeName scope name
     * @param context operation context
     * @param executor executor
     * @return true if scope exists, false otherwise
     */
    CompletableFuture<Boolean> isScopeSealed(final String scopeName, OperationContext context, Executor executor);

    /**
     * Checks if kv table exists or not. 
     * @param scope Scope.
     * @param kvt key value table name
     * @param context operation context.
     * @param executor executor
     * @return Completable Future which when completed will hold a boolan which will indicate if kvt exists or not. 
     */
    CompletableFuture<Boolean> checkTableExists(String scope, String kvt, OperationContext context, Executor executor);

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param kvtName         KeyValueTable name
     * @param id              Unique Identifier for KVTable
     * @param context         operation context
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                  final String kvtName,
                                                  final UUID id,
                                                  final OperationContext context,
                                                  final Executor executor);

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param kvtName         stream name
     * @param configuration   stream configuration
     * @param createTimestamp stream creation timestamp
     * @param context         operation context
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<CreateKVTableResponse> createKeyValueTable(final String scopeName,
                                            final String kvtName,
                                            final KeyValueTableConfiguration configuration,
                                            final long createTimestamp,
                                            final OperationContext context,
                                            final Executor executor);

    /**
     * Api to get creation time for the stream. 
     * 
     * @param scopeName       scope name
     * @param kvtName         name of KeyValueTable
     * @param context         operation context
     * @param executor        callers executor
     * @return CompletableFuture, which when completed, will contain the creation time of the stream. 
     */
    CompletableFuture<Long> getCreationTime(final String scopeName,
                                            final String kvtName,
                                            final OperationContext context,
                                            final Executor executor);

    /**
     * Api to set the state for stream in metadata.
     * @param scope scope name
     * @param name kvtable name
     * @param state kvtable state
     * @param context operation context
     * @param executor callers executor
     * @return Future of boolean if state update succeeded.
     */

    CompletableFuture<Void> setState(String scope, String name,
                                     KVTableState state, OperationContext context,
                                     Executor executor);


    /**
     * Api to get the state for kvtable from metadata.
     *
     * @param scope scope name
     * @param name kvtable name
     * @param ignoreCached ignore cached value and fetch from store.
     * @param context operation context
     * @param executor callers executor
     * @return Future of boolean if state update succeeded.
     */
    CompletableFuture<KVTableState> getState(final String scope, final String name, final boolean ignoreCached, 
                                             final OperationContext context, final Executor executor);

    /**
     * Api to get the current state with its current version.
     *
     * @param scope scope
     * @param name kvtable
     * @param context operation context
     * @param executor executor
     * @return Future which when completed has the versioned state.
     */

    CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState(final String scope, final String name,
                                                                         final OperationContext context, final Executor executor);

    /**
     * Api to update versioned state as a CAS operation.
     *
     * @param scope scope name
     * @param name kvTable name
     * @param state desired state
     * @param previous current state with version
     * @param context operation context
     * @param executor executor
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */

    CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final String scope, final String name,
                                                    final KVTableState state, final VersionedMetadata<KVTableState> previous,
                                                    final OperationContext context,
                                                    final Executor executor);

    KeyValueTable getKVTable(String scope, final String name, OperationContext context);

    /**
     * Get active segments.
     *
     * @param scope    kvtable scope
     * @param name     kvtable name.
     * @param executor callers executor
     * @param context  operation context
     * @return currently active segments
     */
    CompletableFuture<List<KVTSegmentRecord>> getActiveSegments(final String scope, final String name, final OperationContext context,
                                                                final Executor executor);

    /**
     * Fetches the current stream configuration.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return current stream configuration.
     */
    CompletableFuture<KeyValueTableConfiguration> getConfiguration(final String scope, final String name,
                                                                   final OperationContext context,
                                                                   final Executor executor);

    /**
     * List existing KeyValueTables in scopes with pagination.
     * This api continues listing KeyValueTables from the supplied continuation token
     * and returns a count of limited list of KeyValueTables and a new continuation token.
     *
     * @param scopeName Name of the scope
     * @param continuationToken continuation token
     * @param limit limit on number of streams to return
     * @param context operation context
     * @param executor executor
     * @return A pair of list of KeyValueTables in scope with the continuation token.
     */
    CompletableFuture<Pair<List<String>, String>> listKeyValueTables(final String scopeName, final String continuationToken,
                                                                     final int limit, final OperationContext context, 
                                                                     final Executor executor);

    /**
     * Returns a Scope object from scope identifier.
     *
     * @param scopeName scope identifier is scopeName.
     * @return Scope object.
     */
    Scope newScope(final String scopeName);

    /**
     * Api to get all segments in the stream.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     *
     * @return Future, which when complete will contain a list of all segments in the stream.
     */
    CompletableFuture<Set<Long>> getAllSegmentIds(final String scope, final String name,
                                                  final OperationContext context, final Executor executor);


    /**
     * Api to Delete the kvtable related metadata.
     *
     * @param scopeName       scope name
     * @param kvtName         KeyValueTable name
     * @param context         operation context
     * @param executor        callers executor
     * @return future
     */
    CompletableFuture<Void> deleteKeyValueTable(final String scopeName,
                                                final String kvtName,
                                                final OperationContext context,
                                                final Executor executor);
}
