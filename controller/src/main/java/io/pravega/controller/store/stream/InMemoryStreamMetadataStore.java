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
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.internal.LinkedTreeMap;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.common.lang.Int96;
import io.pravega.controller.store.InMemoryScope;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.index.InMemoryHostIndex;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory stream store.
 */
@Slf4j
public class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    @GuardedBy("$lock")
    private final Map<String, InMemoryStream> streams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, Integer> deletedStreams = new HashMap<>();

    @GuardedBy("$lock")
    private final HashMap<String, InMemoryScope> scopes = new HashMap<>();
    @GuardedBy("$lock")
    private final HashMap<String, InMemoryScope> deletingScopes = new HashMap<>();
    private final AtomicInteger position = new AtomicInteger();
    @GuardedBy("$lock")
    private final LinkedTreeMap<String, Integer> orderedScopes = new LinkedTreeMap<>();

    @GuardedBy("$lock")
    private final Map<String, InMemoryReaderGroup> readerGroups = new HashMap<>();


    @GuardedBy("$lock")
    private final Map<Integer, List<String>> bucketedStreams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, RetentionPolicy> streamPolicyMap = new HashMap<>();

    private final AtomicInt96 counter;

    public InMemoryStreamMetadataStore() {
        super(new InMemoryHostIndex(), new InMemoryHostIndex());
        this.counter = new AtomicInt96();
    }

    @Synchronized
    public boolean scopeExists(String scopeName) {
        return scopes.containsKey(scopeName);
    }

    @Synchronized
    public boolean scopeInDeletingTable(String scopeName) {
        return deletingScopes.containsKey(scopeName);
    }

    @Override
    @Synchronized
    Stream newStream(String scopeName, String name) {
        String key = scopedStreamName(scopeName, name);
        if (streams.containsKey(key)) {
            return streams.get(key);
        } else {
            return new InMemoryStream(scopeName, name);
        }
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return CompletableFuture.completedFuture(counter.incrementAndGet());
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkScopeExists(String scope, OperationContext context, Executor executor) {
        log.debug("InMemory checking if scope exists");
        return CompletableFuture.completedFuture(scopes.containsKey(scope));
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> isScopeSealed(String scope, OperationContext context, Executor executor) {
        log.debug("InMemory checking if scope exists");
        return CompletableFuture.completedFuture(deletingScopes.containsKey(scope));
    }

    @Override
    Version getEmptyVersion() {
        return Version.IntVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
    }

    @Override
    @Synchronized
    @SneakyThrows
    ReaderGroup newReaderGroup(String scope, String name) {
        final String scopedRGName = scopedStreamName(scope, name);
        if (readerGroups.containsKey(scopedRGName)) {
            return readerGroups.get(scopedRGName);
        } else {
           return new InMemoryReaderGroup(scope, name);
        }
    }

    @Override
    @Synchronized
    Scope newScope(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName);
        } else {
            return new InMemoryScope(scopeName);
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<CreateStreamResponse> createStream(final String scopeName, final String streamName,
                                                                final StreamConfiguration configuration,
                                                                final long timeStamp,
                                                                final OperationContext ctx,
                                                                final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        if (scopes.containsKey(scopeName)) {
            InMemoryStream stream = (InMemoryStream) getStream(scopeName, streamName, context);
            return getSafeStartingSegmentNumberFor(scopeName, streamName, context, executor)
                    .thenCompose(startingSegmentNumber -> {
                        CompletableFuture<CreateStreamResponse> future = stream.create(
                                configuration, timeStamp, startingSegmentNumber, context);
                        return future.thenCompose(status -> {
                                  streams.put(scopedStreamName(scopeName, streamName), stream);
                                  return scopes.get(scopeName).addStreamToScope(streamName, context).thenApply(v -> status);
                              });
                    });
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName, final String streamName,
                                                        final OperationContext context, final Executor executor) {
        return CompletableFuture.completedFuture(streams.containsKey(scopedStreamName(scopeName, streamName)));
    }

    @Override
    @Synchronized
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName,
                                                                      final OperationContext context, final Executor executor) {
        final Integer safeStartingSegmentNumber = deletedStreams.get(scopedStreamName(scopeName, streamName));
        return CompletableFuture.completedFuture((safeStartingSegmentNumber != null) ? safeStartingSegmentNumber + 1 : 0);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> deleteStream(final String scopeName, final String streamName,
                                                final OperationContext ctx,
                                                final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        String scopedStreamName = scopedStreamName(scopeName, streamName);
        if (scopes.containsKey(scopeName) && streams.containsKey(scopedStreamName)) {
            return getCreationTime(scopeName, streamName, context, executor)
                    .thenCompose(time -> scopes.get(scopeName).removeStreamFromScope(streamName, context))
                    .thenCompose(v -> super.deleteStream(scopeName, streamName, context, executor))
                    .thenAccept(v -> streams.remove(scopedStreamName));
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, streamName));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> startUpdateConfiguration(final String scopeName,
                                                            final String streamName,
                                                            final StreamConfiguration configuration,
                                                            final OperationContext ctx,
                                                            final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        if (scopes.containsKey(scopeName)) {
            String scopeStreamName = scopedStreamName(scopeName, streamName);
            if (!streams.containsKey(scopeStreamName)) {
                return Futures.
                        failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeStreamName));
            }
            return streams.get(scopeStreamName).startUpdateConfiguration(configuration, context);
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Override
    public CompletableFuture<Void> addStreamTagsToIndex(String scope, String name, StreamConfiguration config, OperationContext context, Executor executor) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeTagsFromIndex(String scope, String name, Set<String> tagsRemoved, OperationContext context, Executor executor) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> addReaderGroupToScope(String scopeName, String rgName, UUID readerGroupId,
                                                         final OperationContext context, final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            return Futures.toVoid(scopes.get(scopeName).addReaderGroupToScope(rgName, readerGroupId));
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    // region ReaderGroup
    @Override
    public CompletableFuture<Void> createReaderGroup(final String scope,
                                                     final String name,
                                                     final ReaderGroupConfig configuration,
                                                     final long createTimestamp,
                                                     final OperationContext context,
                                                     final Executor executor) {
        if (scopes.containsKey(scope)) {
           InMemoryReaderGroup readerGroup = (InMemoryReaderGroup) getReaderGroup(scope, name, context);
           readerGroups.put(scopedStreamName(scope, name), readerGroup);
           return readerGroup.create(configuration, createTimestamp, context);
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scope));
        }
    }

    @Override
    public CompletableFuture<Void> deleteReaderGroup(final String scopeName, final String rgName,
                                                     final OperationContext context, final Executor executor) {
        String scopedRGName = scopedStreamName(scopeName, rgName);
        if (scopes.containsKey(scopeName) && readerGroups.containsKey(scopedRGName)) {
            readerGroups.remove(scopedRGName);
            return scopes.get(scopeName).removeReaderGroupFromScope(rgName);
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, rgName));
        }
    }

    @Override
    public CompletableFuture<Boolean> checkReaderGroupExists(final String scopeName,
                                                             final String rgName,
                                                             OperationContext context, Executor executor) {
        return Futures.completeOn(((InMemoryScope) getScope(scopeName, context)).checkReaderGroupExistsInScope(rgName), executor);
    }

    @Override
    public CompletableFuture<Void> sealScope(final String scopeName, OperationContext ctx, ScheduledExecutorService executor) {
        return CompletableFuture.completedFuture(null);
    }

    // endregion

    @Override
    @Synchronized
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName, OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        if (!scopes.containsKey(scopeName)) {
            InMemoryScope scope = (InMemoryScope) getScope(scopeName, context);
            return scope.createScope(context)
                 .thenApply(x -> {
                     scopes.put(scopeName, scope);
                     orderedScopes.put(scopeName, position.incrementAndGet());
                     return CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SUCCESS).build();
                 });
        } else {
            return CompletableFuture.completedFuture(CreateScopeStatus.newBuilder().setStatus(
                    CreateScopeStatus.Status.SCOPE_EXISTS).build());
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName, OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName).listStreamsInScope(context).thenCompose(streams -> {
                if (streams.isEmpty()) {
                    return scopes.get(scopeName).deleteScope(context)
                                 .thenApply(x -> {
                                     scopes.remove(scopeName);
                                     orderedScopes.remove(scopeName);
                                     return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SUCCESS).build();
                                 });
                } else {
                    return CompletableFuture.completedFuture(
                            DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build());
                }
            });
        } else {
            return CompletableFuture.completedFuture(DeleteScopeStatus.newBuilder().setStatus(
                    DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build());
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<String> getScopeConfiguration(final String scopeName, OperationContext context, Executor executor) {
        if (scopes.containsKey(scopeName)) {
            return CompletableFuture.completedFuture(scopeName);
        } else {
            return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<List<String>> listScopes(Executor executor, long requestId) {
        return CompletableFuture.completedFuture(new ArrayList<>(scopes.keySet()));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listScopes(String continuationToken, int limit, Executor executor,
                                                                    long requestId) {
        List<String> result = new ArrayList<>();
        String nextToken = continuationToken;
        int start = Strings.isNullOrEmpty(continuationToken) ? 0 : Integer.parseInt(continuationToken);
        for (Map.Entry<String, Integer> x : orderedScopes.entrySet()) {
            if (x.getValue() > start) {
                result.add(x.getKey());
                nextToken = x.getValue().toString();
            }
            if (result.size() == limit) {
                break;
            }
        }
        return CompletableFuture.completedFuture(new ImmutablePair<>(result, nextToken));
    }

    /**
     * List the streams in scope.
     *
     * @param scopeName Name of scope
     * @return List of streams in scope
     */
    @Override
    @Synchronized
    public CompletableFuture<Map<String, StreamConfiguration>> listStreamsInScope(final String scopeName,
                                                                                  final OperationContext ctx,
                                                                                  final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        InMemoryScope inMemoryScope = scopes.get(scopeName);
        if (inMemoryScope != null) {
            return inMemoryScope.listStreamsInScope(context)
                    .thenApply(streams -> {
                        HashMap<String, StreamConfiguration> result = new HashMap<>();
                        for (String stream : streams) {
                            State state = getState(scopeName, stream, true, null, executor).join();
                            StreamConfiguration configuration = Futures.exceptionallyExpecting(
                                    getConfiguration(scopeName, stream, null, executor),
                                    e -> e instanceof StoreException.DataNotFoundException, null).join();
                            if (configuration != null && !state.equals(State.CREATING) && !state.equals(State.UNKNOWN)) {
                                result.put(stream, configuration);
                            }
                        }
                        return result;
                    });
        } else {
            return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Override
    @Synchronized
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, int lastActiveSegment,
                                                    OperationContext context, final Executor executor) {
        Integer oldLastActiveSegment = deletedStreams.put(getScopedStreamName(scope, stream), lastActiveSegment);
        Preconditions.checkArgument(oldLastActiveSegment == null || lastActiveSegment >= oldLastActiveSegment);
        log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
        return CompletableFuture.completedFuture(null);
    }
    
    private String scopedStreamName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }

    @Override
    public void close() throws IOException {
        
    }
    
    @VisibleForTesting
    void addStreamObjToScope(String scopeName, String streamName) {
        InMemoryStream stream = (InMemoryStream) getStream(scopeName, streamName, null);

        streams.put(scopedStreamName(scopeName, streamName), stream);
        scopes.get(scopeName).addStreamToScope(streamName, null).join();
    }
}
