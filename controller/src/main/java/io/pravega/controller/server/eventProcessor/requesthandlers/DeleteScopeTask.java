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
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.base.Preconditions;

import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.NoSuchScopeException;
import io.pravega.client.stream.ReaderGroupNotFoundException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.DeleteScopeEvent;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.READER_GROUP_STREAM_PREFIX;

/**
 * Request handler for executing a delete operation for a ReaderGroup.
 */
public class DeleteScopeTask implements ScopeTask<DeleteScopeEvent> {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(DeleteScopeTask.class));

    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final KVTableMetadataStore kvtMetadataStore;
    private final ScheduledExecutorService executor;

    public DeleteScopeTask(final StreamMetadataTasks streamMetadataTasks,
                           final StreamMetadataStore streamMetaStore,
                           final KVTableMetadataStore kvtMetadataStore,
                           final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetaStore);
        Preconditions.checkNotNull(executor);
        this.streamMetadataStore = streamMetaStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.kvtMetadataStore = kvtMetadataStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final DeleteScopeEvent request) {
        String scope = request.getScope();
        long requestId = request.getRequestId();
        final OperationContext context = streamMetadataStore.createScopeContext(scope, requestId);
        log.debug(requestId, "Deleting {} scope", scope);
        return streamMetadataStore.checkScopeExists(scope, context, executor).thenCompose( exists -> {
            if (!exists) {
                log.warn(requestId, "Scope not found: {}", scope);
                throw new NoSuchScopeException();
            }
            return CompletableFuture.completedFuture(null);
        }).thenCompose(v1 -> {
            try {
                deleteScopeContent(scope, context, requestId);
            } catch (DeleteScopeFailedException e) {
                e.printStackTrace();
            }
            streamMetadataStore.deleteScopeRecursive(scope, context, executor);
            return CompletableFuture.completedFuture(null);
        });
    }

    public void deleteScopeContent(String scopeName, OperationContext context, long requestId) throws DeleteScopeFailedException {
        List<String> readerGroupList = new ArrayList<>();
        Iterator<Stream> iterator = listStreams(scopeName, context).asIterator();

        // Seal and delete streams and add entry to RGList
        while (iterator.hasNext()) {
            Stream stream = iterator.next();
            if (stream.getStreamName().startsWith(READER_GROUP_STREAM_PREFIX)) {
                readerGroupList.add(stream.getStreamName().substring(
                        READER_GROUP_STREAM_PREFIX.length()));
            }
            try {
                streamMetadataTasks.sealStream(scopeName, stream.getStreamName(), requestId);
                streamMetadataTasks.deleteStream(scopeName, stream.getStreamName(), requestId);
            } catch (Exception e) {
                String message = String.format("Failed to seal and delete stream %s", stream.getStreamName());
                throw new DeleteScopeFailedException(message, e);
            }
        }
        // Delete KVTs
        deleteKVTs(scopeName, context);

        // Delete ReaderGroups
        for (String rgName: readerGroupList) {
            try {
                streamMetadataTasks.getReaderGroupConfig(scopeName, rgName, requestId)
                        .thenCompose(conf -> streamMetadataTasks.deleteReaderGroup(scopeName, rgName,
                                conf.getConfig().getReaderGroupId(), requestId));
            } catch (Exception e) {
                if (Exceptions.unwrap(e) instanceof ReaderGroupNotFoundException) {
                    continue;
                }
                String message = String.format("Failed to delete reader group %s", rgName);
                throw new DeleteScopeFailedException(message, e);
            }
        }
    }

    public AsyncIterator<Stream> listStreams(String scopeName, OperationContext context) {
        final Function<String, CompletableFuture<Map.Entry<String, Collection<Stream>>>> function = token ->
                streamMetadataStore.listStream(scopeName, token, 1000, executor, context)
                        .thenApply(result -> {
                            List<Stream> asStreamList = result.getKey().stream().map(m -> new StreamImpl(scopeName, m))
                                    .collect(Collectors.toList());
                            return new AbstractMap.SimpleEntry<>(result.getValue(), asStreamList);
                        });

        return new ContinuationTokenAsyncIterator<>(function, "");
    }

    private ContinuationTokenAsyncIterator<String, KeyValueTableInfo> listKVTs(String scopeName, OperationContext context) {
        final Function<String, CompletableFuture<Map.Entry<String, Collection<KeyValueTableInfo>>>> function = token ->
                kvtMetadataStore.listKeyValueTables(scopeName, token, 1000, context, executor)
                        .thenApply(result -> {
                            List<KeyValueTableInfo> kvTablesList = result.getLeft().stream()
                                    .map(kvt -> new KeyValueTableInfo(scopeName, kvt))
                                    .collect(Collectors.toList());

                            return new AbstractMap.SimpleEntry<>(result.getValue(), kvTablesList);
                        });
        return new ContinuationTokenAsyncIterator<>(function, "");
    }

    private CompletableFuture<Void> deleteKVTs(String scopeName, OperationContext context) throws DeleteScopeFailedException {
        Iterator<KeyValueTableInfo> kvtIterator = listKVTs(scopeName, context).asIterator();
        while (kvtIterator.hasNext()) {
            KeyValueTableInfo kvt = kvtIterator.next();
            try {
                Futures.getThrowingException(kvtMetadataStore.deleteKeyValueTable(scopeName, kvt.getKeyValueTableName(), context, executor));
            } catch (Exception e) {
                String message = String.format("Failed to delete key-value table %s", kvt.getKeyValueTableName());
                throw new DeleteScopeFailedException(message, e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }
}
