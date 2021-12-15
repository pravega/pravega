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
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.stream.InvalidStreamException;
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
import io.pravega.controller.stream.api.grpc.v1.Controller;
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

    private static final int PAGE_LIMIT = 1000;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final KVTableMetadataStore kvtMetadataStore;
    private final ScheduledExecutorService executor;

    public DeleteScopeTask(final StreamMetadataTasks streamMetadataTasks,
                           final StreamMetadataStore streamMetaStore,
                           final KVTableMetadataStore kvtMetadataStore,
                           final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(streamMetaStore);
        Preconditions.checkNotNull(kvtMetadataStore);
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
        log.debug(requestId, "Deleting {} scope recursively", scope);
        return streamMetadataStore.checkScopeInDeletingTable(scope, context, executor).thenCompose( exists -> {
            if (exists) {
                return deleteScopeContent(scope, context, requestId);
            } else {
                log.info(requestId, "Skipping processing delete scope recursive for scope {} as scope" +
                        " does not exist in deleting table", scope);
                return CompletableFuture.completedFuture(null);
            }

        });
    }

    public CompletableFuture<Void> deleteScopeContent(String scopeName, OperationContext context, long requestId) {
        List<String> readerGroupList = new ArrayList<>();
        Iterator<Stream> iterator = listStreams(scopeName, context).asIterator();

        // Seal and delete streams and add entry to RGList
        while (iterator.hasNext()) {
            Stream stream = iterator.next();
            if (stream.getStreamName().startsWith(READER_GROUP_STREAM_PREFIX)) {
                readerGroupList.add(stream.getStreamName().substring(
                        READER_GROUP_STREAM_PREFIX.length()));
            }
            log.debug("Processing seal and delete stream for Stream {}", stream);
            Futures.getThrowingException(Futures.exceptionallyExpecting(streamMetadataTasks.sealStream(scopeName, stream.getStreamName(), requestId),
                    e -> {
                        Throwable unwrap = Exceptions.unwrap(e);
                        // If the stream was removed by another request while we attempted to seal it, we could get InvalidStreamException.
                        // ignore failures if the stream doesn't exist or we are unable to seal it.
                        return unwrap instanceof InvalidStreamException || unwrap instanceof ControllerFailureException;
                    }, Controller.UpdateStreamStatus.Status.STREAM_NOT_FOUND)
                    .thenCompose(sealed -> streamMetadataTasks.deleteStream(stream.getScope(), stream.getStreamName(), requestId)));
        }

        // Delete ReaderGroups
        for (String rgName: readerGroupList) {
            log.debug("Processing delete ReaderGroup for {}", rgName);
            Futures.getThrowingException(streamMetadataTasks.getReaderGroupConfig(scopeName, rgName, requestId)
                    .thenCompose(conf -> streamMetadataTasks.deleteReaderGroup(scopeName, rgName,
                            conf.getConfig().getReaderGroupId(), requestId)));
        }
        // Delete KVTs
        Iterator<KeyValueTableInfo> kvtIterator = listKVTs(scopeName, requestId, context).asIterator();
         while (kvtIterator.hasNext()) {
         KeyValueTableInfo kvt = kvtIterator.next();
             log.debug("Processing delete kvt for {}", kvt);
         Futures.getThrowingException(kvtMetadataStore
                 .deleteKeyValueTable(scopeName, kvt.getKeyValueTableName(), context, executor));
         }
        return streamMetadataStore.deleteScopeRecursive(scopeName, context, executor)
                .thenApply(status -> {
                    log.debug("Recursive Delete Scope returned with a status {}", status);
                    return null;
                });
    }

    private AsyncIterator<Stream> listStreams(String scopeName, OperationContext context) {
        final Function<String, CompletableFuture<Map.Entry<String, Collection<Stream>>>> function = token ->
                streamMetadataStore.listStream(scopeName, token, PAGE_LIMIT, executor, context)
                        .thenApply(result -> {
                            List<Stream> asStreamList = result.getKey().stream().map(m -> new StreamImpl(scopeName, m))
                                    .collect(Collectors.toList());
                            return new AbstractMap.SimpleEntry<>(result.getValue(), asStreamList);
                        });
        return new ContinuationTokenAsyncIterator<>(function, "");
    }

     private AsyncIterator<KeyValueTableInfo> listKVTs(final String scopeName, final long requestId, OperationContext context) {
         final Function<String, CompletableFuture<Map.Entry<String, Collection<KeyValueTableInfo>>>> function = token ->
                 kvtMetadataStore.listKeyValueTables(scopeName, token, PAGE_LIMIT, context, executor)
                         .thenApply(result -> {
                             List<KeyValueTableInfo> kvtList = result.getLeft().stream()
                                     .map(m -> new KeyValueTableInfo(scopeName, m))
                                     .collect(Collectors.toList());
                             return new AbstractMap.SimpleEntry<>(result.getValue(), kvtList);
                         });
         return new ContinuationTokenAsyncIterator<>(function, "");
     }
}
