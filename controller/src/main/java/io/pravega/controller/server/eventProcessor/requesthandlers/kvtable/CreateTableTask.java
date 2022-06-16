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
package io.pravega.controller.server.eventProcessor.requesthandlers.kvtable;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.CreateKVTableResponse;
import io.pravega.controller.store.kvtable.KVTableState;
import io.pravega.controller.store.kvtable.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Request handler for executing a create operation for a KeyValueTable.
 */
public class CreateTableTask implements TableTask<CreateTableEvent> {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(CreateTableTask.class));

    private final KVTableMetadataStore kvtMetadataStore;
    private final TableMetadataTasks kvtMetadataTasks;
    private final ScheduledExecutorService executor;

    public CreateTableTask(final KVTableMetadataStore kvtMetaStore,
                           final TableMetadataTasks kvtMetaTasks,
                           final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(kvtMetaStore);
        Preconditions.checkNotNull(kvtMetaTasks);
        Preconditions.checkNotNull(executor);
        this.kvtMetadataStore = kvtMetaStore;
        this.kvtMetadataTasks = kvtMetaTasks;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final CreateTableEvent request) {
        Timer timer = new Timer();
        String scope = request.getScopeName();
        String kvt = request.getKvtName();
        int partitionCount = request.getPartitionCount();
        int primaryKeyLength = request.getPrimaryKeyLength();
        int secondaryKeyLength = request.getSecondaryKeyLength();
        long creationTime = request.getTimestamp();
        long requestId = request.getRequestId();
        long rolloverSize = request.getRolloverSizeBytes();
        String kvTableId = request.getTableId().toString();
        KeyValueTableConfiguration config = KeyValueTableConfiguration.builder()
                                            .partitionCount(partitionCount)
                                            .primaryKeyLength(primaryKeyLength)
                                            .secondaryKeyLength(secondaryKeyLength)
                                            .rolloverSizeBytes(rolloverSize)
                                            .build();

        final OperationContext context = kvtMetadataStore.createContext(scope, kvt, requestId);
        return RetryHelper.withRetriesAsync(() ->
                getKeyValueTable(scope, kvt)
                .thenCompose(table -> table.getId(context)).thenCompose(id -> {
            if (!id.equals(kvTableId)) {
                log.debug(requestId, "Skipped processing create event for KeyValueTable {}/{} with Id:{} as UUIDs did not match.", scope, kvt, id);
                return CompletableFuture.completedFuture(null);
            } else {
                return kvtMetadataStore.isScopeSealed(scope, context, executor).thenCompose(isScopeSealed -> {
                    if (isScopeSealed) {
                        log.warn(requestId, "Scope {} is in sealed state: ", scope);
                        throw new IllegalStateException("Scope in sealed state: " + scope);
                    }
                    return this.kvtMetadataStore.createKeyValueTable(scope, kvt, config, creationTime, context, executor)
                            .thenComposeAsync(response -> {
                                // only if its a new kvtable or an already existing non-active kvtable then we will create
                                // segments and change the state of the kvtable to active.
                                if (response.getStatus().equals(CreateKVTableResponse.CreateStatus.NEW) ||
                                        response.getStatus().equals(CreateKVTableResponse.CreateStatus.EXISTS_CREATING)) {
                                    final int startingSegmentNumber = response.getStartingSegmentNumber();
                                    final int minNumSegments = response.getConfiguration().getPartitionCount();
                                    final int keyLength = response.getConfiguration().getPrimaryKeyLength() + response.getConfiguration().getSecondaryKeyLength();
                                    List<Long> newSegments = IntStream.range(startingSegmentNumber, startingSegmentNumber + minNumSegments)
                                            .boxed()
                                            .map(x -> NameUtils.computeSegmentId(x, 0))
                                            .collect(Collectors.toList());
                                    kvtMetadataTasks.createNewSegments(scope, kvt, newSegments, keyLength, requestId, config.getRolloverSizeBytes())
                                            .thenCompose(y -> {
                                                kvtMetadataStore.getVersionedState(scope, kvt, context, executor)
                                                        .thenCompose(state -> {
                                                            if (state.getObject().equals(KVTableState.CREATING)) {
                                                                kvtMetadataStore.updateVersionedState(scope, kvt, KVTableState.ACTIVE,
                                                                        state, context, executor);
                                                            }
                                                            return CompletableFuture.completedFuture(null);
                                                        });
                                                return CompletableFuture.completedFuture(null);
                                            });
                                }
                                return CompletableFuture.completedFuture(null);
                            }, executor).thenAccept(v -> StreamMetrics.getInstance().controllerEventProcessorCreateTableEvent(timer.getElapsed()));
                });
             }
                        }), e -> Exceptions.unwrap(e) instanceof RetryableException, Integer.MAX_VALUE, executor);
    }

    private CompletableFuture<KeyValueTable> getKeyValueTable(String scope, String kvt) {
        return CompletableFuture.completedFuture(kvtMetadataStore.getKVTable(scope, kvt, null));
    }
}
