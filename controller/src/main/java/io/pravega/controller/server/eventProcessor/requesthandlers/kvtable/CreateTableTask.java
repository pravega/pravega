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
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.CreateKVTableResponse;
import io.pravega.controller.store.kvtable.KVTableState;
import io.pravega.controller.store.kvtable.KeyValueTable;
import io.pravega.controller.store.kvtable.KVTOperationContext;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Request handler for executing a create operation for a KeyValueTable.
 */
@Slf4j
public class CreateTableTask implements TableTask<CreateTableEvent> {

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
        String scope = request.getScopeName();
        String kvt = request.getKvtName();
        int partitionCount = request.getPartitionCount();
        long creationTime = request.getTimestamp();
        long requestId = request.getRequestId();
        String kvTableId = request.getTableId().toString();
        KeyValueTableConfiguration config = KeyValueTableConfiguration.builder()
                                            .partitionCount(partitionCount).build();

        return RetryHelper.withRetriesAsync(() -> getKeyValueTable(scope, kvt)
                .thenCompose(table -> table.getId()).thenCompose(id -> {
            if (!id.equals(kvTableId)) {
                log.debug("Skipped processing create event for KeyValueTable {}/{} with Id:{} as UUIDs did not match.", scope, kvt, id);
                return CompletableFuture.completedFuture(null);
            } else {
                return this.kvtMetadataStore.createKeyValueTable(scope, kvt, config, creationTime, null, executor)
                        .thenComposeAsync(response -> {
                            // only if its a new kvtable or an already existing non-active kvtable then we will create
                            // segments and change the state of the kvtable to active.
                            if (response.getStatus().equals(CreateKVTableResponse.CreateStatus.NEW) ||
                                    response.getStatus().equals(CreateKVTableResponse.CreateStatus.EXISTS_CREATING)) {
                                final int startingSegmentNumber = response.getStartingSegmentNumber();
                                final int minNumSegments = response.getConfiguration().getPartitionCount();
                                List<Long> newSegments = IntStream.range(startingSegmentNumber, startingSegmentNumber + minNumSegments)
                                        .boxed()
                                        .map(x -> NameUtils.computeSegmentId(x, 0))
                                        .collect(Collectors.toList());
                                kvtMetadataTasks.createNewSegments(scope, kvt, newSegments, requestId)
                                        .thenCompose(y -> {
                                            final KVTOperationContext context = kvtMetadataStore.createContext(scope, kvt);
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
                        }, executor);
             }
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, Integer.MAX_VALUE, executor);
    }

    private CompletableFuture<KeyValueTable> getKeyValueTable(String scope, String kvt) {
        return CompletableFuture.completedFuture(kvtMetadataStore.getKVTable(scope, kvt, null));
    }
}
