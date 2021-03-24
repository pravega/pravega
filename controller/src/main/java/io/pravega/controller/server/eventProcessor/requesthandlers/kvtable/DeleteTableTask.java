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
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTOperationContext;
import io.pravega.controller.store.kvtable.KeyValueTable;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.controller.event.kvtable.DeleteTableEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for executing a delete operation for a KeyValueTable.
 */
@Slf4j
public class DeleteTableTask implements TableTask<DeleteTableEvent> {

    private final KVTableMetadataStore kvtMetadataStore;
    private final TableMetadataTasks kvtMetadataTasks;
    private final ScheduledExecutorService executor;

    public DeleteTableTask(final KVTableMetadataStore kvtMetaStore,
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
    public CompletableFuture<Void> execute(final DeleteTableEvent request) {
        String scope = request.getScope();
        String kvt = request.getKvtName();
        long requestId = request.getRequestId();
        String kvTableId = request.getTableId().toString();

        return RetryHelper.withRetriesAsync(() -> getKeyValueTable(scope, kvt)
                .thenCompose(table -> table.getId()).thenCompose(id -> {
            if (!id.equals(kvTableId)) {
                log.debug("Skipped processing delete event for KeyValueTable {}/{} with Id:{} as UUIDs did not match.", scope, kvt, id);
                return CompletableFuture.completedFuture(null);
            } else {
                final KVTOperationContext context = kvtMetadataStore.createContext(scope, kvt);
                return Futures.exceptionallyExpecting(kvtMetadataStore.getAllSegmentIds(scope, kvt, context, executor)
                        .thenComposeAsync(allSegments ->
                                        kvtMetadataTasks.deleteSegments(scope, kvt, allSegments, kvtMetadataTasks.retrieveDelegationToken(), requestId), executor),
                        e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                        .thenCompose(v -> this.kvtMetadataStore.deleteKeyValueTable(scope, kvt, context, executor));
             }
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, Integer.MAX_VALUE, executor);
    }

    private CompletableFuture<KeyValueTable> getKeyValueTable(String scope, String kvt) {
        return CompletableFuture.completedFuture(kvtMetadataStore.getKVTable(scope, kvt, null));
    }
}
