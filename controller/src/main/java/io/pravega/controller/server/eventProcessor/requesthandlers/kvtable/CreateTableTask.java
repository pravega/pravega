/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers.kvtable;

import com.google.common.base.Preconditions;
import io.pravega.controller.store.kvtable.CreateKVTableResponse;
import io.pravega.controller.store.kvtable.KVTableState;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.store.kvtable.KVTOperationContext;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static io.pravega.controller.task.Stream.TaskStepsRetryHelper.withRetries;
/**
 * Request handler for performing scale operations received from requeststream.
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
        KeyValueTableConfiguration config = new KeyValueTableConfiguration(partitionCount);

        return kvtMetadataStore.getKVTable(scope, kvt, null).getId()
        .thenCompose(id -> {
            if (!id.equals(kvTableId)) {
                return CompletableFuture.completedFuture(null);
            } else {
                withRetries(() -> this.kvtMetadataStore.createKeyValueTable(scope, kvt, config, creationTime, null, executor)
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
                                return kvtMetadataTasks.createNewSegments(scope, kvt, newSegments, requestId)
                                        .thenCompose(y -> {
                                            final KVTOperationContext context = kvtMetadataStore.createContext(scope, kvt);
                                            kvtMetadataStore.getVersionedState(scope, kvt, context, executor)
                                                    .thenCompose(state -> {
                                                        if (state.getObject().equals(State.CREATING)) {
                                                            kvtMetadataStore.updateVersionedState(scope, kvt, KVTableState.ACTIVE,
                                                                    state, context, executor);
                                                        }
                                                        return CompletableFuture.completedFuture(null);
                                                    });
                                            return CompletableFuture.completedFuture(null);
                                        });
                            }
                            return CompletableFuture.completedFuture(null);
                        }), executor);
                return CompletableFuture.completedFuture(null);
             }
        });
    }
}
