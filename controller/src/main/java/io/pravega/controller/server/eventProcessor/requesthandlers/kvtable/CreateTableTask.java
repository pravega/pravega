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
import io.pravega.controller.store.kvtable.TableMetadataStore;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.store.stream.CreateStreamResponse;
import io.pravega.controller.store.kvtable.KVTOperationContext;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Request handler for performing scale operations received from requeststream.
 */
@Slf4j
public class CreateTableTask implements TableTask<CreateTableEvent> {

    private final TableMetadataStore kvtMetadataStore;
    private final TableMetadataTasks kvtMetadataTasks;
    private final ScheduledExecutorService executor;

    public CreateTableTask(final TableMetadataStore kvtMetaStore,
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
                // we don;t execute the request if Table IDs do not match
                return CompletableFuture.completedFuture(null);
            } else {
                this.kvtMetadataStore.createKeyValueTable(scope, kvt, config, creationTime, null, executor)
                        .thenComposeAsync(response -> {
                            // only if its a new kvtable or an already existing non-active kvtable then we will create
                            // segments and change the state of the kvtable to active.
                            if (response.getStatus().equals(CreateKVTableResponse.CreateStatus.NEW) ||
                                    response.getStatus().equals(CreateStreamResponse.CreateStatus.EXISTS_CREATING)) {
                                final int startingSegmentNumber = response.getStartingSegmentNumber();
                                final int minNumSegments = response.getConfiguration().getPartitionCount();
                                List<Long> newSegments = IntStream.range(startingSegmentNumber, startingSegmentNumber + minNumSegments)
                                        .boxed()
                                        .map(x -> NameUtils.computeSegmentId(x, 0))
                                        .collect(Collectors.toList());
                                return kvtMetadataTasks.notifyNewSegments(scope, kvt, newSegments, requestId)
                                        .thenCompose(y -> {
                                            final KVTOperationContext context = kvtMetadataStore.createContext(scope, kvt);
                                            //TODO: add withRetries
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
                        });
                return CompletableFuture.completedFuture(null);
             }
        });
    }

    private Controller.CreateKeyValueTableStatus.Status translate(CreateKVTableResponse.CreateStatus status) {
        Controller.CreateKeyValueTableStatus.Status retVal;
        switch (status) {
            case NEW:
                retVal = Controller.CreateKeyValueTableStatus.Status.SUCCESS;
                break;
            case EXISTS_ACTIVE:
            case EXISTS_CREATING:
                retVal = Controller.CreateKeyValueTableStatus.Status.TABLE_EXISTS;
                break;
            case FAILED:
            default:
                retVal = Controller.CreateKeyValueTableStatus.Status.FAILURE;
                break;
        }
        return retVal;
    }


}
