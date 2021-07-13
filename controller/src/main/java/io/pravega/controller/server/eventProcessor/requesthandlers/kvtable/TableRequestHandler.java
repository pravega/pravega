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


import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import io.pravega.shared.controller.event.kvtable.DeleteTableEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class TableRequestHandler extends AbstractTableRequestProcessor<ControllerEvent> {

    private final CreateTableTask createTask;
    private final DeleteTableTask deleteTask;

    public TableRequestHandler(CreateTableTask createTask, DeleteTableTask deleteTask,
                               KVTableMetadataStore store,
                               ScheduledExecutorService executor) {
        super(store, executor);
        this.createTask = createTask;
        this.deleteTask = deleteTask;
    }

    @Override
    public CompletableFuture<Void> processCreateKVTable(CreateTableEvent createKVTEvent) {
        log.info("Processing create request {} for KeyValueTable {}/{}",
                createKVTEvent.getRequestId(), createKVTEvent.getScopeName(), createKVTEvent.getKvtName());
        return createTask.execute(createKVTEvent);
    }

    @Override
    public CompletableFuture<Void> processDeleteKVTable(DeleteTableEvent deleteKVTEvent) {
        log.info("Processing delete request {} for KeyValueTable {}/{}",
                deleteKVTEvent.getRequestId(), deleteKVTEvent.getScope(), deleteKVTEvent.getKvtName());
        return deleteTask.execute(deleteKVTEvent);
    }
}
