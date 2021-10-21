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
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.impl.SerializedRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.RequestUnsupportedException;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;

import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import io.pravega.shared.controller.event.kvtable.DeleteTableEvent;
import io.pravega.shared.controller.event.kvtable.TableRequestProcessor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract common class for all KeyValueTable related request processing done over SerializedRequestHandler.
 * This implements TableRequestProcessor interface.
 * Its derived classes should implement specific processing that they wish to handle.
 */
@Slf4j
public abstract class AbstractTableRequestProcessor<T extends ControllerEvent> extends SerializedRequestHandler<T> implements TableRequestProcessor {

    @Getter
    protected final KVTableMetadataStore metadataStore;

    public AbstractTableRequestProcessor(KVTableMetadataStore store, ScheduledExecutorService executor) {
        super(executor);
        Preconditions.checkNotNull(store);
        this.metadataStore = store;
    }

    public String getProcessorName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public CompletableFuture<Void> processEvent(ControllerEvent controllerEvent) {
        return controllerEvent.process(this);
    }

    @Override
    public CompletableFuture<Void> processCreateKVTable(CreateTableEvent createTableEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }

    @Override
    public CompletableFuture<Void> processDeleteKVTable(DeleteTableEvent deleteTableEvent) {
        return Futures.failedFuture(new RequestUnsupportedException("Request Unsupported"));
    }
}
