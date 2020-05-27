package io.pravega.shared.controller.event.kvtable;

import io.pravega.shared.controller.event.RequestProcessor;

import java.util.concurrent.CompletableFuture;

public interface TableRequestProcessor extends RequestProcessor {

    /**
     * Method to create a KeyValueTable event.
     *
     * @param createKVTEvent create event
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processCreateKVTable(CreateTableEvent createKVTEvent);

}
