/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event.kvtable;

import io.pravega.shared.controller.event.RequestProcessor;

import java.util.concurrent.CompletableFuture;

public interface TableRequestProcessor extends RequestProcessor {

    /**
<<<<<<< HEAD
     * Method to create a KeyValueTable.
=======
     * Method to create a KeyValueTable event.
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
     *
     * @param createKVTEvent create event
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processCreateKVTable(CreateTableEvent createKVTEvent);

<<<<<<< HEAD
    /**
     * Method to delete a KeyValueTable.
     *
     * @param deleteEvent deletion event
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processDeleteKVTable(DeleteTableEvent deleteEvent);

=======
>>>>>>> Issue 4796: (KeyValue Tables) CreateAPI for Key Value Tables (#4797)
}
