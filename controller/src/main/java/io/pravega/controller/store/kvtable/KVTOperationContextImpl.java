/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

class KVTOperationContextImpl<T> implements KVTOperationContext {

    private final KeyValueTable kvTable;

    KVTOperationContextImpl(KeyValueTable kvt) {
        this.kvTable = kvt;
    }

    public KeyValueTable getKeyValueTable(){
        return kvTable;
    }
}
