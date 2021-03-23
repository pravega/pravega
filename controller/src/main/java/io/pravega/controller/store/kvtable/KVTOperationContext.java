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

import io.pravega.controller.store.Scope;
import io.pravega.controller.store.stream.OperationContext;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
class KVTOperationContext implements OperationContext {
    private final Scope scope;
    private final KeyValueTable kvTable;
    private final long requestId;
    private final long operationStartTime = System.currentTimeMillis();
}