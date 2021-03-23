/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import io.pravega.controller.store.Scope;
import lombok.Getter;

class StreamOperationContext implements OperationContext {
    @Getter
    private final Scope scope;
    @Getter
    private final Stream stream;
    @Getter
    private final long requestId;
    @Getter
    private final long operationStartTime = System.currentTimeMillis();

    StreamOperationContext(Scope scope, Stream stream, long requestId) {
        Preconditions.checkNotNull(scope, "scope cannot be null");
        Preconditions.checkNotNull(scope, "stream cannot be null");
        this.scope = scope;
        this.stream = stream;
        this.requestId = requestId;
    }
}
