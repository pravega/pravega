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

import io.pravega.controller.store.Scope;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.function.Function;

class OperationContextImpl implements OperationContext {
    private final Function<OperationContext, Scope> scopeFactory;
    private final Function<OperationContext, Stream> streamFactory;
    @GuardedBy("$lock")
    private Scope scope;
    @GuardedBy("$lock")
    private Stream stream;
    
    OperationContextImpl(Function<OperationContext, Scope> scopeFactory, Function<OperationContext, Stream> streamFactory) {
        this.scopeFactory = scopeFactory;
        this.streamFactory = streamFactory;
    }
    
    @Synchronized
    Scope getScope() {
        if (scope == null) {
            scope = scopeFactory.apply(this);
        }
        return scope;
    }
    
    @Synchronized
    Stream getStream() {
        if (stream == null) {
            stream = streamFactory.apply(this);
        }
        return stream;
    }
}
