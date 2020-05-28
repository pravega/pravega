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

import io.pravega.controller.store.OperationContext;


class StreamOperationContext implements OperationContext<Stream> {
    private final Stream stream;

    StreamOperationContext(Stream stream) {
        this.stream = stream;
    }

    public Stream getObject() {
        return this.stream;
    }
}
