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

/**
 * Interface for defining an operation context.
 * A context caches metadata fetches so within a context if for the same entity, multiple
 * read operations against the store are requested, the values are served from the context's cache.
 */
public interface OperationContext {

    Stream getStream();
}
