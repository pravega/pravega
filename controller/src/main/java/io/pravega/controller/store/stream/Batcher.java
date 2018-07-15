/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

/**
 * Batcher interface to facilitate callers to group their data into different batches.
 * Implementation of this interface should manage the batch identifier and update it's value
 * based on their business logic.
 */
public interface Batcher<T> {
    T getLatestBatch();
}
