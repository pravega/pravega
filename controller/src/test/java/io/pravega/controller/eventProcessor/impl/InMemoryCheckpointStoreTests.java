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
package io.pravega.controller.eventProcessor.impl;

import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;

/**
 * Tests for in-memory checkpoint store.
 */
public class InMemoryCheckpointStoreTests extends CheckpointStoreTests {
    @Override
    public void setupCheckpointStore() {
        this.checkpointStore = CheckpointStoreFactory.createInMemoryStore();
    }

    @Override
    public void cleanupCheckpointStore() {

    }
}
