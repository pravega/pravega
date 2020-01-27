/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Tests for ControllerService With ZK Stream Store
 */
@Slf4j
public class ControllerServiceWithZKStreamTest extends ControllerServiceWithStreamTest {
    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createZKStore(zkClient, executor);
    }
}
