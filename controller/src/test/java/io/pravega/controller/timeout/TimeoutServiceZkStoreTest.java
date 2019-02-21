/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.timeout;

import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Test class for TimeoutService.
 */
@Slf4j
public class TimeoutServiceZkStoreTest extends TimeoutServiceTest {
    public TimeoutServiceZkStoreTest() throws Exception {
        super();
    }

    @Override
    protected StreamMetadataStore getStore() {
        return StreamStoreFactory.createZKStore(client, executor);
    }
}
