/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import org.junit.Before;

public class SecureStreamMetadataTasksTest extends StreamMetadataTasksTest {
    @Override
    @Before
    public void setup() throws Exception {
        this.authEnabled = true;
        super.setup();
    }

    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createInMemoryStore(executor);
    }
}
