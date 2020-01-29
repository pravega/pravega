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

import io.pravega.controller.store.client.impl.StoreClientConfigImpl;

/**
 * In-memory store based ControllerServiceMain tests.
 */
public class InMemoryControllerServiceMainTest extends ControllerServiceMainTest {
    public InMemoryControllerServiceMainTest() {
        super(true);
    }

    @Override
    public void setup() {
        storeClientConfig = StoreClientConfigImpl.withInMemoryClient();
    }

    @Override
    public void tearDown() {
    }
}
