/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task;


import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Task test cases.
 */
@Slf4j
public class PravegaTableTaskTest extends TaskTest {
    @Override
    protected StreamMetadataStore getStream() {
        return StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor),
                GrpcAuthHelper.getDisabledAuthHelper(), cli, executor);
    }
}

