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


import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * RequestSweeper test cases.
 */
@Slf4j
public class PravegaTablesRequestSweeperTest extends RequestSweeperTest {
    SegmentHelper helper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
    
    @Override
    StreamMetadataStore getStream() {
        return StreamStoreFactory.createPravegaTablesStore(helper, AuthHelper.getDisabledAuthHelper(), cli, executor);
    }

    @Override
    HostIndex getHostIndex() {
        return new ZKHostIndex(cli, "/hostRequestIndex", executor);
    }
}