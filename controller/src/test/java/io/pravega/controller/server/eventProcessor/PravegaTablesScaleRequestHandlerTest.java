/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;

import static org.mockito.Mockito.mock;

public class PravegaTablesScaleRequestHandlerTest extends ScaleRequestHandlerTest {
    SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables();

    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createPravegaTablesStore(segmentHelper, zkClient, executor);
    }
}
