/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.VersionedMetadata;

public class PravegaTablesScaleRequestHandlerTest extends ScaleRequestHandlerTest {
    SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);

    @Override
    <T> Number getVersionNumber(VersionedMetadata<T> versioned) {
        return (int) versioned.getVersion().asLongVersion().getLongValue();
    }

    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createPravegaTablesStore(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(), zkClient, executor);
    }
}
