/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;

public class PravegaTablesStreamMetadataTasksTest extends StreamMetadataTasksTest {
    SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
    
    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createPravegaTablesStore(segmentHelper, zkClient, executor);
    }
}
