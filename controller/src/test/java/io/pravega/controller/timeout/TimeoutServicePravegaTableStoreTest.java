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

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.Version;
import lombok.extern.slf4j.Slf4j;

/**
 * Test class for TimeoutService.
 */
@Slf4j
public class TimeoutServicePravegaTableStoreTest extends TimeoutServiceTest {
    private final SegmentHelper segmentHelper;
    public TimeoutServicePravegaTableStoreTest() throws Exception {
        super();
        segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
    }

    @Override
    SegmentHelper getSegmentHelper() {
        return segmentHelper;
    }

    @Override
    protected StreamMetadataStore getStore() {
        return StreamStoreFactory.createPravegaTablesStore(segmentHelper, client, executor);
    }

    @Override
    Version getVersion(int i) {
        return Version.LongVersion.builder().longValue(i).build();
    }

    @Override
    Version getNextVersion(Version version) {
        return Version.LongVersion.builder().longValue(version.asLongVersion().getLongValue() + 1).build();
    }
}
