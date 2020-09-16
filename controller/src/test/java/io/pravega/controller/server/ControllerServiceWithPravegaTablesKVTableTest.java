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

import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j 
public class ControllerServiceWithPravegaTablesKVTableTest extends ControllerServiceWithKVTableTest {
    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createPravegaTablesStore(segmentHelperMock,
                GrpcAuthHelper.getDisabledAuthHelper(), zkClient, executor);
    }

    @Override
    KVTableMetadataStore getKVTStore() {
        return KVTableStoreFactory.createPravegaTablesStore(segmentHelperMock,
                GrpcAuthHelper.getDisabledAuthHelper().getDisabledAuthHelper(), zkClient, executor);
    }
}
