/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.mocks;

import com.emc.pravega.service.server.MetadataRepository;
import com.emc.pravega.service.server.SegmentContainerManager;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import com.emc.pravega.service.storage.mocks.InMemoryStorageFactory;

/**
 * ServiceBuilder that uses all in-memory components. Upon closing this object, all data would be lost.
 */
public class InMemoryServiceBuilder extends ServiceBuilder {
    public InMemoryServiceBuilder(ServiceBuilderConfig config) {
        super(config);
    }

    @Override
    protected DurableDataLogFactory createDataLogFactory() {
        return new InMemoryDurableDataLogFactory();
    }

    @Override
    protected StorageFactory createStorageFactory() {
        return new InMemoryStorageFactory(super.executorService);
    }

    @Override
    protected MetadataRepository createMetadataRepository() {
        return new InMemoryMetadataRepository();
    }

    @Override
    protected SegmentContainerManager createSegmentContainerManager() {
        return new LocalSegmentContainerManager(getSegmentContainerRegistry(), this.segmentToContainerMapper);
    }
}
