/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.timeout;

import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.Version;
import lombok.extern.slf4j.Slf4j;

import static org.mockito.Mockito.mock;

/**
 * Test class for TimeoutService.
 */
@Slf4j
public class TimeoutServiceZkStoreTest extends TimeoutServiceTest {
    @Override
    SegmentHelper getSegmentHelper() {
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

        return SegmentHelperMock.getSegmentHelperMock();
    }

    @Override
    protected StreamMetadataStore getStore() {
        return StreamStoreFactory.createZKStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
    }

    @Override
    Version getVersion(int i) {
        return Version.IntVersion.builder().intValue(i).build();
    }

    @Override
    Version getNextVersion(Version version) {
        return Version.IntVersion.builder().intValue(version.asIntVersion().getIntValue() + 1).build();
    }
}