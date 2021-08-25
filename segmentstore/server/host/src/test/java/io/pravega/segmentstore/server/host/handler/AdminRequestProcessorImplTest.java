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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.shared.protocol.netty.AdminRequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.SerializedClassRunner;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@Slf4j
@RunWith(SerializedClassRunner.class)
public class AdminRequestProcessorImplTest extends PravegaRequestProcessorTest {

    @Test(timeout = 60000)
    public void testFlushToStorage() throws Exception {
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = spy(serviceBuilder.createStreamSegmentService());
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        AdminRequestProcessor processor = new AdminRequestProcessorImpl(store, mock(TableStore.class), connection);

        processor.flushToStorage(new WireCommands.FlushToStorage(0, "", 1));
        order.verify(connection).send(new WireCommands.StorageFlushed(1));
    }
}
