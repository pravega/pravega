package io.pravega.segmentstore.server.host.handler;

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.InOrder;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

public class AdminRequestProcessorImplTest extends PravegaRequestProcessorTest {
    @Test(timeout = 20000)
    public void testFlush() throws Exception {
        @Cleanup
        ServiceBuilder serviceBuilder = newInlineExecutionInMemoryBuilder(getBuilderConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        ServerConnection connection = mock(ServerConnection.class);
        InOrder order = inOrder(connection);
        AdminRequestProcessorImpl processor = new AdminRequestProcessorImpl(store, mock(TableStore.class), connection);

        processor.flushStorage(new WireCommands.FlushStorage("", 1));
        order.verify(connection).send(new WireCommands.StorageFlushed(1));
    }
}
