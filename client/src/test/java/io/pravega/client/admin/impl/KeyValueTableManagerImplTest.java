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
package io.pravega.client.admin.impl;

import com.google.common.collect.Streams;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link KeyValueTableManagerImpl} class.
 */
public class KeyValueTableManagerImplTest {
    private static final PravegaNodeUri SERVER_LOCATION = new PravegaNodeUri("localhost", 1234);
    private static final String DEFAULT_SCOPE = "DefaultScope";
    private static final KeyValueTableConfiguration DEFAULT_CONFIG = KeyValueTableConfiguration.builder()
            .partitionCount(4)
            .primaryKeyLength(8)
            .secondaryKeyLength(4)
            .build();
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(30);
    private MockConnectionFactoryImpl connectionFactory;
    private Controller controller = null;
    private final Set<String> segments = Collections.synchronizedSet(new HashSet<>());

    @Before
    public void setUp() throws ConnectionFailedException {
        this.connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        Mockito.doAnswer(invocation -> {
            WireCommands.CreateTableSegment request = invocation.getArgument(0);
            if (segments.add(request.getSegment())) {
                this.connectionFactory.getProcessor(SERVER_LOCATION).process(
                        new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
            } else {
                this.connectionFactory.getProcessor(SERVER_LOCATION).process(
                        new WireCommands.SegmentAlreadyExists(request.getRequestId(), request.getSegment(), ""));
            }
            return null;
        }).when(connection).send(Mockito.any(WireCommands.CreateTableSegment.class));

        Mockito.doAnswer(invocation -> {
            WireCommands.DeleteTableSegment request = invocation.getArgument(0);
            if (segments.remove(request.getSegment())) {
                this.connectionFactory.getProcessor(SERVER_LOCATION).process(
                        new WireCommands.SegmentDeleted(request.getRequestId(), request.getSegment()));
            } else {
                this.connectionFactory.getProcessor(SERVER_LOCATION).process(
                        new WireCommands.NoSuchSegment(request.getRequestId(), request.getSegment(), "", 0L));
            }
            return null;
        }).when(connection).send(Mockito.any(WireCommands.DeleteTableSegment.class));

        this.connectionFactory.provideConnection(SERVER_LOCATION, connection);
        this.controller = new MockController(SERVER_LOCATION.getEndpoint(), SERVER_LOCATION.getPort(), this.connectionFactory, true);
        this.controller.createScope(DEFAULT_SCOPE).join();
    }

    @After
    public void tearDown() {
        if (this.controller != null) {
            this.controller.close();
        }

        if (this.connectionFactory != null) {
            this.connectionFactory.close();
        }
    }

    /**
     * Tests the following methods:
     * - {@link KeyValueTableManagerImpl#createKeyValueTable}
     * - {@link KeyValueTableManagerImpl#deleteKeyValueTable}
     */
    @Test
    public void testCreateDeleteUpdate() {
        @Cleanup
        val manager = new KeyValueTableManagerImpl(this.controller, this.connectionFactory);
        final String name = "KeyValueTable";
        AssertExtensions.assertThrows(
                "Bad name.",
                () -> manager.createKeyValueTable(DEFAULT_SCOPE, "123%$", DEFAULT_CONFIG),
                ex -> ex instanceof IllegalArgumentException);

        Assert.assertFalse(manager.deleteKeyValueTable(DEFAULT_SCOPE, name));

        Assert.assertTrue(manager.createKeyValueTable(DEFAULT_SCOPE, name, DEFAULT_CONFIG));
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), this.segments.size());
        Assert.assertFalse(manager.createKeyValueTable(DEFAULT_SCOPE, name, DEFAULT_CONFIG));

        Assert.assertTrue(manager.deleteKeyValueTable(DEFAULT_SCOPE, name));
        Assert.assertEquals(0, this.segments.size());
        Assert.assertFalse(manager.deleteKeyValueTable(DEFAULT_SCOPE, name));
        Assert.assertTrue(manager.createKeyValueTable(DEFAULT_SCOPE, name, DEFAULT_CONFIG));
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), this.segments.size());
        manager.close(); // Closing twice to verify operation is idempotent.
    }

    /**
     * Tests the following method(s):
     * - {@link KeyValueTableManagerImpl#listKeyValueTables}
     */
    @Test
    public void testListKeyValueTables() {
        @Cleanup
        val manager = new KeyValueTableManagerImpl(this.controller, this.connectionFactory);
        final String[] names = new String[]{"kvt1", "kvt2", "kvt3", "kvt4"};
        Assert.assertFalse(manager.listKeyValueTables(DEFAULT_SCOPE).hasNext());
        for (String name : names) {
            manager.createKeyValueTable(DEFAULT_SCOPE, name, DEFAULT_CONFIG);
            manager.createKeyValueTable(DEFAULT_SCOPE, name, DEFAULT_CONFIG); // Do it twice.
        }

        val listResult = Streams.stream(manager.listKeyValueTables(DEFAULT_SCOPE))
                .map(KeyValueTableInfo::getScopedName)
                .sorted()
                .toArray(String[]::new);

        val expectedListResult = Arrays.stream(names)
                .map(name -> new KeyValueTableInfo(DEFAULT_SCOPE, name).getScopedName())
                .sorted()
                .toArray(String[]::new);
        Assert.assertArrayEquals(expectedListResult, listResult);
    }

}
