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
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.tables.impl.KeyValueTableTestBase;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Integration test for {@link KeyValueTable}s using real Segment Store, Controller and connection.
 *
 */
@Slf4j
public class KeyValueTableTest extends KeyValueTableTestBase {
    private static final String ENDPOINT = "localhost";
    private static final String SCOPE = "Scope";
    private static final KeyValueTableConfiguration DEFAULT_CONFIG = KeyValueTableConfiguration.builder()
            .partitionCount(5)
            .primaryKeyLength(Long.BYTES)
            .secondaryKeyLength(Integer.BYTES)
            .build();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private ServiceBuilder serviceBuilder;
    private TableStore tableStore;
    private PravegaConnectionListener serverListener = null;
    private ConnectionPool connectionPool;
    private TestingServer zkTestServer = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller;
    private KeyValueTableFactory keyValueTableFactory;
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = ENDPOINT;
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        this.tableStore = serviceBuilder.createTableStoreService();

        this.serverListener = new PravegaConnectionListener(false, servicePort, serviceBuilder.createStreamSegmentService(), this.tableStore,
                executorService(), new IndexAppendProcessor(executorService(), serviceBuilder.createStreamSegmentService()));
        this.serverListener.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();

        // 4. Create Scope
        this.controller.createScope(SCOPE).get();
        ClientConfig clientConfig = ClientConfig.builder().build();
        SocketConnectionFactoryImpl connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        this.connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        this.keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE, this.controller, this.connectionPool);
    }


    @After
    public void tearDown() throws Exception {
        this.controller.close();
        this.connectionPool.close();
        this.controllerWrapper.close();
        this.serverListener.close();
        this.serviceBuilder.close();
        this.zkTestServer.close();
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and listed.
     */
    @Test
    public void testCreateListKeyValueTable() {
        val kvt1 = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);

        val segments = this.controller.getCurrentSegmentsForKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), segments.getSegments().size());

        for (val s : segments.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[DEFAULT_CONFIG.getTotalKeyLength()])), TIMEOUT).join();
        }

        // Verify re-creation does not work.
        Assert.assertFalse(this.controller.createKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName(), DEFAULT_CONFIG).join());

        // Try to create a KVTable with 0 partitions, and it should fail
        val kvtZero = newKeyValueTableName();
        assertThrows(IllegalArgumentException.class, () -> this.controller.createKeyValueTable(kvtZero.getScope(), kvtZero.getKeyValueTableName(),
                KeyValueTableConfiguration.builder().partitionCount(0).build()).join());

        // Create 2 more KVTables
        val kvt2 = newKeyValueTableName();
        created = this.controller.createKeyValueTable(kvt2.getScope(), kvt2.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);

        val kvt3 = newKeyValueTableName();
        created = this.controller.createKeyValueTable(kvt3.getScope(), kvt3.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);

        // Check list tables...
        AsyncIterator<KeyValueTableInfo> kvTablesIterator =  this.controller.listKeyValueTables(SCOPE);
        Iterator<KeyValueTableInfo> iter = kvTablesIterator.asIterator();
        Map<String, Integer> countMap = new HashMap<String, Integer>(3);
        while (iter.hasNext()) {
            KeyValueTableInfo kvtInfo = iter.next();
            if (kvtInfo.getScope().equals(SCOPE)) {
                if (countMap.containsKey(kvtInfo.getKeyValueTableName())) {
                    Integer newCount = Integer.valueOf(countMap.get(kvtInfo.getKeyValueTableName()).intValue() + 1);
                    countMap.put(iter.next().getKeyValueTableName(), newCount);
                } else {
                    countMap.put(kvtInfo.getKeyValueTableName(), 1);
                }
            }
        }
        Assert.assertEquals(3, countMap.size());
        Assert.assertEquals(1, countMap.get(kvt1.getKeyValueTableName()).intValue());
        Assert.assertEquals(1, countMap.get(kvt2.getKeyValueTableName()).intValue());
        Assert.assertEquals(1, countMap.get(kvt3.getKeyValueTableName()).intValue());
    }

    /**
     * Smoke Test. Verify that the KeyValueTable can be created and deleted.
     */
    @Test
    public void testCreateDeleteKeyValueTable() {
        val kvt1 = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);
        val segments = this.controller.getCurrentSegmentsForKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertEquals(DEFAULT_CONFIG.getPartitionCount(), segments.getSegments().size());

        for (val s : segments.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            log.info("Segment Number {}", s.getSegmentId());
            this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[DEFAULT_CONFIG.getTotalKeyLength()])), TIMEOUT).join();
        }

        // Delete and verify segments have been deleted too.
        val deleted = this.controller.deleteKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertTrue(deleted);

        Assert.assertFalse(this.controller.deleteKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join());
        for (val s : segments.getSegments()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "Segment " + s + " has not been deleted.",
                    () -> this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[DEFAULT_CONFIG.getTotalKeyLength()])), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }

    }

    @Override
    protected KeyValueTable createKeyValueTable() {
        val kvt = newKeyValueTableName();
        return createKeyValueTable(kvt, DEFAULT_CONFIG);
    }

    @Override
    protected KeyValueTable createKeyValueTable(KeyValueTableInfo kvt, KeyValueTableConfiguration configuration) {
        boolean created = this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), configuration).join();
        Assert.assertTrue(created);
        return this.keyValueTableFactory.forKeyValueTable(kvt.getKeyValueTableName(), KeyValueTableClientConfiguration.builder().build());
    }

    private KeyValueTableInfo newKeyValueTableName() {
        return new KeyValueTableInfo(SCOPE, String.format("KVT-%d", System.nanoTime()));
    }

}
