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
package io.pravega.client.tables.impl;

import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Unit tests for the {@link KeyValueTableImpl} class. This uses mocked {@link TableSegment}s so it does not actually
 * verify over-the-wire commands. Integration tests (`io.pravega.test.integration.KeyValueTableImplTests`) cover end-to-end
 * scenarios instead.
 */
public class KeyValueTableImplTests extends KeyValueTableTestBase {
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo("Scope", "KVT");
    private MockConnectionFactoryImpl connectionFactory;
    private MockController controller;
    private KeyValueTableConfiguration defaultConfig;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Override
    protected KeyValueTable createKeyValueTable() {
        return createKeyValueTable(KVT, this.defaultConfig);
    }

    @Override
    protected KeyValueTable createKeyValueTable(KeyValueTableInfo kvt, KeyValueTableConfiguration config) {
        this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), config);
        int segmentKeyLength = config.getTotalKeyLength();
        val segmentFactory = new MockTableSegmentFactory(getSegmentCount(), segmentKeyLength, executorService());
        return new KeyValueTableImpl(kvt, segmentFactory, this.controller, executorService());
    }

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.connectionFactory = new MockConnectionFactoryImpl();
        this.controller = new MockController("localhost", 0, this.connectionFactory, false);
        boolean isScopeCreated = this.controller.createScope(KVT.getScope()).get();
        Assert.assertTrue(isScopeCreated);
        this.defaultConfig = KeyValueTableConfiguration.builder()
                .partitionCount(getSegmentCount())
                .primaryKeyLength(getPrimaryKeyLength())
                .secondaryKeyLength(getSecondaryKeyLength())
                .build();
    }

    @After
    public void tearDown() {
        this.controller.close();
        this.connectionFactory.close();
    }
}
