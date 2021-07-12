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

import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.shared.security.auth.AccessOperation;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Implementation for {@link KeyValueTableFactory}.
 */
@RequiredArgsConstructor
public class KeyValueTableFactoryImpl implements KeyValueTableFactory {
    @NonNull
    private final String scope;
    @NonNull
    private final Controller controller;
    @NonNull
    private final ConnectionPool connectionPool;

    @Override
    public KeyValueTable forKeyValueTable(@NonNull String keyValueTableName, @NonNull KeyValueTableClientConfiguration clientConfiguration) {
        val kvt = new KeyValueTableInfo(this.scope, keyValueTableName);
        val provider = DelegationTokenProviderFactory.create(this.controller, kvt.getScope(), kvt.getKeyValueTableName(), AccessOperation.READ_WRITE);
        val tsf = new TableSegmentFactoryImpl(this.controller, this.connectionPool, clientConfiguration, provider);
        return new KeyValueTableImpl(kvt, tsf, this.controller, this.connectionPool.getInternalExecutor());
    }

    @Override
    public void close() {
        // These two are passed in via the constructor, however they are created inside the KeyValueTableFactory.withScope,
        // which creates this instance, so we are the only ones who use it.
        this.controller.close();
        this.connectionPool.close();
    }
}
