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
package io.pravega.client;

import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import lombok.NonNull;

/**
 * Creates instances of {@link KeyValueTable}.
 */
public interface KeyValueTableFactory extends AutoCloseable {

    /**
     * Creates a new instance of {@link KeyValueTableFactory}.
     *
     * @param scope  The Key-Value Table scope.
     * @param config Configuration for the client.
     * @return Instance of {@link KeyValueTableFactory} implementation.
     */
    static KeyValueTableFactory withScope(String scope, ClientConfig config) {
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(config);
        ConnectionPool connectionPool = new ConnectionPoolImpl(config, connectionFactory);
        Controller controller = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(config).build(), connectionFactory.getInternalExecutor());
        return new KeyValueTableFactoryImpl(scope, controller, connectionPool);
    }

    /**
     * Creates a new {@link KeyValueTable} that provides access to a Pravega Key-Value Table.
     *
     * @param keyValueTableName   Name of the {@link KeyValueTable}.
     * @param clientConfiguration A {@link KeyValueTableClientConfiguration} to use for configuring the
     *                            {@link KeyValueTable} client.
     * @return A {@link KeyValueTable} that provides access to the requested Key-Value Table.
     */
    KeyValueTable forKeyValueTable(
            @NonNull String keyValueTableName, @NonNull KeyValueTableClientConfiguration clientConfiguration);

    /**
     * Closes the {@link KeyValueTableFactory}. This will close any connections created through it.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
