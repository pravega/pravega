/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.Serializer;
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
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(config);
        Controller controller = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(config).build(), connectionFactory.getInternalExecutor());
        return new KeyValueTableFactoryImpl(scope, controller, connectionFactory);
    }

    /**
     * Creates a new {@link KeyValueTable} that provides access to a Pravega Key-Value Table.
     *
     * @param keyValueTableName   Name of the {@link KeyValueTable}.
     * @param keySerializer       A {@link Serializer} for {@link KeyValueTable} Keys. Refer to the {@link KeyValueTable}
     *                            Javadoc for constraints relating to the size of the serialization.
     * @param valueSerializer     A {@link Serializer} for {@link KeyValueTable} Values. Refer to the {@link KeyValueTable}
     *                            Javadoc for constraints relating to the size of the serialization.
     * @param clientConfiguration A {@link KeyValueTableClientConfiguration} to use for configuring the
     *                            {@link KeyValueTable} client.
     * @param <KeyT>              Key Type.
     * @param <ValueT>            Value Type.
     * @return A {@link KeyValueTable} that provides access to the requested Key-Value Table.
     */
    <KeyT, ValueT> KeyValueTable<KeyT, ValueT> forKeyValueTable(
            @NonNull String keyValueTableName, @NonNull Serializer<KeyT> keySerializer,
            @NonNull Serializer<ValueT> valueSerializer, @NonNull KeyValueTableClientConfiguration clientConfiguration);

    /**
     * Closes the {@link KeyValueTableFactory}. This will close any connections created through it.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
