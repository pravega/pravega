/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
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
    public <KeyT, ValueT> KeyValueTable<KeyT, ValueT> forKeyValueTable(
            @NonNull String keyValueTableName, @NonNull Serializer<KeyT> keySerializer,
            @NonNull Serializer<ValueT> valueSerializer, @NonNull KeyValueTableClientConfiguration clientConfiguration) {
        val kvt = new KeyValueTableInfo(this.scope, keyValueTableName);
        val provider = DelegationTokenProviderFactory.create(this.controller, kvt.getScope(), kvt.getKeyValueTableName());
        val tsf = new TableSegmentFactoryImpl(this.controller, this.connectionPool, clientConfiguration, provider);
        return new KeyValueTableImpl<>(kvt, tsf, this.controller, keySerializer, valueSerializer);
    }

    @Override
    public void close() {
        // These two are passed in via the constructor, however they are created inside the KeyValueTableFactory.withScope,
        // which creates this instance, so we are the only ones who use it.
        this.controller.close();
        this.connectionPool.close();
    }
}
