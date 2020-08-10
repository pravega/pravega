/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin.impl;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BlockingAsyncIterator;
import io.pravega.shared.NameUtils;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * An implementation of {@link KeyValueTableManager}. Used to bootstrap the client.
 */
@Beta
@Slf4j
public class KeyValueTableManagerImpl implements KeyValueTableManager {
    //region Members

    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    //endregion

    //region Constructors

    /**
     * Creates a new instance of the {@link KeyValueTableManager} class.
     *
     * @param clientConfig A {@link ClientConfig} that can be used to configure the connection to Pravega.
     */
    public KeyValueTableManagerImpl(@NonNull ClientConfig clientConfig) {
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "KeyValueTableManagerImpl-Controller");
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), this.executor);
        this.connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
    }

    /**
     * Creates a new instance of the {@link KeyValueTableManager} class.
     *
     * @param controller        The {@link Controller} to use.
     * @param connectionFactory The {@link ConnectionFactory} to use.
     */
    @VisibleForTesting
    KeyValueTableManagerImpl(@NonNull Controller controller, @NonNull ConnectionFactory connectionFactory) {
        this.executor = null;
        this.controller = controller;
        this.connectionFactory = connectionFactory;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            if (this.controller != null) {
                this.controller.close();
            }

            if (this.connectionFactory != null) {
                this.connectionFactory.close();
            }

            if (this.executor != null) {
                ExecutorServiceHelpers.shutdown(this.executor);
            }
        }
    }

    //endregion

    //region KeyValueTableManager Implementation

    @Override
    public boolean createKeyValueTable(@NonNull String scopeName, @NonNull String keyValueTableName, @NonNull KeyValueTableConfiguration config) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        NameUtils.validateUserKeyValueTableName(keyValueTableName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Creating scope/key-value-table: {}/{} with configuration: {}", scopeName, keyValueTableName, config);
        return Futures.getThrowingException(this.controller.createKeyValueTable(
                scopeName, keyValueTableName, config));
    }

    @Override
    public boolean deleteKeyValueTable(@NonNull String scopeName, @NonNull String keyValueTableName) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        NameUtils.validateUserKeyValueTableName(keyValueTableName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Deleting scope/key-value-table: {}/{}", scopeName, keyValueTableName);
        return Futures.getThrowingException(controller.deleteKeyValueTable(scopeName, keyValueTableName));
    }

    @Override
    public Iterator<KeyValueTableInfo> listKeyValueTables(@NonNull String scopeName) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Listing key-value-tables in scope: {}", scopeName);
        AsyncIterator<KeyValueTableInfo> asyncIterator = controller.listKeyValueTables(scopeName);
        return new BlockingAsyncIterator<>(asyncIterator);
    }

    //endregion
}
