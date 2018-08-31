/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin;

import com.google.common.annotations.Beta;
import io.pravega.client.ClientConfig;
import io.pravega.client.tables.TableConfiguration;
import java.net.URI;

/**
 * Used to create and manage Tables.
 */
@Beta
public interface TableManager extends AutoCloseable {
    /**
     * Creates a new instance of TableManager.
     *
     * @param scope         The Scope string.
     * @param controllerUri The Controller URI.
     * @return Instance of TableManager implementation.
     */
    static TableManager create(String scope, URI controllerUri) {
        return create(scope, ClientConfig.builder().controllerURI(controllerUri).build());
    }

    /**
     * Creates a new instance of TableManager.
     *
     * @param scope        The Scope string.
     * @param clientConfig Configuration for the client.
     * @return Instance of Stream Manager implementation.
     */
    static TableManager create(String scope, ClientConfig clientConfig) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Creates a new Table.
     *
     * Note: This method is idempotent assuming called with the same scope, name and config. This method may block.
     *
     * @param scopeName The name of the scope to create this Table in.
     * @param tableName The name of the Table to be created.
     * @param config    The {@link TableConfiguration} the Table should use.
     * @return True if stream is created.
     */
    boolean createTable(String scopeName, String tableName, TableConfiguration config);

    /**
     * Changes the configuration for an existing Table.
     *
     * Note: This method is idempotent assuming called with the same scope, name and config. This method may block.
     *
     * @param scopeName The Table's Scope.
     * @param tableName The Table's Name.
     * @param config    The {@link TableConfiguration} to update.
     * @return True if Table is created.
     */
    boolean updateTable(String scopeName, String tableName, TableConfiguration config);

    /**
     * Seals an existing Table.
     *
     * @param scopeName The Table's Scope.
     * @param tableName The Table's Name.
     * @return True if the Table is sealed.
     */
    boolean sealTable(String scopeName, String tableName);

    /**
     * Deletes an existing Table. No more operations may be performed on it. Resources used by this Table will be freed.
     *
     * @param scopeName The Table's Scope.
     * @param tableName The Table's Name.
     * @return True if the Table is deleted.
     */
    boolean deleteTable(String scopeName, String tableName);

    /**
     * Deletes an existing Table, but only if it is empty.
     *
     * @param scopeName The Table's Scope.
     * @param tableName The Table's Name.
     * @return True if the Table is deleted.
     */
    boolean deleteTableIfEmpty(String scopeName, String tableName);


    @Override
    void close();
}
