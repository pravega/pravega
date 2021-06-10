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
package io.pravega.client.admin;

import com.google.common.annotations.Beta;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.impl.KeyValueTableManagerImpl;
import io.pravega.client.tables.KeyValueTableConfiguration;
import java.net.URI;
import java.util.Iterator;

/**
 * Used to create, delete and manage Key-Value Tables.
 */
@Beta
public interface KeyValueTableManager extends AutoCloseable {
    /**
     * Creates a new instance of {@link KeyValueTableManager}.
     *
     * @param controller The Controller URI.
     * @return An Instance of {@link KeyValueTableManager} implementation.
     */
    static KeyValueTableManager create(URI controller) {
        return create(ClientConfig.builder().controllerURI(controller).build());
    }

    /**
     * Creates a new instance of {@link KeyValueTableManager}.
     *
     * @param config Configuration for the client connection to Pravega.
     * @return An Instance of {@link KeyValueTableManager} implementation.
     */
    static KeyValueTableManager create(ClientConfig config) {
        return new KeyValueTableManagerImpl(config);
    }

    /**
     * Creates a new Key-Value Table.
     * <p>
     * Note: This method is idempotent assuming called with the same name and config. This method may block.
     *
     * @param scopeName         The name of the scope to create this Key-Value Table in.
     * @param keyValueTableName The name of the Key-Value Table to be created.
     * @param config            The configuration the Key-Value Table should use.
     * @return True if the Key-Value Table is created
     */
    boolean createKeyValueTable(String scopeName, String keyValueTableName, KeyValueTableConfiguration config);
    
    /**
     * Deletes the provided Key-Value Table. No more updates, removals or queries may be performed.
     * Resources used by the Key-Value Table will be freed.
     *
     * @param scopeName         The name of the scope of the Key-Value Table to delete.
     * @param keyValueTableName The name of the Key-Value Table to be deleted.
     * @return True if Key-Value Table is deleted.
     */
    boolean deleteKeyValueTable(String scopeName, String keyValueTableName);

    /**
     * Gets an iterator for all Key-Value Table in the given scope.
     *
     * @param scopeName The name of the scope for which to list Key-Value Tables in.
     * @return An Iterator of {@link KeyValueTableInfo} that can be used to iterate through all Key-Value Tables in the
     * Scope.
     */
    Iterator<KeyValueTableInfo> listKeyValueTables(String scopeName);

    /**
     * Closes the {@link KeyValueTableManager}.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
