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
package io.pravega.client.tables;

import lombok.Getter;

/**
 * Exception that is thrown whenever a Conditional Update to a {@link KeyValueTable} failed.
 */
public abstract class ConditionalTableUpdateException extends Exception {
    private static final long serialVersionUID = 1L;
    @Getter
    private final String tableName;

    /**
     * Creates a new instance of the {@link ConditionalTableUpdateException} class.
     *
     * @param tableName The name of the {@link KeyValueTable} for which the update failed.
     */
    public ConditionalTableUpdateException(String tableName) {
        this(tableName, String.format("Conditional update failed for %s.", tableName));
    }

    /**
     * Creates a new instance of the {@link ConditionalTableUpdateException} class.
     *
     * @param tableName The name of the {@link KeyValueTable} for which the update failed.
     * @param message   Error message.
     */
    public ConditionalTableUpdateException(String tableName, String message) {
        super(message);
        this.tableName = tableName;
    }
}
