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

import java.nio.ByteBuffer;
import lombok.Getter;
import lombok.NonNull;

/**
 * Defines a {@link TableModification} that updates a {@link TableKey} with a new value.
 */
public abstract class TableEntryUpdate extends TableModification {
    /**
     * The new value to associate the {@link TableKey} with.
     *
     * @return The new value to associate the {@link TableKey} with.
     */
    @Getter
    private final ByteBuffer value;

    /**
     * Creates a new instance of the {@link TableEntryUpdate} class.
     *
     * @param key   The {@link TableKey}.
     * @param value The Value to associate with the {@link TableKey}.
     */
    TableEntryUpdate(TableKey key, @NonNull ByteBuffer value) {
        super(key);
        this.value = value;
    }

    @Override
    public boolean isRemoval() {
        return false;
    }
}
