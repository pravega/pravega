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
import lombok.ToString;

/**
 * Represents an Insertion of a {@link TableKey} (with a value) into a {@link KeyValueTable}.
 */
@Getter
@ToString
public final class Insert extends TableEntryUpdate {
    /**
     * Creates a new instance of the {@link Insert} class.
     *
     * @param key   The {@link TableKey} to insert.
     * @param value The Value to associate with {@code key}.
     */
    public Insert(TableKey key, ByteBuffer value) {
        super(key, value);
    }

    @Override
    public Version getVersion() {
        return Version.NOT_EXISTS;
    }
}
