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
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A {@link KeyValueTable} Key.
 */
@Data
@RequiredArgsConstructor
public final class TableKey {
    /**
     * The Primary Key.
     *
     * @param primaryKey Primary Key.
     * @return Primary Key.
     */
    @NonNull
    private final ByteBuffer primaryKey;

    /**
     * The Secondary Key (Optional).
     *
     * @param version Secondary Key.
     * @return Secondary Key.
     */
    private final ByteBuffer secondaryKey;

    public TableKey(ByteBuffer primaryKey) {
        this(primaryKey, null);
    }
}
