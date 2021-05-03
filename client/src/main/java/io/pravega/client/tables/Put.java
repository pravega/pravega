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
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;

/**
 * Updates the Value associated with a {@link TableKey} in a {@link KeyValueTable}.
 */
@ToString
public final class Put extends TableEntryUpdate {
    @Getter
    private final Version version;

    /**
     * Creates a new instance of the {@link Put} class for an unconditional update.
     *
     * @param key   The {@link TableKey} to update.
     * @param value The Value to associate with {@link TableKey}.
     */
    public Put(TableKey key, ByteBuffer value) {
        this(key, value, null);
    }

    /**
     * Creates a new instance of the {@link Put} class for a conditional update.
     *
     * @param key     The {@link TableKey} to update.
     * @param value   The Value to associate with {@link TableKey}.
     * @param version (Optional) The {@link Version} to condition the update on. This can be retrieved from
     *                {@link TableEntry#getVersion()} or it may be {@link Version#NOT_EXISTS} or {@link Version#NO_VERSION}.
     *                If {@code null}, this will be substituted with {@link Version#NO_VERSION}, which makes this
     *                operation an Unconditional Update.
     */
    public Put(TableKey key, ByteBuffer value, @Nullable Version version) {
        super(key, value);
        this.version = version == null ? Version.NO_VERSION : version;
    }
}
