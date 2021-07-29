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

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;

/**
 * Removes a {@link TableKey} from a {@link KeyValueTable}.
 */
@Getter
@ToString
public final class Remove extends TableModification {
    private final Version version;

    /**
     * Creates a new instance of the {@link Remove} class for an unconditional removal.
     *
     * @param key The {@link TableKey} to remove.
     */
    public Remove(TableKey key) {
        this(key, null);
    }

    /**
     * Creates a new instance of the {@link Remove} class for a conditional removal.
     *
     * @param key     The {@link TableKey} to remove.
     * @param version Optional) The {@link Version} to condition the removal on. This can be retrieved from
     *                {@link TableEntry#getVersion()} or it may be {@link Version#NOT_EXISTS} or {@link Version#NO_VERSION}.
     *                If {@code null}, this will be substituted with {@link Version#NO_VERSION}, which makes this
     *                operation an Unconditional Update.
     */
    public Remove(TableKey key, @Nullable Version version) {
        super(key);
        this.version = version == null ? Version.NO_VERSION : version;
    }

    @Override
    public boolean isRemoval() {
        return true;
    }
}
