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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Defines a modification that can be applied to a {@link TableKey} in a {@link KeyValueTable}.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public abstract class TableModification {
    /**
     * The {@link TableKey} affected.
     *
     * @return The {@link TableKey} affected.
     */
    @NonNull
    private final TableKey key;

    /**
     * Gets a value indicating whether this modification attempts to remove the {@link TableKey} or not.
     * @return True if removal, false otherwise.
     */
    public abstract boolean isRemoval();

    /**
     * Gets a {@link Version} that may condition the modification. See {@link KeyValueTable} for details on conditional
     * updates.
     * @return The {@link Version}.
     */
    @Nullable
    public abstract Version getVersion();
}
