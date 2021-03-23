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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * A {@link KeyValueTable} Entry.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public class TableEntry<KeyT, ValueT> {
    /**
     * The {@link TableKey}.
     *
     * @return The content of {@link TableKey}.
     */
    @NonNull
    private final TableKey<KeyT> key;

    /**
     * The Value.
     *
     * @return The value associated to the {@link TableKey}.
     */
    private final ValueT value;

    /**
     * Creates a new {@link TableEntry} with no specific version. When used with {@link KeyValueTable#replaceAll}, this
     * {@link TableEntry} will be treated as an unconditional update.
     *
     * @param key      The Entry Key.
     * @param value    The Entry Value.
     * @param <KeyT>   Key Type.
     * @param <ValueT> Value Type.
     * @return An unversioned {@link TableEntry} (version set to {@link Version#NO_VERSION}).
     */
    public static <KeyT, ValueT> TableEntry<KeyT, ValueT> unversioned(KeyT key, ValueT value) {
        return new TableEntry<>(TableKey.unversioned(key), value);
    }

    /**
     * Creates a new {@link TableEntry} with a version that indicates the key must not exist. When used with
     * {@link KeyValueTable#replaceAll}, this {@link TableEntry} will be treated as a conditional update, conditioned
     * on the Key not existing.
     *
     * @param key      The Entry Key.
     * @param value    The Entry Value.
     * @param <KeyT>   Key Type.
     * @param <ValueT> Value Type.
     * @return A {@link TableEntry} with a version set to {@link Version#NOT_EXISTS}.
     */
    public static <KeyT, ValueT> TableEntry<KeyT, ValueT> notExists(KeyT key, ValueT value) {
        return new TableEntry<>(TableKey.notExists(key), value);
    }

    /**
     * Creates a new {@link TableEntry} with a specific key version.. When used with {@link KeyValueTable#replaceAll},
     * this {@link TableEntry} will be treated as a conditional update, conditioned on the Key existing and having the
     * specified version.
     *
     * @param key      The Entry Key.
     * @param value    The Entry Value.
     * @param version  The Version to use.
     * @param <KeyT>   Key Type.
     * @param <ValueT> Value Type.
     * @return A {@link TableEntry}.
     */
    public static <KeyT, ValueT> TableEntry<KeyT, ValueT> versioned(KeyT key, Version version, ValueT value) {
        return new TableEntry<>(TableKey.versioned(key, version), value);
    }

}
