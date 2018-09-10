/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A Table Entry with a Version.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TableEntry<KeyT, ValueT> {
    /**
     * The Key.
     */
    private final TableKey<KeyT> key;
    /**
     * The Value.
     */
    private final ValueT value;

    /**
     * Creates a new instance of the {@link TableEntry} class with no versioning specified.
     *
     * @param key      The Key.
     * @param value    The Value
     * @param <KeyT>   Key Type.
     * @param <ValueT> Value Type.
     * @return A new instance of the {@link TableEntry} class.
     */
    static <KeyT, ValueT> TableEntry<KeyT, ValueT> unversioned(@NonNull KeyT key, @NonNull ValueT value) {
        return new TableEntry<>(TableKey.unversioned(key), value);
    }

    /**
     * Creates a new instance of the {@link TableEntry} class with explicit key versioning.
     *
     * @param key      The Key.
     * @param version  A {@link KeyVersion} representing the Key Version.
     * @param value    The Value
     * @param <KeyT>   Key Type.
     * @param <ValueT> Value Type.
     * @return A new instance of the {@link TableEntry} class.
     */
    static <KeyT, ValueT> TableEntry<KeyT, ValueT> versioned(@NonNull KeyT key, @NonNull KeyVersion version, @NonNull ValueT value) {
        return new TableEntry<>(TableKey.versioned(key, version), value);
    }
}