/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
     */
    @NonNull
    private final TableKey<KeyT> key;

    /**
     * The Value.
     */
    private final ValueT value;

    public static <KeyT, ValueT> TableEntry<KeyT, ValueT> unversioned(KeyT key, ValueT value) {
        return new TableEntry<>(TableKey.unversioned(key), value);
    }

    public static <KeyT, ValueT> TableEntry<KeyT, ValueT> notExists(KeyT key, ValueT value) {
        return new TableEntry<>(TableKey.notExists(key), value);
    }

    public static <KeyT, ValueT> TableEntry<KeyT, ValueT> versioned(KeyT key, KeyVersion version, ValueT value) {
        return new TableEntry<>(TableKey.versioned(key, version), value);
    }

}
