/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.client.tables.KeyVersion;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A Table Key with a Version.
 *
 * @param <KeyT> Type of the Key.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class TableKey<KeyT> {
    /**
     * The Key.
     */
    private final KeyT key;

    /**
     * The Version. If null, any updates for this Key will be non-conditional (blind).
     */
    private final KeyVersion version;

    /**
     * Creates a new instance of the {@link TableKey} class with no versioning specified.
     *
     * @param key    The Key.
     * @param <KeyT> Key Type.
     * @return A new instance of the {@link TableKey} class.
     */
    static <KeyT> TableKey<KeyT> unversioned(@NonNull KeyT key) {
        return new TableKey<>(key, null);
    }

    /**
     * Creates a new instance of the {@link TableKey} class with explicit key versioning.
     *
     * @param key     The Key.
     * @param version A {@link KeyVersion} representing the Key Version.
     * @param <KeyT>  Key Type.
     * @return A new instance of the {@link TableKey} class.
     */
    static <KeyT> TableKey<KeyT> versioned(@NonNull KeyT key, @NonNull KeyVersion version) {
        return new TableKey<>(key, version);
    }
}
