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
 * A {@link KeyValueTable} Key with a {@link KeyVersion}.
 *
 * @param <KeyT> Type of the Key.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public class TableKey<KeyT> {
    /**
     * The Key.
     */
    @NonNull
    private final KeyT key;

    /**
     * The Version. If null, any updates for this Key will be unconditional. See {@link KeyValueTable} for details on
     * conditional updates.
     */
    private final KeyVersion version;

    public static <KeyT> TableKey<KeyT> unversioned(KeyT key) {
        return versioned(key, KeyVersion.NO_VERSION);
    }

    public static <KeyT> TableKey<KeyT> notExists(KeyT key) {
        return versioned(key, KeyVersion.NOT_EXISTS);
    }

    public static <KeyT> TableKey<KeyT> versioned(KeyT key, KeyVersion version) {
        return new TableKey<>(key, version);
    }
}
