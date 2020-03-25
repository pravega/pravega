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

    /**
     * Creates a new {@link TableKey} with no specific version. When used with {@link KeyValueTable#removeAll}, this
     * {@link TableKey} will be treated as an unconditional removal.
     *
     * @param key    The Key.
     * @param <KeyT> Key Type.
     * @return An unversioned {@link TableKey} (version set to {@link KeyVersion#NO_VERSION}).
     */
    public static <KeyT> TableKey<KeyT> unversioned(KeyT key) {
        return versioned(key, KeyVersion.NO_VERSION);
    }

    /**
     * Creates a new {@link TableKey} with a version that indicates the key must not exist. When used with
     * {@link KeyValueTable#removeAll}, this {@link TableKey} will be treated conditional removal.
     * <p>
     * By itself, this is not a useful scenario (removing a key conditioned on it not existing in the first place doesn't
     * make much sense). However, when used in combination with other removals ({@link KeyValueTable#removeAll} accepts
     * multiple {@link TableKey}s), this can be used to condition the entire batch on a particular {@link TableKey}'s
     * inexistence (i.e., only perform these removals iff a certain {@link TableKey} is not present).
     *
     * @param key    The Key.
     * @param <KeyT> Key Type
     * @return A {@link TableKey} with a version set to {@link KeyVersion#NOT_EXISTS}.
     */
    public static <KeyT> TableKey<KeyT> notExists(KeyT key) {
        return versioned(key, KeyVersion.NOT_EXISTS);
    }

    /**
     * Creates a new {@link TableKey} with a specific version. When used with {@link KeyValueTable#removeAll}, this
     * {@link TableKey} will be treated as a conditional removal, conditioned on the Key existing and having the specified
     * version.
     *
     * @param key     The Key.
     * @param version The Version.
     * @param <KeyT>  Key Type.
     * @return A {@link TableKey}.
     */
    public static <KeyT> TableKey<KeyT> versioned(KeyT key, KeyVersion version) {
        return new TableKey<>(key, version);
    }
}
