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

import lombok.Getter;

/**
 * A Table Entry with a Version.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type
 */
@Getter
public class VersionedEntry<KeyT, ValueT> extends VersionedKey<KeyT> {
    /**
     * The Value.
     */
    private final ValueT value;

    VersionedEntry(KeyT key, ValueT value, KeyVersion version) {
        super(key, version);
        this.value = value;
    }
}