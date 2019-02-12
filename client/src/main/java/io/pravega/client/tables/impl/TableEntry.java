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

/**
 * A Table Entry with a Version.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type
 */
public interface TableEntry<KeyT, ValueT> {
    /**
     * The Key.
     */
    TableKey<KeyT> getKey();

    /**
     * The Value.
     */
    ValueT getValue();
}