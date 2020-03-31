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

import lombok.Data;
import lombok.NonNull;

/**
 * A {@link KeyValueTable} Entry.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type
 */
@Data
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
}
