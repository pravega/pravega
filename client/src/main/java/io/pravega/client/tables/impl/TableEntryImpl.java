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

import lombok.Data;

@Data
public class TableEntryImpl<KeyT, ValueT> implements TableEntry<KeyT, ValueT> {
    private final TableKey<KeyT> key;
    private final ValueT value;
}
