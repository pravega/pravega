/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import lombok.Data;

/**
 * Implementation of {@link TableKey}.
 * @param <KeyT> Key Type.
 */
@Data
public class TableKeyImpl<KeyT> implements TableKey<KeyT> {

    private final KeyT key;
    private final KeyVersion version;

}
