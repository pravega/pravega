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

import lombok.Builder;
import lombok.Data;

/**
 * The result of a Get operation from a Table.
 * @param <ValueT> Type of the Value.
 */
@Data
@Builder
public class GetResult<ValueT> {
    /**
     * The version corresponding to this Value.
     */
    private final KeyVersion keyVersion;

    /**
     * Deserialized value.
     */
    private final ValueT value;
}
