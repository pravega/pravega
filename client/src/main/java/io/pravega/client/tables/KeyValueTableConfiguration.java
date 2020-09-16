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

import com.google.common.annotations.Beta;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * The configuration of a Key-Value Table.
 */
@Beta
@Data
@Builder
public class KeyValueTableConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * The number of Partitions for a Key-Value Table. This value cannot be adjusted after the Key-Value Table has been
     * created.
     *
     * @param partitionCount The number of Partitions for a Key-Value Table.
     * @return The number of Partitions for a Key-Value Table.
     */
    private final int partitionCount;
}
