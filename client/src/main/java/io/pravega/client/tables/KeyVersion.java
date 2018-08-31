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

import java.io.Serializable;
import lombok.Data;

/**
 * Version of a Key in a Table.
 */
@Data
public class KeyVersion implements Serializable {
    /**
     * A special KeyVersion which indicates the Key must not exist when performing Conditional Updates.
     */
    public static final KeyVersion NOT_EXISTS = new KeyVersion(Long.MIN_VALUE, Long.MIN_VALUE);
    private static final long serialVersionUID = 1L;

    /**
     * The internal Table Segment Id where this Key Version refers to.
     */
    private final long segmentId;
    /**
     * The internal version inside the Table Segment for this Key.
     */
    private final long segmentVersion;
}
