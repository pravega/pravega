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

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Version of a Key in a Table.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class KeyVersion implements Serializable {
    /**
     * The Segment where this Key resides.
     */
    private final String segmentName;
    /**
     * The internal version inside the Table Segment for this Key.
     */
    private final long segmentVersion;
}
