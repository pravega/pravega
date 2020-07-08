/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * The result of a BTreePage or sets.BTreeSetPage Search.
 */
@Getter
@RequiredArgsConstructor
public class SearchResult {
    /**
     * The resulting position.
     */
    private final int position;
    /**
     * Indicates whether an exact match for the sought key was found. If so, position refers to that key. If not,
     * position refers to the location where the key would have been.
     */
    private final boolean exactMatch;

    @Override
    public String toString() {
        return String.format("%s (%s)", this.position, this.exactMatch ? "E" : "NE");
    }
}
