/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.server.CacheKey;
import com.google.common.base.Preconditions;

/**
 * A ReadIndexEntry that points to data in the cache.
 */
class CacheReadIndexEntry extends ReadIndexEntry {
    //region Members

    private final CacheKey cacheKey;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param cacheKey The CacheKey for this Entry.
     * @param length   The length for this Entry.
     * @throws NullPointerException     If cacheKey is null.
     * @throws IllegalArgumentException If length or cacheKey.getOffset() are negative numbers.
     */
    CacheReadIndexEntry(CacheKey cacheKey, int length) {
        super(cacheKey.getOffset(), length);
        this.cacheKey = cacheKey;
    }

    private CacheReadIndexEntry(CacheKey cacheKey, long overriddenOffset, int length) {
        super(overriddenOffset, length);
        Preconditions.checkNotNull(cacheKey, "cacheKey");
        this.cacheKey = cacheKey;
    }

    //endregion

    //region Properties

    /**
     * Gets a pointer to the CacheKey for this entry.
     */
    CacheKey getCacheKey() {
        return this.cacheKey;
    }

    /**
     * Returns a new instance of the CacheReadIndexEntry with the same CacheKey and Length as this one, but with the offset
     * adjusted by the given amount.
     *
     * @param offsetAdjustment The amount to adjust the offset by.
     */
    CacheReadIndexEntry withAdjustedOffset(long offsetAdjustment) {
        // CacheReadIndexEntry always have length < Int32.MAX_VALUE, so it's safe to cast to int.
        return new CacheReadIndexEntry(this.cacheKey, this.getStreamSegmentOffset() + offsetAdjustment, (int) this.getLength());
    }

    //endregion
}
