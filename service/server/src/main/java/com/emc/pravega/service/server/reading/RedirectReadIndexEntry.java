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

import com.google.common.base.Preconditions;

/**
 * Read Index Entry that redirects to a different StreamSegment.
 */
class RedirectReadIndexEntry extends ReadIndexEntry {
    private final StreamSegmentReadIndex redirectReadIndex;

    /**
     * Creates a new instance of the RedirectReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param length              The length of the redirected Read Index.
     * @param redirectReadIndex   The StreamSegmentReadIndex to redirect to.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException if the offset is a negative number.
     */
    RedirectReadIndexEntry(long streamSegmentOffset, long length, StreamSegmentReadIndex redirectReadIndex) {
        super(streamSegmentOffset, length);

        Preconditions.checkNotNull(redirectReadIndex, "redirectReadIndex");
        this.redirectReadIndex = redirectReadIndex;
    }

    /**
     * Gets a reference to the StreamSegmentReadIndex that this entry redirects to.
     */
    StreamSegmentReadIndex getRedirectReadIndex() {
        return this.redirectReadIndex;
    }

    @Override
    boolean isDataEntry() {
        return false;
    }
}
