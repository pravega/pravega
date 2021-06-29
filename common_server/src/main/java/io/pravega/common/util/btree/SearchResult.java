/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util.btree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * The result of a BTreePage or sets.BTreeSetPage Search.
 */
@Getter
@RequiredArgsConstructor
class SearchResult {
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
