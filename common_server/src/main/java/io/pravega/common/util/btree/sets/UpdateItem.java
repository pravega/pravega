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
package io.pravega.common.util.btree.sets;

import io.pravega.common.util.ArrayView;
import lombok.Data;
import lombok.NonNull;

/**
 * An item to be updated.
 */
@Data
class UpdateItem implements Comparable<UpdateItem> {
    /**
     * The item.
     */
    @NonNull
    private final ArrayView item;
    /**
     * True if this item is to be removed, false if it is to be inserted.
     */
    private final boolean removal;

    @Override
    public int compareTo(UpdateItem other) {
        return BTreeSet.COMPARATOR.compare(this.item, other.item);
    }
}
