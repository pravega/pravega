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
package io.pravega.segmentstore.contracts;

import com.google.common.base.Preconditions;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents an update to a value of an Attribute.
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@NotThreadSafe
public class AttributeUpdate {
    /**
     * The ID of the Attribute to update.
     */
    private final UUID attributeId;

    /**
     * The UpdateType of the attribute.
     */
    private final AttributeUpdateType updateType;

    /**
     * The new Value of the attribute.
     */
    private long value;

    /**
     * If UpdateType is ReplaceIfEquals, then this is the value that the attribute must currently have before making the
     * update. Otherwise this field is ignored.
     */
    private final long comparisonValue;

    /**
     * Creates a new instance of the AttributeUpdate class, except for ReplaceIfEquals.
     *
     * @param attributeId A UUID representing the ID of the attribute to update.
     * @param updateType  The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param value       The new value to set.
     */
    public AttributeUpdate(UUID attributeId, AttributeUpdateType updateType, long value) {
        this(attributeId, updateType, value, Long.MIN_VALUE);
        Preconditions.checkArgument(updateType != AttributeUpdateType.ReplaceIfEquals,
                "Cannot use this constructor with ReplaceIfEquals.");
    }

    @Override
    public String toString() {
        return String.format("AttributeId = %s, Value = %s, UpdateType = %s", this.attributeId, this.value, this.updateType);
    }
}
