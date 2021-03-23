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

import io.pravega.common.util.EnumHelpers;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines a type of update for a particular Attribute.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum AttributeUpdateType {
    /**
     * Only allows setting the value if it has not already been set. If it is set, the update will fail.
     */
    None((byte) 0),

    /**
     * Replaces the current value of the attribute with the one given. If no value is currently set, it sets the value
     * to the given one.
     */
    Replace((byte) 1),

    /**
     * Any updates will replace the current attribute value, but only if the new value is greater than the current
     * value (or no value defined currently). This does not require the updates to be consecutive. For example,
     * if A and B (A &lt; B) are updated concurrently, odds are that B will make it but A won't - this will be observed
     * by either A failing or both succeeding, but in the end, the final result will contain B.
     */
    ReplaceIfGreater((byte) 2),

    /**
     * Replaces the current value of the attribute with the sum of the current value and the one given. If no value is
     * currently set, it sets the given value as the attribute value.
     */
    Accumulate((byte) 3),

    /**
     * Any updates will replace the current attribute value, but only if the existing value matches
     * an expected value. If the value is different the update will fail. This can be used to
     * perform compare and set operations. The value {io.pravega.segmentstore.contracts.Attributes.NULL_ATTRIBUTE_VALUE}
     * is used to indicate a non value, i.e., to remove the attribute or if the value is expected not to
     * exist.
     */
    ReplaceIfEquals((byte) 4);


    private static final AttributeUpdateType[] MAPPING = EnumHelpers.indexById(AttributeUpdateType.class, AttributeUpdateType::getTypeId);
    @Getter
    private final byte typeId;

    /**
     * Gets the AttributeUpdateType that has the given type id.
     *
     * @param typeId The type id to search by.
     * @return The mapped AttributeUpdateType, or null
     */
    public static AttributeUpdateType get(byte typeId) {
        if (typeId < 0 || typeId >= MAPPING.length || MAPPING[typeId] == null) {
            throw new IllegalArgumentException("Unsupported AttributeUpdateType Id " + typeId);
        }

        return MAPPING[typeId];
    }
}