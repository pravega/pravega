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
import javax.annotation.concurrent.NotThreadSafe;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * An {@link AttributeUpdate} that will set the value of an Attribute based on a reference to another Segment property.
 * The actual value associated with the {@link AttributeUpdate#getAttributeId()} will be determined at time of processing.
 */
@NotThreadSafe
@EqualsAndHashCode(callSuper = true)
public class DynamicAttributeUpdate extends AttributeUpdate {
    //region Members

    /**
     * True if the value has been calculated and set, false otherwise.
     * If true, then the value should not be re-evaluated again.
     */
    private boolean valueSet;
    /**
     * The {@link DynamicAttributeValue} to evaluate.
     */
    @Getter
    private final DynamicAttributeValue valueReference;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AttributeUpdateByReference class, except for ReplaceIfEquals.
     *
     * @param attributeId    An {@link AttributeId} representing the ID of the attribute to update.
     * @param updateType     The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param valueReference The AttributeValueReference to evaluate.
     */
    public DynamicAttributeUpdate(AttributeId attributeId, AttributeUpdateType updateType, DynamicAttributeValue valueReference) {
        this(attributeId, updateType, valueReference, Attributes.NULL_ATTRIBUTE_VALUE);
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class.
     *
     * @param attributeId     An {@link AttributeId} representing the ID of the attribute to update.
     * @param updateType      The UpdateType.
     * @param valueReference  The AttributeValueReference to evaluate.
     * @param comparisonValue The value to compare against.
     */
    public DynamicAttributeUpdate(AttributeId attributeId, AttributeUpdateType updateType, DynamicAttributeValue valueReference, long comparisonValue) {
        super(attributeId, updateType, Attributes.NULL_ATTRIBUTE_VALUE, comparisonValue);
        this.valueReference = Preconditions.checkNotNull(valueReference, "reference");
    }

    //endregion

    //region AttributeUpdate overrides.

    @Override
    public void setValue(long value) {
        super.setValue(value);
        this.valueSet = true;
    }

    @Override
    public long getValue() {
        Preconditions.checkState(this.valueSet, "value not set");
        return super.getValue();
    }

    @Override
    public boolean isDynamic() {
        return true;
    }

    @Override
    public String toString() {
        return String.format("[Dynamic]%s, ValRef = (%s)", super.toString(), this.valueReference);
    }

    //endregion

}