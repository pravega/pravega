package com.emc.pravega.service.contracts;

import lombok.Data;

/**
 * Represents an Attribute-Value pair.
 */
@Data
public class AttributeValue {
    private final Attribute attribute;
    private final long value;

    @Override
    public String toString() {
        return String.format("Attribute = %s, Value = %s", this.attribute, this.value);
    }
}
