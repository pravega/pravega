package com.emc.pravega.service.contracts;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents an Attribute-Value pair.
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(of = {"attribute", "value"})
public class AttributeUpdate {
    private final Attribute attribute;
    private long value;

    @Override
    public String toString() {
        return String.format("Attribute = %s, Value = %s", this.attribute, this.value);
    }
}
