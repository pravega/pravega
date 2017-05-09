/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.contracts;

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
@EqualsAndHashCode(of = {"attributeId", "value"})
@NotThreadSafe
public class AttributeUpdate {
    private final UUID attributeId;
    private final AttributeUpdateType updateType;
    private long value;

    @Override
    public String toString() {
        return String.format("AttributeId = %s, Value = %s, UpdateType = %s", this.attributeId, this.value, this.updateType);
    }
}
