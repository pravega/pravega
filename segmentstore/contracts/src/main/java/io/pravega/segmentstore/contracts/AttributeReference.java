/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import java.util.UUID;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A Reference that can be used to evaluate the value of another Attribute and transform it.
 *
 * @param <T> Return type.
 */
@RequiredArgsConstructor
public class AttributeReference<T> {
    /**
     * The Attribute Id to fetch the value of.
     */
    @Getter
    @NonNull
    private final UUID attributeId;

    /**
     * A Function that transforms the value of an Attribute into another value.
     */
    @Getter
    @NonNull
    private final Function<Long, T> transformation;

    @Override
    public String toString() {
        return "Attribute[" + this.attributeId + "]" + (getTransformation() == null ? "" : "+Transformation");
    }
}