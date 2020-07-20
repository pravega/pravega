/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import io.pravega.common.util.ArrayView;

/**
 * {@link IteratorState} encapsulates classes that will need to capture and pass state during iteration of a TableSegment.
 */
public interface IteratorState {

    /**
     * When paired with a deserialization method in the implementing class, this allows us to encapsulate asynchronous
     * iteration state in a portable manner.
     *
     * @return An {@link ArrayView} based serialization of the IteratorState we are encapsulating.
     */
    ArrayView serialize();

}
