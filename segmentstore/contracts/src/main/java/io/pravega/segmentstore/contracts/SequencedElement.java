/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

/**
 * Defines an Element that has a Sequence Number.
 */
public interface SequencedElement {
    /**
     * Gets a value indicating the Sequence Number for this item.
     * The Sequence Number is a unique, strictly monotonically increasing number that assigns order to items.
     *
     * @return Long indicating the Sequence number for this item.
     */
    long getSequenceNumber();
}
