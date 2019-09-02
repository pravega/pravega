/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

/**
 * Defines a Handle that can be used to operate on Segments in Storage.
 */
public interface SegmentHandle {
    /**
     * Gets the name of the Segment, as perceived by users of the Storage interface.
     */
    String getSegmentName();

    /**
     * Gets a value indicating whether this Handle was open in ReadOnly mode (true) or ReadWrite mode (false).
     */
    boolean isReadOnly();
}
