/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.server.SegmentOperation;
import java.util.Collection;

/**
 * Defines an Operation that can update a Segment's Attribute.
 */
public interface AttributeUpdaterOperation extends SegmentOperation {
    /**
     * Gets the Attribute updates for this StreamSegmentAppendOperation, if any.
     *
     * @return A Collection of Attribute updates, or null if no updates are available.
     */
    Collection<AttributeUpdate> getAttributeUpdates();
}
