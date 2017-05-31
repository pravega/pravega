/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

/**
 * A client for looking at and editing the metadata related to a specific segment.
 */
public interface SegmentMetadataClient {
    
    /**
     * Returns the length of the current segment. i.e. the total length of all data written to the segment.
     *
     * @return The length of the current segment.
     */
    abstract long fetchCurrentStreamLength();

    /**
     * Gets the current value of the provided attribute.
     * @param attribute The attribute to get the value of.
     * @return The value of the attribute or {@link SegmentAttribute#NULL_VALUE} if it is not set.
     */
    abstract long fetchProperty(SegmentAttribute attribute);

    /**
     * Atomically replaces the value of attribute with newValue if it is expectedValue.
     * 
     * @param attribute The attribute to set
     * @param expectedValue The value the attribute is expected to be
     * @param newValue The new value for the attribute
     * @return If the replacement occurred. (False if the attribute was not expectedValue)
     */
    abstract boolean compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue);
    
}
