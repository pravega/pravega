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

/**
 * Indicates that the given Segment has reached the maximum number of allowed attributes and no new attributes can be
 * added anymore.
 */
public class TooManyAttributesException extends StreamSegmentException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the TooManyAttributesException class.
     * @param streamSegmentName The name of the Stream Segment
     * @param maxAttributeCount The maximum number of allowed attributes per Stream segment.
     */
    public TooManyAttributesException(String streamSegmentName, int maxAttributeCount) {
        super(streamSegmentName, String.format("The maximum number of attributes (%d) has been reached.", maxAttributeCount));
    }
}
