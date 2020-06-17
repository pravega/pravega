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

import java.util.UUID;
import lombok.Getter;

/**
 * Exception that is thrown whenever an Operation failed because of bad Attribute Updates.
 */
public class BadAttributeUpdateException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    /**
     * Whether the previous value for this attempted Attribute Update was missing. If true, this is likely the cause for
     * the failed update.
     */
    @Getter
    private final boolean previousValueMissing;

    @Getter
    private final UUID attributeId;

    /**
     * Creates a new instance of the BadAttributeUpdateException class.
     *
     * @param streamSegmentName    The name of the StreamSegment.
     * @param attributeUpdate      The AttributeUpdate that failed the check.
     * @param previousValueMissing If true, indicates that the previous value for this attempted Attribute Update was missing
     *                             and was likely the cause for the failed update.
     * @param errorMessage         The Event Number that was given as part of the Operation.
     */
    public BadAttributeUpdateException(String streamSegmentName, AttributeUpdate attributeUpdate, boolean previousValueMissing, String errorMessage) {
        super(streamSegmentName, getMessage(attributeUpdate, previousValueMissing, errorMessage));
        this.previousValueMissing = previousValueMissing;
        this.attributeId = attributeUpdate == null ? null : attributeUpdate.getAttributeId();
    }

    private static String getMessage(AttributeUpdate attributeUpdate, boolean previousValueMissing, String errorMessage) {
        return String.format("Bad Attribute Update (%s): %s%s.", attributeUpdate, errorMessage,
                previousValueMissing ? " (missing value)" : "");
    }
}
