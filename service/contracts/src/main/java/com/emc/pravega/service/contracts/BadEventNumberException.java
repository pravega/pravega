/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.contracts;

/**
 * Exception that is thrown whenever an Append Operation failed because of inconsistent AppendContext.EventNumbers.
 */
public class BadEventNumberException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the BadEventNumberException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param lastEventNumber   The last known Event Number for the StreamSegment.
     * @param actualEventNumber The Event Number that was given as part of the Operation.
     */
    public BadEventNumberException(String streamSegmentName, long lastEventNumber, long actualEventNumber) {
        super(streamSegmentName, getMessage(lastEventNumber, actualEventNumber));
    }

    private static String getMessage(long expectedOffset, long actualOffset) {
        return String.format("Bad EventNumber. Expected greater than %d, given %d.", expectedOffset, actualOffset);
    }
}
