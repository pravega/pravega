/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.service.selftest;

import lombok.Getter;

/**
 * Exception thrown whenever a validation error occurred.
 */
class ValidationException extends Exception {
    @Getter
    private final String segmentName;

    /**
     * Creates a new instance of the ValidationException class.
     *
     * @param segmentName      The name of the Segment that failed validation.
     * @param validationResult The ValidationResult that triggered this.
     */
    ValidationException(String segmentName, ValidationResult validationResult) {
        super(String.format("Segment = %s, Offset = %s, Reason = %s", segmentName, validationResult.getSegmentOffset(), validationResult.getFailureMessage()));
        this.segmentName = segmentName;
    }
}
