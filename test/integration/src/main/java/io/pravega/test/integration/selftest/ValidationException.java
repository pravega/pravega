/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.selftest;

import lombok.Getter;

/**
 * Exception thrown whenever a validation error occurred.
 */
class ValidationException extends Exception {
    private static final long serialVersionUID = 1L;
    @Getter
    private final String target;

    /**
     * Creates a new instance of the ValidationException class.
     *
     * @param target           The name of the Target(Stream/Segment) that failed validation.
     * @param validationResult The ValidationResult that triggered this.
     */
    ValidationException(String target, ValidationResult validationResult) {
        super(String.format("Target = %s, Address = %s, Reason = %s", target, validationResult.getAddress(), validationResult.getFailureMessage()));
        this.target = target;
    }
}
