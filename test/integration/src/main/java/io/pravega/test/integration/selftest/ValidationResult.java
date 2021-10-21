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

import io.pravega.common.Exceptions;
import java.time.Duration;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents the result of a validation process.
 */
class ValidationResult {
    //region Members
    @Getter
    @Setter
    private ValidationSource source;
    @Getter
    @Setter
    private Duration elapsed;
    @Getter
    @Setter
    private Object address;

    /**
     * Indicates the length of the validated append. This value is undefined if isSuccess() == false.
     */
    @Getter
    private int length;

    /**
     * Indicates the Routing Key (if applicable) for the validation result.  This value is undefined if isSuccess() == false.
     */
    @Getter
    private int routingKey;

    /**
     * Indicates the failure message. This is undefined if isFailed() == false.
     */
    @Getter
    private String failureMessage;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ValidationResult class. Not to be used externally (use the static factory methods instead).
     */
    private ValidationResult() {
        this.length = 0;
        this.routingKey = -1;
        this.failureMessage = null;
        this.elapsed = null;
    }

    /**
     * Creates a new ValidationResult for a failed verification.
     */
    static ValidationResult failed(String message) {
        Exceptions.checkNotNullOrEmpty(message, "message");
        ValidationResult result = new ValidationResult();
        result.failureMessage = message;
        return result;
    }

    /**
     * Creates a new ValidationResult for a successful test.
     * @param routingKey  The Event Routing Key.
     * @param length      The Event size, in bytes, including header and contents.
     */
    static ValidationResult success(int routingKey, int length) {
        ValidationResult result = new ValidationResult();
        result.length = length;
        result.routingKey = routingKey;
        return result;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating whether the verification succeeded.
     */
    boolean isSuccess() {
        return this.failureMessage == null;
    }

    @Override
    public String toString() {
        if (isSuccess()) {
            return String.format("Success (Source=%s, Address=[%s])", this.source, this.address);
        } else {
            return String.format("Failed (Source=%s, Address=[%s], Reason=%s)", this.source, this.address, this.failureMessage);
        }
    }

    //endregion
}
