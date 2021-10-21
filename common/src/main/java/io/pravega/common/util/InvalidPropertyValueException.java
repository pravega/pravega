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
package io.pravega.common.util;

/**
 * Exception that is thrown whenever a Property Value is invalid based on what is expected.
 */
public class InvalidPropertyValueException extends ConfigurationException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the InvalidPropertyValueException class.
     *
     * @param message The message of the exception.
     */
    public InvalidPropertyValueException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the InvalidPropertyValueException class.
     *
     * @param fullPropertyName The full name (component + property) of the property.
     * @param actualValue      The actual value that was about to be processed.
     */
    public InvalidPropertyValueException(String fullPropertyName, String actualValue) {
        super(String.format("Value '%s' is invalid for property '%s'.", actualValue, fullPropertyName));
    }

    /**
     * Creates a new instance of the InvalidPropertyValueException class.
     *
     * @param fullPropertyName The full name (component + property) of the property.
     * @param actualValue      The actual value that was about to be processed.
     * @param cause            The causing Exception for this.
     */
    public InvalidPropertyValueException(String fullPropertyName, String actualValue, Throwable cause) {
        super(String.format("Value '%s' is invalid for property '%s'.", actualValue, fullPropertyName), cause);
    }
}
