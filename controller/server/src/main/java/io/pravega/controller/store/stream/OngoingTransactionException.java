/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.controller.store.stream;

import io.pravega.controller.retryable.RetryableException;

/**
 * Exception thrown when scale cant be performed because on ongoing transactions.
 */
public class OngoingTransactionException extends RuntimeException implements RetryableException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of OngoingTransactionException class.
     *
     * @param value value
     */
    public OngoingTransactionException(final String value) {
        super(value);
    }
}
