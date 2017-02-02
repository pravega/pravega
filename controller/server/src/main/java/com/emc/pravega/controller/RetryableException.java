/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller;

/**
 * Retryable exception. Throw this when you want to let the caller know that this exception is transient and
 * warrants another retry.
 */
public class RetryableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of RetryableException class.
     *
     * @param reason reason for failure
     */
    public RetryableException(final String reason) {
        super(reason);
    }

    /**
     * Creates a new instance of RetryableException class.
     *
     * @param cause reason for failure
     */
    public RetryableException(final Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new instance of RetryableException class.
     *
     * @param reason  reason for failure
     * @param cause error cause
     */
    public RetryableException(final String reason, final Throwable cause) {
        super(reason, cause);
    }

    /**
     * Check if the exception is a subclass of retryable.
     * @param e exception thrown
     * @return whether it can be assigned to Retryable
     */
    public static boolean isRetryable(Throwable e) {
        return e.getClass().isAssignableFrom(RetryableException.class);
    }
}
