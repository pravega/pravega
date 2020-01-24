/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.retryable;

import io.pravega.common.Exceptions;

/**
 * Retryable exception. Throw this when you want to let the caller know that this exception is transient and
 * warrants another retry.
 * This is used to wrap exceptions from retrieables list into a new RetryableException
 */
public interface RetryableException {

    static boolean isRetryable(Throwable e) {
        Throwable cause = Exceptions.unwrap(e);

        return cause instanceof RetryableException;
    }
}
