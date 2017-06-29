/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.retryable.RetryableException;

public class ScaleOperationExceptions {
    public static class ScaleOperationException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    public static class ScaleInputInvalidException extends ScaleOperationException {
    }

    public static class ScalePreConditionFailureException extends ScaleOperationException {
    }

    public static class ScaleConditionInvalidException extends ScaleOperationException {
    }

    public static class ScaleStartException extends ScaleOperationException implements RetryableException {
    }

    public static class ScalePostException extends ScaleOperationException implements RetryableException {
    }

    public static class ScaleRequestNotEnabledException extends ScaleOperationException {
    }
}
