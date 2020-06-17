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

public class EpochTransitionOperationExceptions {
    public static class EpochOperationException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    public static class InputInvalidException extends EpochOperationException {
    }

    public static class PreConditionFailureException extends EpochOperationException {
    }

    public static class ConditionInvalidException extends EpochOperationException {
    }

    public static class ConflictException extends EpochOperationException {
    }
}
