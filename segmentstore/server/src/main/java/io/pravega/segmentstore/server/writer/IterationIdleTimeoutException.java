/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import java.util.concurrent.TimeoutException;

/**
 * Exception that is thrown whenever a {@link StorageWriter} iteration has been idle (no activity) for more than
 * the allowed time.
 */
public class IterationIdleTimeoutException extends TimeoutException {
    IterationIdleTimeoutException(int containerId) {
        super(String.format("StorageWriter[%s] Iteration was idle timeout exceeded.", containerId));
    }
}
