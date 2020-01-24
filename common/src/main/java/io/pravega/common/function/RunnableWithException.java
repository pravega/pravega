/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.function;

/**
 * Functional interface for a runnable with no result that may throw an Exception.
 */
@FunctionalInterface
public interface RunnableWithException {
    void run() throws Exception;
}
