/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.function;

/**
 * Functional interface for a runnable with no result that may throw an Exception.
 */
@FunctionalInterface
public interface RunnableWithException {
    void run() throws Exception;
}
