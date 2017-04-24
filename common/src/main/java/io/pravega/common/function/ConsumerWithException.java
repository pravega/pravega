/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.function;

/**
 * Defines a Consumer that takes in one argument and may throw an exception.
 */
public interface ConsumerWithException<T, TEx extends Throwable> {
    void accept(T var1) throws TEx;
}
