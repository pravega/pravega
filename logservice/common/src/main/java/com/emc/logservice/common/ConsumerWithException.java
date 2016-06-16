package com.emc.logservice.common;

/**
 * Defines a Consumer that takes in one argument and may throw an exception.
 */
public interface ConsumerWithException<T, TEx extends Throwable> {
    void accept(T var1) throws TEx;
}
