/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.function;

import com.google.common.base.Preconditions;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Misc methods that can be used as callbacks.
 */
public final class Callbacks {
    /**
     * Empty consumer. Does nothing.
     *
     * @param ignored Ignored argument.
     * @param <T>     Return type. Ignored.
     */
    public static <T> void doNothing(T ignored) {
        // This method intentionally left blank.
    }

    /**
     * Identity Function.
     *
     * @param input The input.
     * @param <T>   Type of the input.
     * @return The input.
     */
    public static <T> T identity(T input) {
        return input;
    }

    /**
     * Invokes the given Consumer with the given argument, and catches any exceptions that it may throw.
     *
     * @param consumer       The consumer to invoke.
     * @param argument       The argument to pass to the consumer.
     * @param failureHandler An optional callback to invoke if the consumer threw any exceptions.
     * @param <T>            The type of the argument.
     * @throws NullPointerException If the consumer is null.
     */
    public static <T> void invokeSafely(Consumer<T> consumer, T argument, Consumer<Throwable> failureHandler) {
        Preconditions.checkNotNull(consumer, "consumer");

        try {
            consumer.accept(argument);
        } catch (Exception ex) {
            if (failureHandler != null) {
                invokeSafely(failureHandler, ex, null);
            }
        }
    }

    /**
     * Invokes the given Consumer with the given argument, and catches any exceptions that it may throw.
     *
     * @param consumer       The consumer to invoke.
     * @param argument1      The first argument to pass to the consumer.
     * @param argument2      The second argument to pass to the consumer.
     * @param failureHandler An optional callback to invoke if the consumer threw any exceptions.
     * @param <T1>           The type of the first argument.
     * @param <T2>           The type of the second argument.
     * @throws NullPointerException If the consumer is null.
     */
    public static <T1, T2> void invokeSafely(BiConsumer<T1, T2> consumer, T1 argument1, T2 argument2, Consumer<Throwable> failureHandler) {
        Preconditions.checkNotNull(consumer, "consumer");

        try {
            consumer.accept(argument1, argument2);
        } catch (Exception ex) {
            if (failureHandler != null) {
                invokeSafely(failureHandler, ex, null);
            }
        }
    }
}
