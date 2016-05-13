package com.emc.logservice.Core;

import java.util.function.Consumer;

/**
 * Helper methods for invoking Callback methods.
 */
public class CallbackHelpers
{
    /**
     * Invokes the given Consumer with the given argument, and catches any exceptions that it may throw.
     *
     * @param consumer       The consumer to invoke.
     * @param argument       The argument to pass to the consumer.
     * @param failureHandler An optional callback to invoke if the consumer threw any exceptions.
     * @param <T>            The type of the argument.
     * @throws NullPointerException If the consumer is null.
     */
    public static <T> void invokeSafely(Consumer<T> consumer, T argument, Consumer<Throwable> failureHandler)
    {
        if (consumer == null)
        {
            throw new NullPointerException("consumer");
        }

        try
        {
            consumer.accept(argument);
        }
        catch (Exception ex)
        {
            if (failureHandler != null)
            {
                invokeSafely(failureHandler, ex, null);
            }
        }
    }
}
