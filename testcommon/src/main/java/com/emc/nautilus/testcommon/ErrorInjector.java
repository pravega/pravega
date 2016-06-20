package com.emc.nautilus.testcommon;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Simple count-based error injector.
 */
public class ErrorInjector<TEx extends Throwable> {
    private final Predicate<Integer> countTrigger;
    private final Supplier<TEx> errorGenerator;
    private final AtomicInteger count;
    private TEx lastCycleException;

    /**
     * Creates a new instance of the ErrorInjector class.
     *
     * @param countTrigger   A Predicate that will be used to determine whether to throw the exception. The argument to
     *                       the predicate is the number of times the throwIfNecessary() method was invoked.
     * @param errorGenerator A Supplier that creates Exceptions of type TEx, when invoked.
     */
    public ErrorInjector(Predicate<Integer> countTrigger, Supplier<TEx> errorGenerator) {
        this.countTrigger = countTrigger;
        this.errorGenerator = errorGenerator;
        this.count = new AtomicInteger();
    }

    /**
     * Throws an exception of type TEx if the count trigger activates.
     *
     * @throws TEx
     */
    public void throwIfNecessary() throws TEx {
        TEx ex = generateExceptionIfNecessary();
        if (ex != null) {
            throw ex;
        }
    }

    /**
     * Gets a value indicating the Exception (TEx) that was thrown during the last call to throwIfNecessary(). If no
     * exception was thrown, null is returned.
     *
     * @return
     */
    public TEx getLastCycleException() {
        return this.lastCycleException;
    }

    /**
     * Generates an exception of type TEx if the count trigger activates.
     *
     * @throws TEx
     */
    private TEx generateExceptionIfNecessary() {
        this.lastCycleException = null;
        if (countTrigger.test(this.count.getAndDecrement())) {
            this.lastCycleException = errorGenerator.get();
            assert this.lastCycleException != null;
            return this.lastCycleException;
        }

        return null;
    }

    /**
     * If the given error injector generates an exception, that exception will be thrown (Wrapped as a CompletionException).
     * If the given error injector is null or does not generate an exception, nothing will happen.
     *
     * @param injector The Error Injector to use.
     * @param <T>
     */
    public static <T extends Throwable> void throwSyncExceptionIfNeeded(ErrorInjector<T> injector) {
        if (injector != null) {
            T ex = injector.generateExceptionIfNecessary();
            if (ex != null) {
                throw new CompletionException(ex);
            }
        }
    }

    /**
     * Returns a CompletableFuture that, if the given injector generates a non-null exception, will be completed exceptionally.
     * If the given injector is null or does not generate an exception (null), this method returns a normally completed Future with no result.
     *
     * @param injector The Error Injector to use.
     * @return
     */
    public static <T extends Throwable> CompletableFuture<Void> throwAsyncExceptionIfNeeded(ErrorInjector<T> injector) {
        CompletableFuture<Void> result = null;
        if (injector != null) {
            T ex = injector.generateExceptionIfNecessary();
            if (ex != null) {
                result = new CompletableFuture<>();
                result.completeExceptionally(ex);
            }
        }

        return result != null ? result : CompletableFuture.completedFuture(null);
    }

    /**
     * Gets a value indicating the Exception (TEx) that was thrown during the last call to throwIfNecessary() for any of
     * the given ErrorInjectors (inspected in the order in which they were provided). If no exception was thrown, null is returned.
     *
     * @param injectors The injectors to inspect.
     * @return
     */
    @SafeVarargs
    public static <TEx extends Throwable> TEx getLastCycleException(ErrorInjector<TEx>... injectors) {
        for (ErrorInjector<TEx> injector : injectors) {
            TEx ex = injector.getLastCycleException();
            if (ex != null) {
                return ex;
            }
        }

        return null;
    }
}
