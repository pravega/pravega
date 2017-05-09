/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.FutureHelpers;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A Utility class to support retrying something that can fail with exponential backoff.
 * The class is designed to have a declarative interface for ease of use. It can be used as follows:
 * <p>
 * {@code
 * Retry.withExpBackoff(1, 10, 5)
 * .retryingOn(FooException.class)
 * .throwingOn(RuntimeException.class).run(() -> {
 * //Do stuff here.
 * }
 * }
 * <p>
 * The above will retry the code in the block up to 5 times if it throws FooException. If it throws
 * a RuntimeException or returns successfully it will throw or return immediately. The delay
 * following each of the filed attempts would be 1, 10, 100, 1000, and 10000ms respectively. If all
 * retries fail {@link RetriesExhaustedException} will be thrown.
 * <p>
 * Note that the class is not a builder object, so the methods in the chain must be invoked in
 * order. The intermediate objects in the chain are reusable and threadsafe, so they can be shared
 * between
 * invocations.
 * <p>
 * In the event that the exception passed to retryingOn() and throwingOn() are related. i.e. In the
 * above example if FooException were to extend RuntimeException. Then the more specific exception
 * is given preference. (In the above case FooException would be retried).
 */
@Slf4j
public final class Retry {

    private Retry() {
    }

    public static RetryWithBackoff withExpBackoff(long initialMillis, int multiplier, int attempts) {
        return withExpBackoff(initialMillis, multiplier, attempts, Long.MAX_VALUE);
    }

    public static RetryWithBackoff withExpBackoff(long initialMillis, int multiplier, int attempts, long maxDelay) {
        Preconditions.checkArgument(initialMillis >= 1, "InitialMillis must be a positive integer.");
        Preconditions.checkArgument(multiplier >= 1, "multiplier must be a positive integer.");
        Preconditions.checkArgument(attempts >= 1, "attempts must be a positive integer.");
        Preconditions.checkArgument(maxDelay >= 1, "maxDelay must be a positive integer.");
        return new RetryWithBackoff(initialMillis, multiplier, attempts, maxDelay);
    }

    public static RetryUnconditionally indefinitelyWithExpBackoff(long initialMillis, int multiplier, long maxDelay, Consumer<Throwable> consumer) {
        Preconditions.checkArgument(initialMillis >= 1, "InitialMillis must be a positive integer.");
        Preconditions.checkArgument(multiplier >= 1, "multiplier must be a positive integer.");
        Preconditions.checkArgument(maxDelay >= 1, "maxDelay must be a positive integer.");
        RetryWithBackoff params = new RetryWithBackoff(initialMillis, multiplier, Integer.MAX_VALUE, maxDelay);
        return new RetryUnconditionally(consumer, params);
    }

    /**
     * Returned by {@link Retry#withExpBackoff(long, int, int)} to set the retry schedule.
     * Used to invoke {@link #retryingOn(Class)}. Note this object is reusable so this can be done more than once.
     */
    public static final class RetryWithBackoff {
        @Getter
        private final long initialMillis;
        @Getter
        private final int multiplier;
        @Getter
        private final int attempts;
        @Getter
        private final long maxDelay;

        private RetryWithBackoff(long initialMillis, int multiplier, int attempts, long maxDelay) {
            this.initialMillis = initialMillis;
            this.multiplier = multiplier;
            this.attempts = attempts;
            this.maxDelay = maxDelay;
        }

        public <RetryT extends Exception> RetryExceptionally<RetryT> retryingOn(Class<RetryT> retryType) {
            Preconditions.checkNotNull(retryType);
            return new RetryExceptionally<>(retryType, this);
        }

        public RetryConditionally retryWhen(Predicate<Throwable> predicate) {
            Preconditions.checkNotNull(predicate);
            return new RetryConditionally(predicate, this);
        }
    }

    /**
     * Returned by {@link RetryWithBackoff#retryingOn(Class)} to add the type of exception that should result in a retry.
     * Any subtype of this exception will be retried unless the subtype is passed to {@link RetryExceptionally#throwingOn(Class)}.
     */
    public static final class RetryExceptionally<RetryT extends Exception> {
        private final Class<RetryT> retryType;
        private final RetryWithBackoff params;

        private RetryExceptionally(Class<RetryT> retryType, RetryWithBackoff params) {
            this.retryType = retryType;
            this.params = params;
        }

        public <ThrowsT extends Exception> RetryAndThrowExceptionally<RetryT, ThrowsT> throwingOn(Class<ThrowsT> throwType) {
            Preconditions.checkNotNull(throwType);
            return new RetryAndThrowExceptionally<>(retryType, throwType, params);
        }
    }

    /**
     * Returned by {@link RetryWithBackoff#retryingOn(Class)} to add the type of exception that should result in a retry.
     * Any exception will be retried based on the predicate unless the subtype is passed to {@link RetryConditionally#throwingOn(Class)}.
     */
    public static final class RetryConditionally {
        private final Predicate<Throwable> predicate;
        private final RetryWithBackoff params;

        private RetryConditionally(Predicate<Throwable> predicate, RetryWithBackoff params) {
            this.predicate = predicate;
            this.params = params;
        }

        public <ThrowsT extends Exception> RetryAndThrowConditionally<ThrowsT> throwingOn(Class<ThrowsT> throwType) {
            Preconditions.checkNotNull(throwType);
            return new RetryAndThrowConditionally<>(predicate, throwType, params);
        }
    }

    @FunctionalInterface
    public interface Retryable<ReturnT, RetryableET extends Exception, NonRetryableET extends Exception> {
        ReturnT attempt() throws RetryableET, NonRetryableET;
    }

    public static abstract class RetryAndThrowBase<ThrowsT extends Exception> {
        final Class<ThrowsT> throwType;
        final RetryWithBackoff params;

        private RetryAndThrowBase(Class<ThrowsT> throwType, RetryWithBackoff params) {
            this.throwType = throwType;
            this.params = params;
        }

        @SuppressWarnings("unchecked")
        public <RetryT extends Exception, ReturnT> ReturnT run(Retryable<ReturnT, RetryT, ThrowsT> r) throws ThrowsT {
            Preconditions.checkNotNull(r);
            long delay = params.initialMillis;
            Exception last = null;
            for (int attemptNumber = 1; attemptNumber <= params.attempts; attemptNumber++) {
                try {
                    return r.attempt();
                } catch (Exception e) {
                    if (canRetry(e)) {
                        last = e;
                    } else if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    } else {
                        throw (ThrowsT) e;
                    }
                }

                final long sleepFor = delay;
                Exceptions.handleInterrupted(() -> Thread.sleep(sleepFor));

                delay = Math.min(params.maxDelay, params.multiplier * delay);
                log.debug("Retrying command. Retry #{}, timestamp={}", attemptNumber, Instant.now());
            }
            throw new RetriesExhaustedException(last);
        }

        public <ReturnT> CompletableFuture<ReturnT> runAsync(final Supplier<CompletableFuture<ReturnT>> r,
                                                             final ScheduledExecutorService executorService) {
            Preconditions.checkNotNull(r);
            CompletableFuture<ReturnT> result = new CompletableFuture<>();
            AtomicInteger attemptNumber = new AtomicInteger(1);
            AtomicLong delay = new AtomicLong(0);
            FutureHelpers.loop(
                    () -> !result.isDone(),
                    () -> FutureHelpers
                            .delayedFuture(r, delay.get(), executorService)
                            .thenAccept(result::complete) // We are done.
                            .exceptionally(ex -> {
                                if (!canRetry(ex)) {
                                    // Cannot retry this exception. Fail now.
                                    result.completeExceptionally(ex);
                                } else if (attemptNumber.get() + 1 > params.attempts) {
                                    // We have retried as many times as we were asked, unsuccessfully.
                                    result.completeExceptionally(new RetriesExhaustedException(ex));
                                } else {
                                    // Try again.
                                    delay.set(attemptNumber.get() == 1 ?
                                            params.initialMillis :
                                            Math.min(params.maxDelay, params.multiplier * delay.get()));
                                    attemptNumber.incrementAndGet();
                                    log.debug("Retrying command. Retry #{}, timestamp={}", attemptNumber, Instant.now());
                                }

                                return null;
                            }),
                    executorService);
            return result;
        }

        abstract boolean canRetry(final Throwable e);
    }

    /**
     * Returned by {@link RetryExceptionally#throwingOn(Class)} to add the type of exception that should cause the
     * method to throw right away. If any subtype of this exception occurs the method will throw it right away unless
     * that subtype was passed as the RetryType to {@link RetryWithBackoff#retryingOn(Class)}
     */
    public static final class RetryAndThrowExceptionally<RetryT extends Exception, ThrowsT extends Exception>
        extends RetryAndThrowBase<ThrowsT> {
        private final Class<RetryT> retryType;

        private RetryAndThrowExceptionally(Class<RetryT> retryType, Class<ThrowsT> throwType, RetryWithBackoff params) {
            super(throwType, params);
            this.retryType = retryType;
        }

        @Override
        boolean canRetry(final Throwable e) {
            Class<? extends Throwable> type = getErrorType(e);
            if (throwType.isAssignableFrom(type) && retryType.isAssignableFrom(throwType)) {
                return false;
            }
            return retryType.isAssignableFrom(type);
        }

        private Class<? extends Throwable> getErrorType(final Throwable e) {
            if (retryType.equals(CompletionException.class) || throwType.equals(CompletionException.class)) {
                return e.getClass();
            } else {
                if (e instanceof CompletionException && e.getCause() != null) {
                    return e.getCause().getClass();
                } else {
                    return e.getClass();
                }
            }
        }
    }

    /**
     * Returned by {@link RetryExceptionally#throwingOn(Class)} to add the type of exception that should cause the
     * method to throw right away. If any subtype of this exception occurs the method will throw it right away unless
     * the predicate passed to {@link RetryWithBackoff#retryingOn(Class)} says otherwise
     */
    public static final class RetryAndThrowConditionally<ThrowsT extends Exception>
            extends RetryAndThrowBase<ThrowsT> {
        private final Predicate<Throwable> predicate;

        private RetryAndThrowConditionally(Predicate<Throwable> predicate, Class<ThrowsT> throwType, RetryWithBackoff params) {
            super(throwType, params);
            this.predicate = predicate;
        }

        @Override
        boolean canRetry(final Throwable e) {
            return predicate.test(e);
        }
    }

    /**
     * Returned by {@link Retry#indefinitelyWithExpBackoff(long, int, long, Consumer)} (Class)} to
     * retry indefinitely. Its can retry method always returns true.
     */
    public static final class RetryUnconditionally extends RetryAndThrowBase<Exception> {
        private final Consumer<Throwable> consumer;

        RetryUnconditionally(Consumer<Throwable> consumer, RetryWithBackoff params) {
            super(Exception.class, params);
            this.consumer = consumer;
        }

        @Override
        boolean canRetry(final Throwable e) {
            consumer.accept(e);
            return true;
        }
    }
}
