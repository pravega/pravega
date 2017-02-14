/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.common.util;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

    /**
     * Initializes retry with back off instance with given configurations, but no delay.
     *
     * @param initialMillis Initial milliseconds to wait before retry.
     * @param multiplier Multiplier that will apply to initial milliseconds for next retry.
     * @param attempts Number of attempts of retry.
     * @return An Retry with back off instance.
     */
    private Retry() {
    }

    public static RetryWithBackoff withExpBackoff(long initialMillis, int multiplier, int attempts) {
        return withExpBackoff(initialMillis, multiplier, attempts, Long.MAX_VALUE);
    }

    /**
     * Initializes retry with back off instance with given configurations.
     *
     * @param initialMillis Initial milliseconds to wait before retry.
     * @param multiplier Multiplier that will apply to initial milliseconds for next retry.
     * @param attempts Number of attempts of retry.
     * @param maxDelay Maximum delay between retries.
     * @return An Retry with back off instance.
     */
    public static RetryWithBackoff withExpBackoff(long initialMillis, int multiplier, int attempts, long maxDelay) {
        Preconditions.checkArgument(initialMillis >= 1, "InitialMillis must be a positive integer.");
        Preconditions.checkArgument(multiplier >= 1, "multiplier must be a positive integer.");
        Preconditions.checkArgument(attempts >= 1, "attempts must be a positive integer.");
        Preconditions.checkArgument(maxDelay >= 1, "maxDelay must be a positive integer.");
        return new RetryWithBackoff(initialMillis, multiplier, attempts, maxDelay);
    }

    /**
     * Returned by {@link Retry#withExpBackoff(long, int, int)} to set the retry schedule.
     * Used to invoke {@link #retryingOn(Class)}. Note this object is reusable so this can be done more than once.
     */
    public static final class RetryWithBackoff {
        private final long initialMillis;
        private final int multiplier;
        private final int attempts;
        private final long maxDelay;

        private RetryWithBackoff(long initialMillis, int multiplier, int attempts, long maxDelay) {
            this.initialMillis = initialMillis;
            this.multiplier = multiplier;
            this.attempts = attempts;
            this.maxDelay = maxDelay;
        }

        /**
         * An exception that should result in a retry.
         *
         * @param retryType The type of retry.
         * @param <RetryT> Retry type.
         * @return Exception with all required retry params.
         */
        public <RetryT extends Exception> RetryingOnException<RetryT> retryingOn(Class<RetryT> retryType) {
            Preconditions.checkNotNull(retryType);
            return new RetryingOnException<>(retryType, this);
        }
    }

    /**
     * Returned by {@link RetryWithBackoff#retryingOn(Class)} to add the type of exception that should result in a retry.
     *
     * Any subtype of this exception will be retried unless the subtype is passed to {@link RetryingOnException#throwingOn(Class)}.
     */
    public static final class RetryingOnException<RetryT extends Exception> {
        private final Class<RetryT> retryType;
        private final RetryWithBackoff params;

        private RetryingOnException(Class<RetryT> retryType, RetryWithBackoff params) {
            this.retryType = retryType;
            this.params = params;
        }

        /**
         * An exception that should result in a retry.
         *
         * @param throwType Type of throwable.
         * @param <ThrowsT> Exception Type.
         * @return Exception with all required retry params.
         */
        public <ThrowsT extends Exception> ThrowingOnException<RetryT, ThrowsT> throwingOn(Class<ThrowsT> throwType) {
            Preconditions.checkNotNull(throwType);
            return new ThrowingOnException<>(retryType, throwType, params);
        }
    }

    @FunctionalInterface
    public interface Retryable<ReturnT, RetryableET extends Exception, NonRetryableET extends Exception> {

        /**
         * A job that have been attempted to run and throws retry exception indicating whether it should be retried or not.
         *
         * @return Return Type.
         * @throws RetryableET Retryable Type.
         * @throws NonRetryableET NonRetryable Type.
         */

        ReturnT attempt() throws RetryableET, NonRetryableET;
    }

    /**
     * Returned by {@link RetryingOnException#throwingOn(Class)} to add the type of exception that should cause the
     * method to throw right away. If any subtype of this exception occurs the method will throw it right away unless
     * that subtype was passed as the RetryType to {@link RetryWithBackoff#retryingOn(Class)}
     */
    public static final class ThrowingOnException<RetryT extends Exception, ThrowsT extends Exception> {
        private final Class<RetryT> retryType;
        private final Class<ThrowsT> throwType;
        private final RetryWithBackoff params;

        private ThrowingOnException(Class<RetryT> retryType, Class<ThrowsT> throwType, RetryWithBackoff params) {
            this.retryType = retryType;
            this.throwType = throwType;
            this.params = params;
        }

        /**
         * Attempts to run the given retryable job, and if failed, it schedules retry after 'delay'.
         *
         * @param r A job to be run.
         * @param <ReturnT> A Return Type
         * @return Job execution status or exception.
         * @throws ThrowsT that attempt to run R has failed.
         */
        @SuppressWarnings("unchecked")
        public <ReturnT> ReturnT run(Retryable<ReturnT, RetryT, ThrowsT> r) throws ThrowsT {
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

        /**
         * Runs the future using the given executor service for the first time.
         *
         * @param r A future to be run.
         * @param executorService An executor service.
         * @param <ReturnT> A Return Type
         * @return ReturnT
         */
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

        private boolean canRetry(final Throwable e) {
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
}
