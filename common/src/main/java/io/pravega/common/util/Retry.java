/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.With;
import lombok.extern.slf4j.Slf4j;

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
    public static final RetryAndThrowBase<Exception> NO_RETRY = Retry
            .withExpBackoff(1, 1, 1)
            .retryingOn(Exception.class)
            .throwingOn(Exception.class);
    private final static long DEFAULT_RETRY_INIT_DELAY = 100;
    private final static int DEFAULT_RETRY_MULTIPLIER = 2;
    private final static long DEFAULT_RETRY_MAX_DELAY = Duration.ofSeconds(5).toMillis();

    private Retry() {
    }

    public static RetryWithBackoff withoutBackoff(int attempts) {
        return new RetryWithBackoff(0, 1, attempts, 0);
    }

    public static RetryWithBackoff withExpBackoff(long initialMillis, int multiplier, int attempts) {
        return withExpBackoff(initialMillis, multiplier, attempts, Long.MAX_VALUE);
    }

    public static RetryWithBackoff withExpBackoff(long initialMillis, int multiplier, int attempts, long maxDelay) {
        Preconditions.checkArgument(initialMillis >= 0, "InitialMillis cannot be negative.");
        Preconditions.checkArgument(multiplier >= 1, "multiplier must be a positive integer.");
        Preconditions.checkArgument(attempts >= 1, "attempts must be a positive integer.");
        Preconditions.checkArgument(maxDelay >= 0, "maxDelay cannot be negative.");
        return new RetryWithBackoff(initialMillis, multiplier, attempts, maxDelay);
    }

    public static RetryUnconditionally indefinitelyWithExpBackoff(long initialMillis, int multiplier, long maxDelay, Consumer<Throwable> consumer) {
        Preconditions.checkArgument(initialMillis >= 0, "InitialMillis cannot be negative.");
        Preconditions.checkArgument(multiplier >= 1, "multiplier must be a positive integer.");
        Preconditions.checkArgument(maxDelay >= 0, "maxDelay cannot be negative.");
        RetryWithBackoff params = new RetryWithBackoff(initialMillis, multiplier, Integer.MAX_VALUE, maxDelay);
        return new RetryUnconditionally(consumer, params);
    }

    public static RetryUnconditionally indefinitelyWithExpBackoff(String failureMessage) {
        Exceptions.checkNotNullOrEmpty(failureMessage, "failureMessage");
        RetryWithBackoff params = new RetryWithBackoff(DEFAULT_RETRY_INIT_DELAY, DEFAULT_RETRY_MULTIPLIER,
                Integer.MAX_VALUE, DEFAULT_RETRY_MAX_DELAY);
        Consumer<Throwable> consumer = e -> {
            if (log.isDebugEnabled()) {
                log.debug(failureMessage);
            } else {
                log.warn(failureMessage);
            }
        };
        return new RetryUnconditionally(consumer, params);
    }

    /**
     * Returned by {@link Retry#withExpBackoff(long, int, int)} to set the retry schedule.
     * Used to invoke {@link #retryingOn(Class)}. Note this object is reusable so this can be done more than once.
     */
    public static final class RetryWithBackoff {
        @Getter
        @With
        private final long initialMillis;
        @Getter
        @With
        private final int multiplier;
        @Getter
        @With
        private final int attempts;
        @Getter
        @With
        private final long maxDelay;
        @Getter
        @With
        private final boolean isInitialDelayForfirstRetry;

        private RetryWithBackoff(long initialMillis, int multiplier, int attempts, long maxDelay) {
            this(initialMillis, multiplier, attempts, maxDelay, false);
        }

        private RetryWithBackoff(long initialMillis, int multiplier, int attempts, long maxDelay, boolean isInitialDelayForfirstRetry) {
            this.initialMillis = initialMillis;
            this.multiplier = multiplier;
            this.attempts = attempts;
            this.maxDelay = maxDelay;
            this.isInitialDelayForfirstRetry = isInitialDelayForfirstRetry;
        }

        public <RetryT extends Exception> RetryExceptionally<RetryT> retryingOn(Class<RetryT> retryType) {
            Preconditions.checkNotNull(retryType);
            return new RetryExceptionally<>(retryType, this);
        }

        public RetryAndThrowConditionally retryWhen(Predicate<Throwable> predicate) {
            Preconditions.checkNotNull(predicate);
            return new RetryAndThrowConditionally(predicate, this);
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

    @FunctionalInterface
    public interface Retryable<ReturnT, RetryableET extends Exception, NonRetryableET extends Exception> {
        ReturnT attempt() throws RetryableET, NonRetryableET;
    }

    public static abstract class RetryAndThrowBase<ThrowsT extends Exception> {
        final RetryWithBackoff params;

        private RetryAndThrowBase(RetryWithBackoff params) {
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

                if (attemptNumber < params.attempts) {
                    // no need to sleep if it is the last attempt
                    final long sleepFor = delay;
                    Exceptions.handleInterrupted(() -> Thread.sleep(sleepFor));

                    delay = Math.min(params.maxDelay, params.multiplier * delay);
                    log.debug("Retrying command {} due to \"{}\" Retry #{}, timestamp={}",
                              r.toString(),
                              last.getMessage(),
                              attemptNumber,
                              Instant.now());
                }
            }
            throw new RetriesExhaustedException(last);
        }
        
        public CompletableFuture<Void> runInExecutor(final Runnable task,
                                                     final ScheduledExecutorService executorService) {
            Preconditions.checkNotNull(task);
            AtomicBoolean isDone = new AtomicBoolean();
            AtomicInteger attemptNumber = new AtomicInteger(1);
            AtomicLong delay = new AtomicLong(0);
            return Futures.loop(
                    () -> !isDone.get(),
                    () -> Futures.delayedFuture(Duration.ofMillis(delay.get()), executorService)
                                 .thenRunAsync(task, executorService)
                                 .thenRun(() -> isDone.set(true)) // We are done.
                                 .exceptionally(ex -> {
                                if (!canRetry(ex)) {
                                    // Cannot retry this exception. Fail now.
                                    isDone.set(true);
                                } else if (attemptNumber.get() + 1 > params.attempts) {
                                    // We have retried as many times as we were asked, unsuccessfully.
                                    isDone.set(true);
                                    throw new RetriesExhaustedException(ex);
                                } else {
                                    // Try again.
                                    delay.set(attemptNumber.get() == 1 ?
                                            params.initialMillis :
                                            Math.min(params.maxDelay, params.multiplier * delay.get()));
                                    attemptNumber.incrementAndGet();
                                    log.debug("Retrying command {} Retry #{}, timestamp={}", task.toString(), attemptNumber, Instant.now());
                                }
                                return null;
                            }),
                    executorService);
        }
        
        public <ReturnT> CompletableFuture<ReturnT> runAsync(final Supplier<CompletableFuture<ReturnT>> r,
                                                             final ScheduledExecutorService executorService) {
            Preconditions.checkNotNull(r);
            CompletableFuture<ReturnT> result = new CompletableFuture<>();
            AtomicInteger attemptNumber = new AtomicInteger(1);
            AtomicLong delay = new AtomicLong(params.isInitialDelayForfirstRetry ? params.initialMillis : 0);
            Futures.loop(
                    () -> !result.isDone(),
                    () -> Futures
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
                                    log.debug("Retrying command {} Retry #{}, timestamp={}", r.toString(), attemptNumber, Instant.now());
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
        private final Class<ThrowsT> throwType;

        private RetryAndThrowExceptionally(Class<RetryT> retryType, Class<ThrowsT> throwType, RetryWithBackoff params) {
            super(params);
            this.retryType = retryType;
            this.throwType = throwType;
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
            if (Exceptions.shouldUnwrap(retryType) || Exceptions.shouldUnwrap(throwType)) {
                return e.getClass();
            } else {
                return Exceptions.unwrap(e).getClass();
            }
        }
    }

    /**
     * Returned by {@link RetryExceptionally#throwingOn(Class)} to add the type of exception that should cause the
     * method to throw right away. If any subtype of this exception occurs the method will throw it right away unless
     * the predicate passed to {@link RetryWithBackoff#retryingOn(Class)} says otherwise
     */
    public static final class RetryAndThrowConditionally extends RetryAndThrowBase<RuntimeException> {
        private final Predicate<Throwable> predicate;

        private RetryAndThrowConditionally(Predicate<Throwable> predicate, RetryWithBackoff params) {
            super(params);
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
    public static final class RetryUnconditionally extends RetryAndThrowBase<RuntimeException> {
        private final Consumer<Throwable> consumer;

        RetryUnconditionally(Consumer<Throwable> consumer, RetryWithBackoff params) {
            super(params);
            this.consumer = consumer;
        }

        @Override
        boolean canRetry(final Throwable e) {
            consumer.accept(e);
            return true;
        }
    }
}
