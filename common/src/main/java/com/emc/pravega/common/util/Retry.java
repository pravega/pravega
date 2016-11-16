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
import com.google.common.base.Preconditions;

/**
 * A Utility class to support retrying something that can fail with exponential backoff.
 * The class is designed to have a declarative interface for ease of use. It can be used as follows:
 * <p>
 * <pre>
 * <code>
 * Retry.withExpBackoff(1, 10, 5)
 * .retryingOn(FooException.class)
 * .throwingOn(RuntimeException.class).run(() -> {
 * //Do stuff here.
 * }
 * </code>
 * </pre>
 * <p>
 * The above will retry the code in the block up to 5 times if it throws FooException. If it throws
 * a RuntimeException or returns successfully it will throw or return immediately. The delay
 * following each of the filed attempts would be 1, 10, 100, 1000, and 10000ms respectively. If all
 * retries fail {@link RetriesExaustedException} will be thrown.
 * <p>
 * Note that the class is not a builder object, so the methods in the chain must be invoked in
 * order. The intermediate objects in the chain are reusable and threadsafe, so they can be shared
 * between invocations.
 * <p>
 * In the event that the exception passed to retryingOn() and throwingOn() are related. IE: In the
 * above example if FooException were to extend RuntimeException. Then the more specific exception
 * is given preference. (In the above case FooException would be retried).
 */
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

        public <RetryT extends Exception> RetryingOnException<RetryT> retryingOn(Class<RetryT> retryType) {
            Preconditions.checkNotNull(retryType);
            return new RetryingOnException<>(retryType, this);
        }

    }

    /**
     * Returned by {@link RetryWithBackoff#retryingOn(Class)} to add the type of exception
     * that should result in a retry. Any subtype of this exception will be retried unless the subtype is passed to
     * {@link RetryingOnException#throwingOn(Class)}.
     */
    public static final class RetryingOnException<RetryT extends Exception> {
        private final Class<RetryT> retryType;
        private final RetryWithBackoff params;

        private RetryingOnException(Class<RetryT> retryType, RetryWithBackoff params) {
            this.retryType = retryType;
            this.params = params;
        }

        public <ThrowsT extends Exception> ThrowingOnException<RetryT, ThrowsT> throwingOn(Class<ThrowsT> throwType) {
            Preconditions.checkNotNull(throwType);
            return new ThrowingOnException<>(retryType, throwType, params);
        }
    }

    @FunctionalInterface
    public interface Retryable<ReturnT, RetryableET extends Exception, NonRetryableET extends Exception> {
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

        @SuppressWarnings("unchecked")
        public <ReturnT> ReturnT run(Retryable<ReturnT, RetryT, ThrowsT> r) throws ThrowsT {
            Preconditions.checkNotNull(r);
            long delay = params.initialMillis;
            Exception last = null;
            for (int attemptNumber = 1; attemptNumber <= params.attempts; attemptNumber++) {
                try {
                    return r.attempt();
                } catch (Exception e) {
                    Class<? extends Exception> type = e.getClass();
                    if (throwType.isAssignableFrom(type) && retryType.isAssignableFrom(throwType)) {
                        throw (ThrowsT) e;
                    }
                    if (retryType.isAssignableFrom(type)) {
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
            }
            throw new RetriesExaustedException(last);
        }
    }

}
