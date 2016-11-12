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

package com.emc.pravega.common.concurrent;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.CallbackHelpers;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;


import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Extensions to Future and CompletableFuture.
 */
public final class FutureHelpers {
    /**
     * Waits for the provided future to be complete, and returns if it was successful, false otherwise.
     *
     * @param f   The future to wait for.
     * @param <T> The Type of the future's result.
     */
    public static <T> boolean await(CompletableFuture<T> f) {
        try {
            Exceptions.handleInterrupted(() -> f.get());
            return true;
        } catch (ExecutionException e) {
            return false;
        }
    }

    /**
     * Returns true if the future is done and successful.
     *
     * @param f   The future to inspect.
     * @param <T> The Type of the future's result.
     */
    public static <T> boolean isSuccessful(CompletableFuture<T> f) {
        return f.isDone() && !f.isCompletedExceptionally() && !f.isCancelled();
    }

    /**
     * Calls get on the provided future, handling interrupted, and transforming the executionException into an exception
     * of the type whose constructor is provided.
     *
     * @param future               The future whose result is wanted
     * @param exceptionConstructor This can be any function that either transforms an exception
     *                             IE: Passing RuntimeException::new will wrap the exception in a new RuntimeException.
     *                             If null is returned from the function no exception will be thrown.
     * @param <ResultT>            Type of the result.
     * @param <ExceptionT>         Type of the Exception.
     * @return The result of calling future.get()
     * @throws ExceptionT If thrown by the future.
     */
    public static <ResultT, ExceptionT extends Exception> ResultT getAndHandleExceptions(Future<ResultT> future,
                                                                                         Function<Throwable, ExceptionT>
                                                                                                 exceptionConstructor)
            throws ExceptionT {
        Preconditions.checkNotNull(exceptionConstructor);
        try {
            return Exceptions.handleInterrupted(() -> future.get());
        } catch (ExecutionException e) {
            ExceptionT result = exceptionConstructor.apply(e.getCause());
            if (result == null) {
                return null;
            } else {
                throw result;
            }
        }
    }

    /**
     * Same as {@link #getAndHandleExceptions(Future, Function)} but with a timeout on get().
     *
     * @param future               The future whose result is wanted
     * @param exceptionConstructor This can be any function that either transforms an exception
     *                             IE: Passing RuntimeException::new will wrap the exception in a new RuntimeException.
     *                             If null is returned from the function no exception will be thrown.
     * @param timeoutMillis        the timeout expressed in milliseconds before throwing {@link TimeoutException}
     * @param <ResultT>            Type of the result.
     * @param <ExceptionT>         Type of the Exception.
     * @throws ExceptionT       If thrown by the future.
     * @throws TimeoutException If the timeout expired prior to the future completing.
     */
    @SneakyThrows(InterruptedException.class)
    public static <ResultT, ExceptionT extends Exception> ResultT getAndHandleExceptions(Future<ResultT> future,
                                                                                         Function<Throwable, ExceptionT>
                                                                                                 exceptionConstructor,
                                                                                         long timeoutMillis)
            throws TimeoutException, ExceptionT {
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            ExceptionT result = exceptionConstructor.apply(e.getCause());
            if (result == null) {
                return null;
            } else {
                throw result;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /**
     * Creates a new CompletableFuture that is failed with the given exception.
     *
     * @param exception The exception to fail the CompletableFuture.
     * @param <T>       The Type of the future's result.
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable exception) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(exception);
        return result;
    }

    /**
     * Registers an exception listener to the given CompletableFuture.
     *
     * @param completableFuture The Future to register to.
     * @param exceptionListener The Listener to register.
     * @param <T>               The Type of the future's result.
     */
    public static <T> void exceptionListener(CompletableFuture<T> completableFuture, Consumer<Throwable>
            exceptionListener) {
        completableFuture.whenComplete((r, ex) -> {
            if (ex != null) {
                CallbackHelpers.invokeSafely(exceptionListener, ex, null);
            }
        });
    }

    /**
     * Registers an exception listener to the given CompletableFuture for a particular type of exception.
     *
     * @param completableFuture The Future to register to.
     * @param exceptionClass    The type of exception to listen to.
     * @param exceptionListener The Listener to register.
     * @param <T>               The Type of the future's result.
     * @param <E>               The Type of the exception.
     */
    @SuppressWarnings("unchecked")
    public static <T, E extends Throwable> void exceptionListener(CompletableFuture<T> completableFuture, Class<E>
            exceptionClass, Consumer<E> exceptionListener) {
        completableFuture.whenComplete((r, ex) -> {
            if (ex != null && exceptionClass.isAssignableFrom(ex.getClass())) {
                CallbackHelpers.invokeSafely(exceptionListener, (E) ex, null);
            }
        });
    }

    /**
     * Similar implementation to CompletableFuture.allOf(vararg) but that works on a Collection and that returns another
     * Collection which has the results of the given CompletableFutures.
     *
     * @param futures A Collection of CompletableFutures to wait on.
     * @param <T>     The type of the results items.
     */
    public static <T> CompletableFuture<Collection<T>> allOfWithResults(Collection<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures
                .size()]));
        return allDoneFuture.thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }

    /**
     * Similar implementation to CompletableFuture.allOf(vararg) but that works on a Collection.
     *
     * @param futures A Collection of CompletableFutures to wait on.
     * @param <T>     The type of the results items.
     */
    public static <T> CompletableFuture<Void> allOf(Collection<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    /**
     * Creates a new CompletableFuture that will timeout after the given amount of time.
     *
     * @param timeout         The timeout for the future.
     * @param executorService An ExecutorService that will be used to invoke the timeout on.
     * @param <T>             The Type argument for the CompletableFuture to create.
     */
    public static <T> CompletableFuture<T> futureWithTimeout(Duration timeout, ScheduledExecutorService
            executorService) {
        return futureWithTimeout(timeout, null, executorService);
    }

    /**
     * Creates a new CompletableFuture that will timeout after the given amount of time.
     *
     * @param timeout         The timeout for the future.
     * @param tag             A tag (identifier) to be used as a parameter to the TimeoutException.
     * @param executorService An ExecutorService that will be used to invoke the timeout on.
     * @param <T>             The Type argument for the CompletableFuture to create.
     * @return The result.
     */
    public static <T> CompletableFuture<T> futureWithTimeout(Duration timeout, String tag, ScheduledExecutorService
            executorService) {
        CompletableFuture<T> result = new CompletableFuture<T>();
        ScheduledFuture<Boolean> sf = executorService.schedule(() -> result.completeExceptionally(new
                TimeoutException(tag)), timeout.toMillis(), TimeUnit.MILLISECONDS);
        result.whenComplete((r, ex) -> sf.cancel(true));
        return result;
    }

    /**
     * Creates a CompletableFuture that will do nothing and complete after a specified delay, without using a thread
     * during
     * the delay.
     *
     * @param delay           The duration of the delay (how much to wait until completing the Future).
     * @param executorService An ExecutorService that will be used to complete the Future on.
     * @return A CompletableFuture that will complete after the specified delay.
     */
    public static CompletableFuture<Void> delayedFuture(Duration delay, ScheduledExecutorService executorService) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (delay.toMillis() == 0) {
            // Zero delay; no need to bother with scheduling a task in the future.
            result.complete(null);
        } else {
            ScheduledFuture<Boolean> sf = executorService.schedule(() -> result.complete(null), delay.toMillis(),
                    TimeUnit.MILLISECONDS);
            result.whenComplete((r, ex) -> sf.cancel(true));
        }

        return result;
    }

    /**
     * Attaches the given callback as an exception listener to the given CompletableFuture, which will be invoked when
     * the future times out (fails with a TimeoutException).
     *
     * @param future   The future to attach to.
     * @param callback The callback to invoke.
     * @param <T>      The Type of the future's result.
     */
    public static <T> void onTimeout(CompletableFuture<T> future, Consumer<TimeoutException> callback) {
        exceptionListener(future, TimeoutException.class, callback);
    }

    /**
     * Executes a loop using CompletableFutures, without invoking join()/get() on any of them or exclusively hogging
     * a thread.
     *
     * @param condition A Supplier that indicates whether to proceed with the loop or not.
     * @param loopBody  A Supplier that returns a CompletableFuture which represents the body of the loop. This
     *                  supplier is invoked every time the loopBody needs to execute.
     * @param executor  An Executor that is used to execute the condition and the loop support code.
     * @return A CompletableFuture that, when completed, indicates the loop terminated without any exception. If
     * either the loopBody or condition throw/return Exceptions, these will be set as the result of this returned
     * Future.
     */
    public static CompletableFuture<Void> loop(Supplier<Boolean> condition, Supplier<CompletableFuture<Void>>
            loopBody, Executor executor) {
        if (condition.get()) {
            return loopBody.get().thenComposeAsync(v -> loop(condition, loopBody, executor), executor);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Executes a loop using CompletableFutures, without invoking join()/get() on any of them or exclusively hogging
     * a thread.
     *
     * @param condition      A Supplier that indicates whether to proceed with the loop or not.
     * @param loopBody       A Supplier that returns a CompletableFuture which represents the body of the loop. This
     *                       supplier is invoked every time the loopBody needs to execute.
     * @param resultConsumer A Consumer that will be invoked with the result of every call to loopBody.
     * @param executor       An Executor that is used to execute the condition and the loop support code.
     * @param <T>            The Type of the future's result.
     * @return A CompletableFuture that, when completed, indicates the loop terminated without any exception. If
     * either the loopBody or condition throw/return Exceptions, these will be set as the result of this returned
     * Future.
     */
    public static <T> CompletableFuture<Void> loop(Supplier<Boolean> condition, Supplier<CompletableFuture<T>>
            loopBody, Consumer<T> resultConsumer, Executor executor) {
        if (condition.get()) {
            return loopBody.get()
                    .thenAccept(resultConsumer)
                    .thenComposeAsync(v -> loop(condition, loopBody, resultConsumer, executor), executor);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * This utility function returns a CompletableFuture object. This object represents the return of
     * the execution of the given function in an async manner. The exceptions are translated to the exceptions
     * that are understandable by the tier1 implementation.
     *
     * @param function            This function is executed in the async future.
     * @param exceptionTranslator utility function that translates the exception
     * @param executor            The context for the execution.
     * @param <T>                 Return type of the executor.
     * @return The CompletableFuture which either holds the result or is completed exceptionally.
     */
    public static <T> CompletableFuture<T> runAsyncTranslateException(Callable<T> function,
                                                                      Function<Exception, Exception>
                                                                              exceptionTranslator,
                                                                      Executor executor) {
        CompletableFuture<T> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                retVal.complete(function.call());
            } catch (Exception e) {
                retVal.completeExceptionally(exceptionTranslator.apply(e));
            }
        }, executor);
        return retVal;
    }
}
