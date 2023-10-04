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
package io.pravega.common.concurrent;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.function.Callbacks;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Extensions to Future and CompletableFuture.
 */
public final class Futures {

    /**
     * Returns a new {@link CompletableFuture} that completes with the same outcome as the given one, but on the given {@link Executor}.
     * This helps transfer the downstream callback executions on another executor.
     *
     * @param future The future whose result is wanted.
     * @param <T> The Type of the future's result.
     * @param executor The executor to transfer callback execution onto.
     * @return A new {@link CompletableFuture} that will complete with the same outcome as the given one, but on the given {@link Executor}.
     */
    public static <T> CompletableFuture<T> completeOn(CompletableFuture<T> future, final Executor executor) {

        CompletableFuture<T> result = new CompletableFuture<>();

        future.whenCompleteAsync((r, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
            } else {
                result.complete(r);
            }
        }, executor);

        return result;
    }

    /**
     * Waits for the provided future to be complete, and returns true if it was successful, false otherwise.
     *
     * @param f   The future to wait for.
     * @param <T> The Type of the future's result.
     * @return True if the provided CompletableFuture is complete and successful.
     */
    public static <T> boolean await(CompletableFuture<T> f) {
        return await(f, Long.MAX_VALUE);
    }

    /**
     * Waits for the provided future to be complete, and returns true if it was successful, false if it failed
     * or did not complete.
     *
     * @param timeout The maximum number of milliseconds to block
     * @param f       The future to wait for.
     * @param <T>     The Type of the future's result.
     * @return True if the given CompletableFuture is completed and successful within the given timeout.
     */
    public static <T> boolean await(CompletableFuture<T> f, long timeout) {
        Exceptions.handleInterrupted(() -> {
            try {
                f.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException e) {
                // Not handled here.
            }
        });
        return isSuccessful(f);
    }

    /**
     * Given a Supplier returning a Future, completes another future either with the result of the first future, in case
     * of normal completion, or exceptionally with the exception of the first future.
     *
     * @param futureSupplier A Supplier returning a Future to listen to.
     * @param toComplete     A CompletableFuture that has not yet been completed, which will be completed with the result
     *                       of the Future from futureSupplier.
     * @param <T>            Return type of Future.
     */
    public static <T> void completeAfter(Supplier<CompletableFuture<? extends T>> futureSupplier, CompletableFuture<T> toComplete) {
        Preconditions.checkArgument(!toComplete.isDone(), "toComplete is already completed.");
        try {
            CompletableFuture<? extends T> f = futureSupplier.get();

            // Async termination.
            f.thenAccept(toComplete::complete);
            Futures.exceptionListener(f, toComplete::completeExceptionally);
        } catch (Throwable ex) {
            // Synchronous termination.
            toComplete.completeExceptionally(ex);
            throw ex;
        }
    }

    /**
     * Returns true if the future is done and successful.
     *
     * @param f   The future to inspect.
     * @param <T> The Type of the future's result.
     * @return True if the given CompletableFuture has completed successfully.
     */
    public static <T> boolean isSuccessful(CompletableFuture<T> f) {
        return f.isDone() && !f.isCompletedExceptionally() && !f.isCancelled();
    }
    
    /**
     * If the future has failed returns the exception that caused it. Otherwise returns null.
     * 
     * @param <T> The Type of the future's result.
     * @param future   The future to inspect.
     * @return null or the exception that caused the Future to fail.
     */
    public static <T> Throwable getException(CompletableFuture<T> future) {
        try {            
            future.getNow(null);
            return null;
        } catch (Exception e) {
            return Exceptions.unwrap(e);
        }
    }

    /**
     * Gets a future returning its result, or the exception that caused it to fail. (Unlike a normal
     * future the cause does not need to be extracted.) Because some of these exceptions are
     * checked, and the future does not allow expressing this, the compile time information is lost.
     *
     * To get around this this method is generically typed with up to 3 exception types, that can be
     * used to re-introduce the exception types that could cause the future to fail into to the
     * compiler so they can be tracked. Note that nothing restricts or ensures that the exceptions
     * thrown from the future are of this type. The exception will always be throw as it was set on
     * the future, these types are purely for the benefit of the compiler. It is up to the caller to
     * ensure that this type matches the exception type that can fail the future.
     *
     * @param future The future to call get() on.
     * @param <ResultT> The result type of the provided future
     * @param <E1> A type of exception that may cause the future to fail.
     * @param <E2> A type of exception that may cause the future to fail.
     * @param <E3> A type of exception that may cause the future to fail.
     * @return The result of the provided future.
     * @throws E1 If exception E1 occurs.
     * @throws E2 If exception E2 occurs.
     * @throws E3 If exception E3 occurs.
     */
    public static <ResultT, E1 extends Exception, E2 extends Exception, E3 extends Exception> ResultT getThrowingException(Future<ResultT> future) throws E1, E2, E3 {
        Preconditions.checkNotNull(future);
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Exceptions.sneakyThrow(e);
        } catch (Exception e) {
            throw Exceptions.sneakyThrow(Exceptions.unwrap(e));
        }
    }

    /**
     * Gets a future returning its result, or the exception that caused it to fail. (Unlike a normal
     * future the cause does not need to be extracted.) Because some of these exceptions are
     * checked, and the future does not allow expressing this, the compile time information is lost.
     * To get around this method is generically typed with up to 3 exception types, that can be
     * used to re-introduce the exception types that could cause the future to fail into to the
     * compiler so they can be tracked. Note that nothing restricts or ensures that the exceptions
     * thrown from the future are of this type. The exception will always be throw as it was set on
     * the future, these types are purely for the benefit of the compiler. It is up to the caller to
     * ensure that this type matches the exception type that can fail the future.
     *
     * @param <ResultT>     The result type of the provided future
     * @param <E1>          A type of exception that may cause the future to fail.
     * @param <E2>          A type of exception that may cause the future to fail.
     * @param <E3>          A type of exception that may cause the future to fail.
     * @param future        The future to call get() on.
     * @param timeoutMillis This is the maximum time to get the result.
     * @return The result of the provided future.
     * @throws E1 If exception E1 occurs.
     * @throws E2 If exception E2 occurs.
     * @throws E3 If exception E3 occurs.
     * @throws TimeoutException If future doesn't complete in provided time duration.
     */
    public static <ResultT, E1 extends Exception, E2 extends Exception, E3 extends Exception>
                  ResultT getThrowingExceptionWithTimeout(Future<ResultT> future, long timeoutMillis) throws E1, E2, E3, TimeoutException {
        Preconditions.checkNotNull(future);
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Exceptions.sneakyThrow(e);
        } catch (TimeoutException  e) {
            throw Exceptions.sneakyThrow(e);
        } catch (Exception e) {
            throw Exceptions.sneakyThrow(Exceptions.unwrap(e));
        }
    }
    
    /**
     * Similar to {@link CompletableFuture#join()} but with a timeout parameter.
     * 
     * @param <ResultT> The result type of the CompletableFuture
     * @param future The future to join on
     * @param timeout The time to wait
     * @param unit the units the above time is given in
     * @return The result of the future
     * @throws TimeoutException if the timeout is reached and the future has not yet completed.
     */
    public static <ResultT> ResultT join(CompletableFuture<ResultT> future, long timeout, TimeUnit unit)
            throws TimeoutException {
        return Exceptions.handleInterruptedCall(() -> {
            try {
                return future.get(timeout, unit);
            } catch (ExecutionException e) {
                throw new CompletionException(e.getCause());
            }
        });
    }

    /**
     * Calls get on the provided future, handling interrupted, and transforming the executionException into an exception
     * of the type whose constructor is provided.
     *
     * @param future               The future whose result is wanted
     * @param exceptionConstructor This can be any function that either transforms an exception
     *                             i.e. Passing RuntimeException::new will wrap the exception in a new RuntimeException.
     *                             If null is returned from the function no exception will be thrown.
     * @param <ResultT>            Type of the result.
     * @param <ExceptionT>         Type of the Exception.
     * @return The result of calling future.get()
     * @throws ExceptionT If thrown by the future.
     */
    public static <ResultT, ExceptionT extends Exception> ResultT getAndHandleExceptions(Future<ResultT> future,
                                                                                         Function<Throwable, ExceptionT> exceptionConstructor) throws ExceptionT {
        Preconditions.checkNotNull(exceptionConstructor);
        try {
            return Exceptions.handleInterruptedCall(() -> future.get());
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
     * Calls get on the provided future, passing a timeout, handling interrupted, and transforming
     * the {@link ExecutionException} into an exception of the type whose constructor is provided.
     *
     * @param future The future whose result is wanted
     * @param exceptionConstructor This can be any function that either transforms an exception i.e.
     *            Passing RuntimeException::new will wrap the exception in a new RuntimeException.
     *            If null is returned from the function no exception will be thrown.
     * @param timeout The time to wait on the future.
     * @param unit The unit that timeout is specified in.
     * @param <ResultT> Type of the result.
     * @param <ExceptionT> Type of the Exception.
     * @return The result of calling future.get()
     * @throws ExceptionT If thrown by the future.
     * @throws TimeoutException If a timeout occurs.
     */
    @SneakyThrows(InterruptedException.class)
    public static <ResultT, ExceptionT extends Exception> ResultT getAndHandleExceptions(Future<ResultT> future,
                                                                                         Function<Throwable, ExceptionT> exceptionConstructor,
                                                                                         long timeout, TimeUnit unit) throws ExceptionT, TimeoutException {
        
        Preconditions.checkNotNull(exceptionConstructor);
        try {
            return future.get(timeout, unit);
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
     * @return A CompletableFuture that fails with the given exception.
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable exception) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(exception);
        return result;
    }

    /**
     * Returns a CompletableFuture that will complete with the same outcome or result as the given source, but when
     * cancelled, will apply a consumer to the eventual result of the original future.
     * <p>
     * If the returned CompletableFuture is NOT cancelled ({@link CompletableFuture#cancel}):
     * - If source completes normally, the result CompletableFuture will complete with the same result.
     * - If source completes exceptionally, the result CompletableFuture will complete with the same result.
     * <p>
     * If the returned CompletableFuture is cancelled ({@link CompletableFuture#cancel}):
     * - If the source has already completed, the result CompletableFuture will also be completed with the same outcome.
     * - If the source has not already been completed, if it completes normally, then `onCancel` will be applied to
     * the result when it eventually completes. The source completes exceptionally, nothing will happen.
     *
     * @param source   The CompletableFuture to wrap.
     * @param onCancel A Consumer to invoke on source's eventual completion result if the result of this method is cancelled.
     * @param <T>      Result type.
     * @return A CompletableFuture that will complete with the same outcome or result as the given source.
     */
    public static <T> CompletableFuture<T> cancellableFuture(CompletableFuture<T> source, Consumer<T> onCancel) {
        if (source == null) {
            return null;
        }

        val result = new CompletableFuture<T>();
        source.whenComplete((r, ex) -> {
            if (ex == null) {
                result.complete(r);
            } else {
                result.completeExceptionally(ex);
            }
        });
        Futures.exceptionListener(result, ex -> {
            if (ex instanceof CancellationException && !source.isCancelled()) {
                source.thenAccept(onCancel);
            }
        });
        return result;
    }

    /**
     * Registers an exception listener to the given CompletableFuture.
     *
     * @param completableFuture The Future to register to.
     * @param exceptionListener The Listener to register.
     * @param <T>               The Type of the future's result.
     */
    public static <T> void exceptionListener(CompletableFuture<T> completableFuture, Consumer<Throwable> exceptionListener) {
        completableFuture.whenComplete((r, ex) -> {
            if (ex != null) {
                Callbacks.invokeSafely(exceptionListener, ex, null);
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
    public static <T, E extends Throwable> void exceptionListener(CompletableFuture<T> completableFuture, Class<E> exceptionClass, Consumer<E> exceptionListener) {
        completableFuture.whenComplete((r, ex) -> {
            if (ex != null && exceptionClass.isAssignableFrom(ex.getClass())) {
                Callbacks.invokeSafely(exceptionListener, (E) ex, null);
            }
        });
    }

    /**
     * Same as CompletableFuture.exceptionally(), except that it allows returning a CompletableFuture instead of a single value.
     *
     * @param future  The original CompletableFuture to attach an Exception Listener.
     * @param handler A Function that consumes a Throwable and returns a CompletableFuture of the same type as the original one.
     *                This Function will be invoked if the original Future completed exceptionally.
     * @param <T>     Type of the value of the original Future.
     * @return A new CompletableFuture that will be completed either with the result of future (if it completed normally),
     * or with the result of handler when applied to the exception of future, should future complete exceptionally.
     */
    public static <T> CompletableFuture<T> exceptionallyCompose(CompletableFuture<T> future, Function<Throwable, CompletableFuture<T>> handler) {
        return future.handle((r, ex) -> {
            if (ex == null) {
                return CompletableFuture.completedFuture(r);
            }
            return handler.apply(ex);
        }).thenCompose(f -> f);
    }

    /**
     * Same as CompletableFuture.exceptionally(), except that it allows certain exceptions, as defined by the isExpected parameter.
     * If such an exception is caught, the given exceptionValue is then returned. All other Exceptions will be re-thrown.
     *
     * @param future         The original CompletableFuture to attach to.
     * @param isExpected     A Predicate that can check whether an Exception is expected or not.
     * @param exceptionValue The value to return in case the thrown Exception if of type exceptionClass.
     * @param <T>            The Type of the Future's result.
     * @return A new CompletableFuture that will complete either:
     * - With the same result as the original Future if that one completed normally
     * - With exceptionValue if the original Future completed with an expected exception.
     * - Exceptionally with the original Future's exception if none of the above are true.
     */
    public static <T> CompletableFuture<T> exceptionallyExpecting(CompletableFuture<T> future, Predicate<Throwable> isExpected, T exceptionValue) {
        return future.exceptionally(ex -> {
            if (isExpected.test(Exceptions.unwrap(ex))) {
                return exceptionValue;
            }
            throw new CompletionException(ex);
        });
    }

    /**
     * Same as exceptionallyExpecting(), except that it allows executing/returning a Future as a result in case of an
     * expected exception.
     * If such an exception is caught, the given exceptionFutureSupplier is invoked and its result is then returned.
     * All other Exceptions will be re-thrown.
     *
     * @param future                  The original CompletableFuture to attach to.
     * @param isExpected              A Predicate that can check whether an Exception is expected or not.
     * @param exceptionFutureSupplier A Supplier that returns a CompletableFuture which will be invoked in case the thrown
     *                                Exception if of type exceptionClass.
     * @param <T>                     The Type of the Future's result.
     * @return A new CompletableFuture that will complete either:
     * - With the same result as the original Future if that one completed normally
     * - With exceptionValue if the original Future completed with an expected exception.
     * - Exceptionally with the original Future's exception if none of the above are true.
     */
    public static <T> CompletableFuture<T> exceptionallyComposeExpecting(CompletableFuture<T> future, Predicate<Throwable> isExpected,
                                                                         Supplier<CompletableFuture<T>> exceptionFutureSupplier) {
        return exceptionallyCompose(future,
                ex -> {
                    if (isExpected.test(Exceptions.unwrap(ex))) {
                        return exceptionFutureSupplier.get();
                    } else {
                        return Futures.failedFuture(ex);
                    }
                });
    }

    /**
     * Returns a CompletableFuture that will end when the given future ends, but discards its result. If the given future
     * fails, the returned future will fail with the same exception.
     *
     * @param future The CompletableFuture to attach to.
     * @param <T>    The type of the input's future result.
     * @return A CompletableFuture that will complete when the given future completes. If the given future fails, so will
     * this future.
     */
    public static <T> CompletableFuture<Void> toVoid(CompletableFuture<T> future) {
        return future.thenAccept(Callbacks::doNothing);
    }

    /**
     * Returns a CompletableFuture that will end when the given future ends, expecting a certain
     * result. If the supplied value is not the same (using .equals() comparison) as the result an
     * exception from the supplier will be thrown. If the given future fails, the returned future
     * will fail with the same exception.
     *
     * @param <T>                  The type of the value expected
     * @param <E>                  The type of the exception to be throw if the value is not found.
     * @param future               the CompletableFuture to attach to.
     * @param expectedValue        The value expected
     * @param exceptionConstructor Constructor for an exception in the event there is not a match.
     * @return A void completable future.
     */
    public static <T, E extends Exception> CompletableFuture<Void> toVoidExpecting(CompletableFuture<T> future,
                                                                                   T expectedValue, Supplier<E> exceptionConstructor) {
        return future.thenApply(value -> expect(value, expectedValue, exceptionConstructor));
    }

    /**
     * Same as CompletableFuture.handle(), except that it allows returning a CompletableFuture instead of a single value.
     *
     * @param future  The original CompletableFuture to attach a handle callback.
     * @param handler A BiFunction that consumes a Throwable or successful result 
     *                and returns a CompletableFuture of the same type as the original one.
     *                This Function will be invoked after the original Future completes both successfully and exceptionally.
     * @param <T>     Type of the value of the original Future.
     * @param <U>     Type of the value of the returned Future.
     * @return A new CompletableFuture that will handle the result/exception and return a new future or throw the exception. 
     */
    public static <T, U> CompletableFuture<U> handleCompose(CompletableFuture<T> future, 
                                                         BiFunction<T, Throwable, CompletableFuture<U>> handler) {
        return future.handle(handler).thenCompose(f -> f);
    }

    @SneakyThrows
    private static <T, E extends Exception> Void expect(T value, T expected, Supplier<E> exceptionConstructor) {
        if (!expected.equals(value)) {
            throw exceptionConstructor.get();
        }
        return null;
    }

    //endregion

    //region Collections

    /**
     * Similar implementation to CompletableFuture.allOf(vararg) but that works on a Collection and that returns another
     * Collection which has the results of the given CompletableFutures.
     *
     * @param futures A List of CompletableFutures to wait on.
     * @param <T>     The type of the results items.
     * @return  A new CompletableFuture List that is completed when all of the given CompletableFutures complete.
     */
    public static <T> CompletableFuture<List<T>> allOfWithResults(List<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }

    /**
     * Similar to CompletableFuture.allOf(varargs), but that works on a Map and that returns another Map which has the
     * results of the given CompletableFutures, with the same input keys.
     *
     * @param futureMap A Map of Keys to CompletableFutures to wait on.
     * @param <K>       The type of the Keys.
     * @param <V>       The Type of the Values.
     * @return A CompletableFuture that will contain a Map of Keys to Values, where Values are the results of the Futures
     * in the input map.
     */
    public static <K, V> CompletableFuture<Map<K, V>> allOfWithResults(Map<K, CompletableFuture<V>> futureMap) {
        return Futures
                .allOf(futureMap.values())
                .thenApply(ignored ->
                        futureMap.entrySet().stream()
                                 .collect(Collectors.toMap(Map.Entry::getKey, future -> future.getValue().join()))
                );
    }

    /**
     * Similar to CompletableFuture.allOf(varargs), but that works on a Map and that returns another Map which has the
     * results of the given CompletableFutures, with the same input keys.
     *
     * @param futureMap A Map of Keys to CompletableFutures to wait on.
     * @param <K>       The type of the Keys.
     * @param <V>       The Type of the Values.
     * @return A CompletableFuture that will contain a Map of Keys to Values, where Values are the results of the Futures
     * in the input map.
     */
    public static <K, V> CompletableFuture<Map<K, V>> keysAllOfWithResults(Map<CompletableFuture<K>, V> futureMap) {
        return Futures
                .allOf(futureMap.keySet())
                .thenApply(ignored ->
                        futureMap.entrySet().stream()
                                .collect(Collectors.toMap(future -> future.getKey().join(), Map.Entry::getValue)));
    }

    /**
     * Similar implementation to CompletableFuture.allOf(vararg) but that works on a Collection.
     *
     * @param futures A Collection of CompletableFutures to wait on.
     * @param <T>     The type of the results items.
     * @return A new CompletableFuture that is completed when all of the given CompletableFutures complete.
     */
    public static <T> CompletableFuture<Void> allOf(Collection<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    /**
     * Filter that takes a predicate that evaluates in future and returns the filtered list that evaluates in future.
     *
     * @param input     Input list.
     * @param predicate Predicate that evaluates in the future.
     * @param <T>       Type parameter.
     * @return List that evaluates in future.
     */
    public static <T> CompletableFuture<List<T>> filter(List<T> input, Function<T, CompletableFuture<Boolean>> predicate) {
        Preconditions.checkNotNull(input);

        val allFutures = input.stream().collect(Collectors.toMap(key -> key, predicate::apply));
        return Futures
                .allOf(allFutures.values())
                .thenApply(ignored ->
                        allFutures.entrySet()
                                  .stream()
                                  .filter(e -> e.getValue().join())
                                  .map(Map.Entry::getKey).collect(Collectors.toList())
                );
    }

    //endregion

    //region Time-based Futures

    /**
     * Creates a new CompletableFuture that will timeout after the given amount of time.
     *
     * @param timeout         The timeout for the future.
     * @param executorService An ExecutorService that will be used to invoke the timeout on.
     * @param <T>             The Type argument for the CompletableFuture to create.
     * @return A CompletableFuture with a timeout.
     */
    public static <T> CompletableFuture<T> futureWithTimeout(Duration timeout, ScheduledExecutorService executorService) {
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
    public static <T> CompletableFuture<T> futureWithTimeout(Duration timeout, String tag, ScheduledExecutorService executorService) {
        return futureWithTimeout(CompletableFuture::new, timeout, tag, executorService);
    }
    
    /**
     * Creates a new CompletableFuture that either holds the result of future from the futureSupplier
     * or will timeout after the given amount of time.
     *
     * @param futureSupplier  Supplier of the future. 
     * @param timeout         The timeout for the future.
     * @param tag             A tag (identifier) to be used as a parameter to the TimeoutException.
     * @param executorService An ExecutorService that will be used to invoke the timeout on.
     * @param <T>             The Type argument for the CompletableFuture to create.
     * @return A CompletableFuture which is either completed within given timebound or failed with timeout exception.
     */
    public static <T> CompletableFuture<T> futureWithTimeout(Supplier<CompletableFuture<T>> futureSupplier,
                                                             Duration timeout, String tag, ScheduledExecutorService executorService) {
        CompletableFuture<T> future = futureSupplier.get();
        ScheduledFuture<Boolean> sf = executorService.schedule(() -> future.completeExceptionally(
                new TimeoutException(tag)), timeout.toMillis(), TimeUnit.MILLISECONDS);
        
        return future.whenComplete((r, ex) -> {
            sf.cancel(true);
        });
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
     * Creates a CompletableFuture that will do nothing and complete after a specified delay, without using a thread during
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
            ScheduledFuture<Boolean> sf = executorService.schedule(() -> result.complete(null), delay.toMillis(), TimeUnit.MILLISECONDS);
            result.whenComplete((r, ex) -> sf.cancel(true));
        }

        return result;
    }

    /**
     * Executes the asynchronous task after the specified delay.
     *
     * @param task            Asynchronous task.
     * @param delay           Delay in milliseconds.
     * @param executorService Executor on which to execute the task.
     * @param <T>             Type parameter.
     * @return A CompletableFuture that will be completed with the result of the given task.
     */
    public static <T> CompletableFuture<T> delayedFuture(final Supplier<CompletableFuture<T>> task,
                                                         final long delay,
                                                         final ScheduledExecutorService executorService) {
        return delayedFuture(Duration.ofMillis(delay), executorService)
                .thenCompose(v -> task.get());
    }

    /**
     * Executes the given task after the specified delay.
     *
     * @param task            Asynchronous task.
     * @param delay           Delay.
     * @param executorService Executor on which to execute the task.
     * @param <T>             Type parameter.
     * @return A CompletableFuture that will be completed with the result of the given task, or completed exceptionally
     * if the task fails.
     */
    public static <T> CompletableFuture<T> delayedTask(final Supplier<T> task,
                                                       final Duration delay,
                                                       final ScheduledExecutorService executorService) {
        CompletableFuture<T> result = new CompletableFuture<>();
        executorService.schedule(() -> result.complete(runOrFail(() -> task.get(), result)),
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
        return result;
    }

    /**
     * Runs the provided Callable in the current thread (synchronously) if it throws any Exception or
     * Throwable the exception is propagated, but the supplied future is also failed.
     *
     * @param <T>      The type of the future.
     * @param <R>      The return type of the callable.
     * @param callable The function to invoke.
     * @param future   The future to fail if the function fails.
     * @return The return value of the function.
     */
    @SneakyThrows(Exception.class)
    public static <T, R> R runOrFail(Callable<R> callable, CompletableFuture<T> future) {
        try {
            return callable.call();
        } catch (Throwable t) {
            future.completeExceptionally(t);
            throw t;
        }
    }

    //endregion

    //region Loops

    /**
     * Executes a loop using CompletableFutures over the given Iterable, processing each item in order, without overlap,
     * using the given Executor for task execution.
     *
     * @param iterable An Iterable instance to loop over.
     * @param loopBody A Function that, when applied to an element in the given iterable, returns a CompletableFuture which
     *                 will complete when the given element has been processed. This function is invoked every time the
     *                 loopBody needs to execute.
     * @param executor An Executor that is used to execute the condition and the loop support code.
     * @param <T>      Type of items in the given iterable.
     * @return A CompletableFuture that, when completed, indicates the loop terminated without any exception. If
     * the loopBody throws/returns Exceptions, these will be set as the result of this returned Future.
     */
    public static <T> CompletableFuture<Void> loop(Iterable<T> iterable, Function<T, CompletableFuture<Boolean>> loopBody, Executor executor) {
        Iterator<T> iterator = iterable.iterator();
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return loop(
                () -> iterator.hasNext() && canContinue.get(),
                () -> loopBody.apply(iterator.next()),
                canContinue::set, executor);
    }

    /**
     * Executes a loop using CompletableFutures, without invoking join()/get() on any of them or exclusively hogging a thread.
     *
     * @param condition A Supplier that indicates whether to proceed with the loop or not.
     * @param loopBody  A Supplier that returns a CompletableFuture which represents the body of the loop. This
     *                  supplier is invoked every time the loopBody needs to execute.
     * @param executor  An Executor that is used to execute the condition and the loop support code.
     * @return A CompletableFuture that, when completed, indicates the loop terminated without any exception. If
     * either the loopBody or condition throw/return Exceptions, these will be set as the result of this returned Future.
     */
    public static CompletableFuture<Void> loop(Supplier<Boolean> condition, Supplier<CompletableFuture<Void>> loopBody, Executor executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Loop<Void> loop = new Loop<>(condition, loopBody, null, result, executor);
        executor.execute(loop);
        return result;
    }

    /**
     * Executes a loop using CompletableFutures, without invoking join()/get() on any of them or exclusively hogging a thread.
     *
     * @param condition      A Supplier that indicates whether to proceed with the loop or not.
     * @param loopBody       A Supplier that returns a CompletableFuture which represents the body of the loop. This
     *                       supplier is invoked every time the loopBody needs to execute.
     * @param resultConsumer A Consumer that will be invoked with the result of every call to loopBody.
     * @param executor       An Executor that is used to execute the condition and the loop support code.
     * @param <T>            The Type of the future's result.
     * @return A CompletableFuture that, when completed, indicates the loop terminated without any exception. If
     * either the loopBody or condition throw/return Exceptions, these will be set as the result of this returned Future.
     */
    public static <T> CompletableFuture<Void> loop(Supplier<Boolean> condition, Supplier<CompletableFuture<T>> loopBody, Consumer<T> resultConsumer, Executor executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Loop<T> loop = new Loop<>(condition, loopBody, resultConsumer, result, executor);
        executor.execute(loop);
        return result;
    }

    /**
     * Executes a code fragment returning a CompletableFutures while a condition on the returned value is satisfied.
     *
     * @param condition Predicate that indicates whether to proceed with the loop or not.
     * @param loopBody  A Supplier that returns a CompletableFuture which represents the body of the loop. This
     *                  supplier is invoked every time the loopBody needs to execute.
     * @param executor  An Executor that is used to execute the condition and the loop support code.
     * @param <T>       Return type of the executor.
     * @return A CompletableFuture that, when completed, indicates the loop terminated without any exception. If
     * either the loopBody or condition throw/return Exceptions, these will be set as the result of this returned Future.
     */
    public static <T> CompletableFuture<Void> doWhileLoop(Supplier<CompletableFuture<T>> loopBody, Predicate<T> condition, Executor executor) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        // We implement the do-while loop using a regular loop, but we execute one iteration before we create the actual Loop object.
        // Since this method has slightly different arguments than loop(), we need to make one adjustment:
        // * After each iteration, we get the result and run it through 'condition' and use that to decide whether to continue.
        AtomicBoolean canContinue = new AtomicBoolean();
        Consumer<T> iterationResultHandler = ir -> canContinue.set(condition.test(ir));
        loopBody.get()
                .thenAccept(iterationResultHandler)
                .thenRunAsync(() -> {
                    Loop<T> loop = new Loop<>(canContinue::get, loopBody, iterationResultHandler, result, executor);
                    executor.execute(loop);
                }, executor)
                .exceptionally(ex -> {
                    // Handle exceptions from the first iteration.
                    result.completeExceptionally(ex);
                    return null;
                });
        return result;
    }

    //endregion

    //region Loop Implementation

    /**
     * Implements an asynchronous While Loop using CompletableFutures.
     */
    @Data
    private static class Loop<T> implements Runnable, Callable<Void> {
        /**
         * The condition to evaluate at the beginning of each loop iteration.
         */
        final Supplier<Boolean> condition;

        /**
         * A supplier that creates a CompletableFuture which will indicate the end of an iteration when complete.
         */
        final Supplier<CompletableFuture<T>> loopBody;

        /**
         * An optional Consumer that will be passed the result of each loop iteration.
         */
        final Consumer<T> resultConsumer;

        /**
         * A CompletableFuture that will be completed, whether normally or exceptionally, when the loop completes.
         */
        final CompletableFuture<Void> result;

        /**
         * An Executor to run async tasks on.
         */
        final Executor executor;

        @Override
        public Void call() throws Exception {
            if (this.condition.get()) {
                // Execute another iteration of the loop.
                this.loopBody.get()
                             .thenAccept(this::acceptIterationResult)
                             .exceptionally(this::handleException)
                             .thenRunAsync(this, this.executor);
            } else {
                // We are done; set the result and don't loop again.
                this.result.complete(null);
            }
            return null;
        }

        @Override
        public void run() {
            runOrFail(this, this.result);
        }

        private Void handleException(Throwable ex) {
            this.result.completeExceptionally(ex);
            throw new CompletionException(ex);
        }

        private void acceptIterationResult(T iterationResult) {
            if (this.resultConsumer != null) {
                this.resultConsumer.accept(iterationResult);
            }
        }
    }

    //endregion
}
