/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.common.concurrent;

import io.pravega.common.Exceptions;
import io.pravega.common.function.CallbackHelpers;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
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
public final class FutureHelpers {

    /**
     * Waits for the provided future to be complete, and returns if it was successful, false otherwise.
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
    public static <T> void completeAfter(Supplier<CompletableFuture<T>> futureSupplier, CompletableFuture<T> toComplete) {
        Preconditions.checkArgument(!toComplete.isDone(), "toComplete is already completed.");
        try {
            CompletableFuture<T> f = futureSupplier.get();

            // Async termination.
            f.thenAccept(toComplete::complete);
            FutureHelpers.exceptionListener(f, toComplete::completeExceptionally);
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
     *                             i.e. Passing RuntimeException::new will wrap the exception in a new RuntimeException.
     *                             If null is returned from the function no exception will be thrown.
     * @param timeoutMillis        the timeout expressed in milliseconds before throwing {@link TimeoutException}
     * @param <ResultT>            Type of the result.
     * @param <ExceptionT>         Type of the Exception.
     * @throws ExceptionT       If thrown by the future.
     * @throws TimeoutException If the timeout expired prior to the future completing.
     * @return The result of calling future.get().
     */
    @SneakyThrows(InterruptedException.class)
    public static <ResultT, ExceptionT extends Exception> ResultT getAndHandleExceptions(Future<ResultT> future,
                                                                                         Function<Throwable, ExceptionT> exceptionConstructor, long timeoutMillis) throws TimeoutException, ExceptionT {
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
     * @return A CompletableFuture that fails with the given exception.
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
    public static <T> void exceptionListener(CompletableFuture<T> completableFuture, Consumer<Throwable> exceptionListener) {
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
    public static <T, E extends Throwable> void exceptionListener(CompletableFuture<T> completableFuture, Class<E> exceptionClass, Consumer<E> exceptionListener) {
        completableFuture.whenComplete((r, ex) -> {
            if (ex != null && exceptionClass.isAssignableFrom(ex.getClass())) {
                CallbackHelpers.invokeSafely(exceptionListener, (E) ex, null);
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
        return future.thenAccept(FutureHelpers::doNothing);
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

    @SneakyThrows
    private static <T, E extends Exception> Void expect(T value, T expected, Supplier<E> exceptionConstructor) {
        if (!expected.equals(value)) {
            throw exceptionConstructor.get();
        }
        return null;
    }

    private static <T> void doNothing(T ignored) {
        // This method intentionally left blank.
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
        return FutureHelpers
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
        return FutureHelpers
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
        return FutureHelpers
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
        CompletableFuture<T> result = new CompletableFuture<>();
        ScheduledFuture<Boolean> sf = executorService.schedule(() -> result.completeExceptionally(new TimeoutException(tag)), timeout.toMillis(), TimeUnit.MILLISECONDS);
        result.whenComplete((r, ex) -> sf.cancel(true));
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
     * @param future   The future to fail if the fuction fails.
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
