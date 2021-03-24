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
import com.google.common.util.concurrent.Service;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * Helper methods that allow controlling Services.
 */
public final class Services {
    /**
     * Asynchronously starts a Service and returns a CompletableFuture that will indicate when it is running.
     *
     * @param service  The Service to start.
     * @param executor An Executor to use for callback invocations.
     * @return A CompletableFuture that will be completed when the service enters a RUNNING state, or completed
     * exceptionally if the service failed to start.
     */
    public static CompletableFuture<Void> startAsync(Service service, Executor executor) {
        // Service.startAsync() will fail if the service is not in a NEW state. That is, if it is already RUNNING or
        // STARTED, then the method will fail synchronously, hence we are not in danger of not invoking our callbacks,
        // as long as we register the Listener before we attempt to start.
        // Nevertheless, do make a sanity check since once added, a Listener cannot be removed.
        Preconditions.checkState(service.state() == Service.State.NEW,
                "Service expected to be %s but was %s.", Service.State.NEW, service.state());
        Preconditions.checkNotNull(executor, "executor");
        CompletableFuture<Void> result = new CompletableFuture<>();
        service.addListener(new StartupListener(result), executor);
        service.startAsync();
        return result;
    }

    /**
     * Asynchronously stops a Service and returns a CompletableFuture that will indicate when it is stopped.
     *
     * @param service  The Service to stop.
     * @param executor An Executor to use for callback invocations.
     * @return A CompletableFuture that will be completed when the service enters a TERMINATED state, or completed
     * exceptionally if the service enters a FAILED state.
     */
    public static CompletableFuture<Void> stopAsync(Service service, Executor executor) {
        // Service.stopAsync() will not throw any exceptions, but will transition the Service to either TERMINATED
        // or FAILED. We need to register the listener before we attempt to stop.
        CompletableFuture<Void> result = new CompletableFuture<>();
        onStop(service, () -> result.complete(null), result::completeExceptionally, executor);
        service.stopAsync();
        return result;
    }

    /**
     * Attaches the given callbacks which will be invoked when the given Service enters a TERMINATED or FAILED state.
     * The callbacks are optional and may be invoked synchronously if the Service is already in one of these states.
     *
     * @param service            The Service to attach to.
     * @param terminatedCallback (Optional) A Runnable that will be invoked if the Service enters a TERMINATED state.
     * @param failureCallback    (Optional) A Runnable that will be invoked if the Service enters a FAILED state.
     * @param executor           An Executor to use for callback invocations.
     */
    public static void onStop(Service service, Runnable terminatedCallback, Consumer<Throwable> failureCallback, Executor executor) {
        ShutdownListener listener = new ShutdownListener(terminatedCallback, failureCallback);
        service.addListener(listener, executor);

        // addListener() will not invoke the callbacks if the service is already in a terminal state. As such, we need to
        // manually check for these states after registering the listener and invoke the appropriate callback. The
        // ShutdownListener will make sure they are not invoked multiple times.
        Service.State state = service.state();
        if (state == Service.State.FAILED) {
            // We don't care (or know) the state from which we came, so we just pass some random one.
            listener.failed(Service.State.FAILED, service.failureCause());
        } else if (state == Service.State.TERMINATED) {
            listener.terminated(Service.State.TERMINATED);
        }
    }

    /**
     * Determines whether the given Service.State indicates the Service is either in the process of Stopping or already
     * Terminated or Failed.
     *
     * @param state The Service.State to test.
     * @return Whether this is a terminating state.
     */
    public static boolean isTerminating(Service.State state) {
        return state == Service.State.STOPPING
                || state == Service.State.TERMINATED
                || state == Service.State.FAILED;
    }


    //region ShutdownListener

    @RequiredArgsConstructor
    private static class ShutdownListener extends Service.Listener {
        private final Runnable terminatedCallback;
        private final Consumer<Throwable> failureCallback;
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        @Override
        public void terminated(@Nonnull Service.State from) {
            if (!this.invoked.compareAndSet(false, true)) {
                // Already invoked once. Don't double-call.
                return;
            }

            if (this.terminatedCallback != null) {
                this.terminatedCallback.run();
            }
        }

        @Override
        public void failed(@Nonnull Service.State from, @Nonnull Throwable failure) {
            if (!this.invoked.compareAndSet(false, true)) {
                // Already invoked once. Don't double-call.
                return;
            }

            if (this.failureCallback != null) {
                this.failureCallback.accept(failure);
            }
        }
    }

    //endregion

    //region StartupListener

    @RequiredArgsConstructor
    private static class StartupListener extends Service.Listener {
        private final CompletableFuture<Void> completion;

        @Override
        public void running() {
            this.completion.complete(null);
        }

        @Override
        public void terminated(@Nonnull Service.State from) {
            this.completion.completeExceptionally(new IllegalStateException(
                    String.format("Service expected to be %s but was %s.", Service.State.RUNNING, Service.State.TERMINATED)));
        }

        @Override
        public void failed(@Nonnull Service.State from, @Nonnull Throwable failure) {
            this.completion.completeExceptionally(failure);
        }
    }

    //endregion
}
