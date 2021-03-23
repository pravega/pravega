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
package io.pravega.segmentstore.server;

import com.google.common.util.concurrent.Service;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Helper methods that simplify monitoring Services.
 */
public final class ServiceListeners {
    /**
     * Awaits for the given Service to shut down, whether normally or exceptionally.
     *
     * @param service       The store to monitor.
     * @param throwIfFailed Throw the resulting exception if the store ended up in a FAILED state.
     */
    public static void awaitShutdown(Service service, boolean throwIfFailed) {
        try {
            service.awaitTerminated();
        } catch (IllegalStateException ex) {
            if (throwIfFailed || service.state() != Service.State.FAILED) {
                throw ex;
            }
        }
    }

    /**
     * Awaits for the given Service to shut down, whether normally or exceptionally.
     *
     * @param service       The store to monitor.
     * @param timeout       The maximum amount of time to wait for the shutdown.
     * @param throwIfFailed Throw the resulting exception if the store ended up in a FAILED state.
     * @throws TimeoutException If the store did not shut down within the specified amount of time. This exception is
     *                          thrown regardless of the value passed in as throwIfFailed.
     */
    public static void awaitShutdown(Service service, Duration timeout, boolean throwIfFailed) throws TimeoutException {
        try {
            service.awaitTerminated(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (IllegalStateException ex) {
            if (throwIfFailed || service.state() != Service.State.FAILED) {
                throw ex;
            }
        }
    }
}