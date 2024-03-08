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
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.lang.Int96;

import java.util.concurrent.CompletableFuture;

public interface Int96Counter {
    /**
     * This constant defines the size of the block of counter values that will be used by this controller instance.
     * The controller will try to get current counter value from zookeeper/PravegaTables. It then tries to update the value in store
     * by incrementing it by COUNTER_RANGE. If it is able to update the new value successfully, then this controller
     * can safely use the block `previous-value-in-store + 1` to `previous-value-in-store + COUNTER_RANGE` No other controller
     * will use this range for transaction id generation as it will be unique assigned to current controller.
     * If controller crashes, all unused values go to waste. In worst case we may lose COUNTER_RANGE worth of values everytime
     * a controller crashes.
     */
    @VisibleForTesting
    int COUNTER_RANGE = 10000;

    /**
     * Method to get next Int96 value.
     * @param context Operation context.
     * @return Future, which when complete will return next Int96 value.
     */
    CompletableFuture<Int96> getNextCounter(final OperationContext context);
}