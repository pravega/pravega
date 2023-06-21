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

import java.util.concurrent.CompletableFuture;

/**
 * A function which returns a future.
 */
@FunctionalInterface
public interface FutureSupplier<T> {

    /**
     * A function that returns a future. This may either generate async work or simply be an accessor. 
     * @return A CompletableFuture
     */
    CompletableFuture<T> getFuture();
    
}
