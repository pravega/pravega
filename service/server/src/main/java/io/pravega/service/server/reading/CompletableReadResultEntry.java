/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.server.reading;

import io.pravega.service.contracts.ReadResultEntry;

import java.util.function.Consumer;

/**
 * Extends the ReadResultEntry interface by adding the ability to register a callback to be invoked upon completion.
 */
interface CompletableReadResultEntry extends ReadResultEntry {
    /**
     * Registers a CompletionConsumer that will be invoked when the content is retrieved, just before the Future is completed.
     *
     * @param completionCallback The callback to be invoked.
     */
    void setCompletionCallback(CompletionConsumer completionCallback);

    /**
     * Gets the CompletionConsumer that was set using setCompletionCallback.
     */
    CompletionConsumer getCompletionCallback();

    @FunctionalInterface
    interface CompletionConsumer extends Consumer<Integer> {
    }
}
