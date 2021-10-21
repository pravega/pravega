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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Provides a way to run a provided function on items but guarantees that the function is only invoked for one item at a time.
 * If multiple 'updates' to the item are provided while the processor is running, only the most recent is passed to the function.
 * This allows it to 'skip' updates for items that are not the most recent value.
 * 
 * @param <ItemType> The type of item to be processed.
 */
public class LatestItemSequentialProcessor<ItemType> {
    
    private final AtomicReference<ItemType> toProcess = new AtomicReference<>();
    private final Consumer<ItemType> processFunction;
    private final Executor executor;
    
    public LatestItemSequentialProcessor(Consumer<ItemType> processFunction, Executor executor) {
        this.processFunction = Preconditions.checkNotNull(processFunction);
        this.executor = Preconditions.checkNotNull(executor);
        
    }
    
    /**
     * Updates the item and triggers it to be processed. 
     * 
     * @param newItem The item to be processed. (Cannot be null)
     */
    public void updateItem(ItemType newItem) {
        Preconditions.checkNotNull(newItem);
        if (toProcess.getAndSet(newItem) == null) {
            executor.execute(() -> {
                ItemType item = newItem;
                processFunction.accept(item);
                while (!toProcess.compareAndSet(item, null)) {
                    item = toProcess.get();
                    processFunction.accept(item);
                }
            });
        }
    }
    
    

}
