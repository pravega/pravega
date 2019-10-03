/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.concurrent;

import com.google.common.base.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * Provides a way to run a function on Key Value Pairs but guarantees that the function is only invoked for one item at a time per key.
 * If multiple 'updates' to a particular key  are provided while the processor is running, only the most recent is passed to the
 * function.
 * This allows it to 'skip' updates for keys that are not the most recent value.
 *
 * @param <KeyType> The type of the key to be processed.
 * @param <ItemType> The type of item to be processed.
 */
public class MultiKeyLatestItemSequentialProcessor<KeyType, ItemType> {

    private final ConcurrentHashMap<KeyType, ItemType> toProcessKVP = new ConcurrentHashMap<>();
    private final BiConsumer<KeyType, ItemType> processFunction;
    private final Executor executor;

    public MultiKeyLatestItemSequentialProcessor(BiConsumer<KeyType, ItemType> processFunction, Executor executor) {
        this.processFunction = Preconditions.checkNotNull(processFunction);
        this.executor = Preconditions.checkNotNull(executor);
        
    }
    
    /**
     * Updates the item and triggers it to be processed. 
     *
     * @param key The key of the item to be processed (Cannot be null)
     * @param newItem The item to be processed. (Cannot be null)
     */
    public void updateItem(KeyType key, ItemType newItem) {
        Preconditions.checkNotNull(newItem);
        Preconditions.checkNotNull(key);
        if (toProcessKVP.put(key, newItem) == null) {
            executor.execute(() -> {
                ItemType item = newItem;
                processFunction.accept(key, item);
                while (!toProcessKVP.remove(key, item)) {
                    item = toProcessKVP.get(key);
                    processFunction.accept(key, item);
                }
            });
        }
    }
    
    

}
