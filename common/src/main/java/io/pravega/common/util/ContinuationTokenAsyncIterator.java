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
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * This is a continuation token based async iterator implementation. This class takes a function that when completed will
 * have next batch of results with continuation token.
 * This class determines when to call the next iteration of function (if all existing results have been exhausted) and
 * ensures there is only one outstanding call.
 */
@ThreadSafe
@Slf4j
public class ContinuationTokenAsyncIterator<Token, T> implements AsyncIterator<T> {
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Queue<T> queue;
    @GuardedBy("lock")
    private Token token;
    private final Function<Token, CompletableFuture<Map.Entry<Token, Collection<T>>>> function;
    private CompletableFuture<Void> outstanding;
    private final AtomicBoolean canContinue;
    @GuardedBy("lock")
    private boolean isOutstanding;
    /**
     * Constructor takes a Function of token which when applied will return a tuple of new token and collection of elements
     * of type `T`.
     * This function is called whenever the local queue is empty. It is called with last received token and updates
     * the local queue of elements with the result received from the function call.
     * @param function Function of token which when applied will return a tuple of new token and collection of elements
     *                 of type `T`.
     * @param tokenIdentity Token identity which is used while making the very first function call.
     */
    public ContinuationTokenAsyncIterator(@NonNull Function<Token, CompletableFuture<Map.Entry<Token, Collection<T>>>> function,
                                          Token tokenIdentity) {
        this.function = function;
        this.token = tokenIdentity;
        this.queue = new LinkedBlockingQueue<>();
        this.outstanding = CompletableFuture.completedFuture(null);
        this.canContinue = new AtomicBoolean(true);
        this.isOutstanding = false;
    }

    @Override
    public CompletableFuture<T> getNext() {
        final Token continuationToken;
        boolean toCall = false;
        synchronized (lock) {
            // if the result is available, return it without making function call
            if (!queue.isEmpty()) {
                return CompletableFuture.completedFuture(queue.poll());
            } else {
                continuationToken = token;
                // make the function call if previous outstanding call completed.
                if (outstanding.isDone() && !isOutstanding) {
                    // only one getNext will be able to issue a new outstanding call.
                    // everyone else will see isOutstanding as `true` when they acquire the lock.
                    toCall = true;
                    isOutstanding = true;
                }
            }
        }

        if (toCall) {
            outstanding = function.apply(continuationToken)
                                  .thenAccept(resultPair -> {
                                      synchronized (lock) {
                                          if (token != null && token.equals(continuationToken)) {
                                              log.debug("Received the following collection after calling the function: {} with continuation token: {}",
                                                      resultPair.getValue(), resultPair.getKey());
                                              canContinue.set(resultPair.getValue() != null && !resultPair.getValue().isEmpty());
                                              queue.addAll(resultPair.getValue());
                                              token = resultPair.getKey();
                                              // reset isOutstanding to false because this outstanding call is complete.
                                              isOutstanding = false;
                                          }
                                      }
                                  });
        }

        return outstanding.thenCompose(v -> {
            if (canContinue.get()) {
                return getNext();
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    @VisibleForTesting
    boolean isInternalQueueEmpty() {
        synchronized (lock) {
            return queue.isEmpty();
        }
    }

    @VisibleForTesting
    Token getToken() {
        synchronized (lock) {
            return token;
        }
    }
}
