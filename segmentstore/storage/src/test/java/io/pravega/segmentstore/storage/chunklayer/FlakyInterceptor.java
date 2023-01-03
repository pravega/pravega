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
package io.pravega.segmentstore.storage.chunklayer;

import lombok.Getter;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Provides mechanism for implementation that fails predictably based on provided list of {@link FlakinessPredicate}.
 */
public class FlakyInterceptor {
    /**
     * Predicate to evaluate against each invocation.
     */
    @Getter
    private final ArrayList<FlakinessPredicate> flakyPredicates = new ArrayList<>();

    /**
     * Keeps track of invocations.
     */
    private final HashMap<String, Integer> invokeCounts = new HashMap<>();

    FlakinessPredicate getMatchingPredicate(String resourceName, String method, int invocationCount) {
        return flakyPredicates.stream()
                .filter(predicate -> predicate.getMethod().equals(method)
                        && resourceName.contains(predicate.getMatchRegEx())
                        && predicate.getMatchPredicate().apply(invocationCount))
                .findFirst()
                .orElse(null);
    }

    public void intercept(String resourceName, String method) throws ChunkStorageException {
        int invocationCount = 0;
        if (!invokeCounts.containsKey(method)) {
            invokeCounts.put(method, 0);
        }
        invocationCount = invokeCounts.get(method);
        invokeCounts.put(method, invocationCount + 1);
        val predicate = getMatchingPredicate(resourceName, method, invocationCount);
        if (null != predicate) {
            try {
                predicate.getAction().call();
            } catch (ChunkStorageException e) {
                throw e;
            } catch (Exception e) {
                throw new ChunkStorageException(resourceName, "Intentional Failure", e);
            }
        }
    }
}
