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

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Defines the flaky behavior matching given Predicate.
 */
@Builder
@Data
@RequiredArgsConstructor
public class FlakinessPredicate {
    /**
     * Regular expression to match against chunk name.
     */
    @NonNull
    @Getter
    private final String matchRegEx;

    /**
     * Name of the method to intercept.
     */
    @NonNull
    @Getter
    private final String method;

    /**
     * Additional match predicate. The function is passed current invocation count.
     */
    @NonNull
    @Getter
    private final Function<Integer, Boolean> matchPredicate;

    /**
     * Action to take when predicate matches.
     */
    @NonNull
    @Getter
    private final Callable<Void> action;
}
