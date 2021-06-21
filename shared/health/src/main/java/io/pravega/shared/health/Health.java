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
package io.pravega.shared.health;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;


import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;

/**
 * The {@link Health} class represents the data gathered by a {@link HealthContributor} after performing a health check.
 */
@Builder
@ThreadSafe
@AllArgsConstructor
public class Health {

    /**
     * Any sort of identifying string that describes from which component this measurement
     * was taken from.
     */
    @Getter
    private final String name;

    @Getter
    @Builder.Default
    private final Status status = Status.UNKNOWN;

    @Getter
    @Builder.Default
    private final Map<String, Object> details = ImmutableMap.of();

    /**
     * The {@link Health} results of the children belonging to a {@link HealthContributor}.
     */
    @Getter
    @Builder.Default
    private final Map<String, Health> children = ImmutableMap.of();

    /**
     * Used to perform readiness checks. It determines if the {@link Health} object holds a {@link Status} that is considered 'ready'.
     * Logically a component should be considered 'ready' if it has completed it's initialization step(s) and is ready to execute.

     * @return The readiness result.
     */
    public boolean isReady() {
        return status.isReady();
    }

    /**
     * Used to perform liveness checks. It determines if the {@link Health} object holds a {@link Status} that is considered 'alive'.
     * A component that is 'ready' implies that it is 'alive', but not vice versa.
     *
     * @return The liveness result.
     */
    public boolean isAlive() {
        return status.isAlive();
    }
}
