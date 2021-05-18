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
import lombok.Getter;
import lombok.NonNull;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A details object encapsulates any health related, non-{@link Status} state that an operator may be interested in. A details
 * object should provide information that helps gauge the well-being of the associated component.
 */
@ThreadSafe
public class DetailsProvider {
    /**
     * The underlying {@link java.util.Collection} used to hold the detailed information.
     */
    @Getter
    private final Map<String, Supplier<Object>> suppliers = new HashMap<>();

    /**
     * Add some piece of information to be retrieved everytime a health check (by some {@link HealthContributor} occurs.
     *
     * @param key The key to associate the value with.
     * @param supplier A supplier used to return the value to be associated with 'key'.
     * @return Itself with the additions applied.
     */
    @NonNull
    public DetailsProvider add(String key, Supplier<Object> supplier) {
        this.suppliers.put(key, supplier);
        return this;
    }

    /**
     * Fetches and aggregates the results of all the added {@link Supplier} objects.
     *
     * @return An {@link ImmutableMap} of results.
     */
    public ImmutableMap<String, Object> fetch() {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        suppliers.forEach((key, val) -> builder.put(key, val.get()));
        return builder.build();
    }

}
