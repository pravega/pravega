/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import lombok.Data;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A details object encapsulates any health related, non-{@link Status} state that an operator may be interested in. A details
 * object should provide information that helps gauge the well-being of the associated component.
 *
 * This interface is to be deserialized into JSON.
 */
public class Details {
    /**
     * The underlying {@link java.util.Collection} used to hold detailed information.
     */
    @Getter
    private Map<String, Supplier<Object>> details = new HashMap<String, Supplier<Object>>();

    /**
     * Add some piece of information to be retrieved everytime a health check (by some {@link HealthIndicator} occurs.
     *
     * @param key The key to associate the value with.
     * @param supplier A supplier used to return the value to be associated with 'key'.
     */
    Details add(String key, Supplier<Object> supplier) {
        this.details.put(key, supplier);
        return this;
    }

    /**
     * Fetches and aggregates the results of all the added {@link Supplier} objects.
     *
     * @return The {@link Result} list.
     */
    List<Result> fetch() {
        return getDetails()
                .entrySet()
                .stream()
                .map(entry -> new Result(entry.getKey(), entry.getValue().get().toString()))
                .collect(Collectors.toList());
    }

    @Data
    public static class Result {
        final String key;
        final String value;
    }
}
