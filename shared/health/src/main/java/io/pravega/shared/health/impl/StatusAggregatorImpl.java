/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health.impl;

import java.util.Collection;
import java.util.function.Function;

import io.pravega.shared.health.Status;
import io.pravega.shared.health.StatusAggregator;
import io.pravega.shared.health.StatusAggregationRule;

public class StatusAggregatorImpl implements StatusAggregator {

    /**
     * Publicizes a {@link StatusAggregator} using the '{@link StatusAggregationRule#majority(Collection)}' rule.
     */
    public static final StatusAggregator MAJORITY = new StatusAggregatorImpl(StatusAggregationRule::majority);

    /**
     * Publicizes a {@link StatusAggregator} using the '{@link StatusAggregationRule#unanimous(Collection)}' rule.
     */
    public static final StatusAggregator UNANIMOUS = new StatusAggregatorImpl(StatusAggregationRule::unanimous);

    /**
     * Publicizes a {@link StatusAggregator} using the '{@link StatusAggregationRule#any(Collection)}' rule.
     */
    public static final StatusAggregator ANY = new StatusAggregatorImpl(StatusAggregationRule::any);

    /**
     * The default {@link StatusAggregator} to be used by.
     */
    public static final StatusAggregator DEFAULT = UNANIMOUS;

    /**
     * The underlying aggregation rule.
     */
    private final Function<Collection<Status>, Status> rule;

    /**
     * In the case where a custom aggregation rule is required, any lambda function reducing a {@link Collection} of
     * {@link Status} to a single {@link Status} may be supplied.
     * @param rule The rule used to perform the aggregation.
     */
    StatusAggregatorImpl(Function<Collection<Status>, Status> rule) {
        this.rule = rule;
    }

    public Status aggregate(Collection<Status> statuses) {
        return this.rule.apply(statuses);
    }
}
