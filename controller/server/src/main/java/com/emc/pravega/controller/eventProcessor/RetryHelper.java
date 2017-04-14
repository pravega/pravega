/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;

import java.util.function.Predicate;
import java.util.function.Supplier;

public class RetryHelper {

    public static final Predicate<Throwable> CONNECTIVITY_PREDICATE = e -> {
        Throwable t = ExceptionHelpers.getRealException(e);
        return t instanceof CheckpointStoreException &&
                ((CheckpointStoreException) t).getType().equals(CheckpointStoreException.Type.Connectivity);
    };

    public static final Retry.RetryWithBackoff RETRY =
            Retry.withExpBackoff(100, 2, Integer.MAX_VALUE, 1000);

    public static <U> U withRetries(Supplier<U> futureSupplier, Predicate<Throwable> predicate) {
        return RETRY
                .retryWhen(predicate)
                .throwingOn(RuntimeException.class)
                .run(futureSupplier::get);
    }
}
