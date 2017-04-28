/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.connectors.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.CheckpointListener;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An identity MapFunction that calls an interface once it receives a notification
 * that a checkpoint has been completed.
 */
public class NotifyingMapper<T> implements MapFunction<T, T>, CheckpointListener {

    public static final AtomicReference<ExecuteFunction> TO_CALL_ON_CHECKPOINT_COMPLETION = new AtomicReference<>();

    @Override
    public T map(T element) throws Exception {
        return element;
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        final ExecuteFunction executeFunction = TO_CALL_ON_CHECKPOINT_COMPLETION.get();
        if (executeFunction != null) {
            executeFunction.execute();
        }
    }
}
