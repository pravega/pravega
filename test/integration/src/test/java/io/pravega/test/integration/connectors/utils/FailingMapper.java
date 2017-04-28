/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.connectors.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.List;

/**
 * An identity mapper that throws an exception at a specified element.
 * The exception is only thrown during the first execution, prior to the first recovery.
 */
public class FailingMapper<T> implements MapFunction<T, T>, ListCheckpointed<Integer> {

    private final int failAtElement;

    private int elementCount;
    private boolean restored;

    public FailingMapper(int failAtElement) {
        this.failAtElement = failAtElement;
    }

    @Override
    public T map(T element) throws Exception {
        if (!restored && ++elementCount > failAtElement) {
            throw new Exception("artificial failure");
        }

        return element;
    }

    @Override
    public void restoreState(List<Integer> list) throws Exception {
        restored = true;
    }

    @Override
    public List<Integer> snapshotState(long l, long l1) throws Exception {
        return null;
    }
}
