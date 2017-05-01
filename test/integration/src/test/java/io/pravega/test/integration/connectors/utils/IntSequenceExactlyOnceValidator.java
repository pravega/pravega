/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.connectors.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.junit.Assert;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * A sink function that validates that in a sequence of integers, each integer occurs
 * exactly one: The invoke() method counts elements (too many) any detects duplicates (bit set)
 * while the close() method causes an error if the function is closed too early (not all elements).
 * 
 * <p>This sink is expected to be run with parallelism one!
 */
public class IntSequenceExactlyOnceValidator extends RichSinkFunction<Integer>
        implements ListCheckpointed<Tuple2<Integer, BitSet>> {

    private final int numElementsTotal;

    private final BitSet duplicateChecker;

    private int numElementsSoFar;


    public IntSequenceExactlyOnceValidator(int numElementsTotal) {
        this.numElementsTotal = numElementsTotal;
        this.duplicateChecker = new BitSet();
    }

    // ------------------------------------------------------------------------
    //  sink
    // ------------------------------------------------------------------------

    @Override
    public void invoke(Integer value) throws Exception {
        numElementsSoFar++;
        if (numElementsSoFar > numElementsTotal) {
            Assert.fail("Received more elements than expected");
        }

        if (duplicateChecker.get(value)) {
            Assert.fail("Received a duplicate: " + value);
        }
        duplicateChecker.set(value);

        if (numElementsSoFar == numElementsTotal) {
            throw new SuccessException();
        }
    }

    @Override
    public void close() throws Exception {
        if (numElementsSoFar < numElementsTotal) {
            Assert.fail("Missing elements, only received " + numElementsSoFar);
        }
    }

    // ------------------------------------------------------------------------
    //  checkpointing
    // ------------------------------------------------------------------------

    @Override
    public List<Tuple2<Integer, BitSet>> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(new Tuple2<>(numElementsSoFar, duplicateChecker));
    }

    @Override
    public void restoreState(List<Tuple2<Integer, BitSet>> state) throws Exception {
        if (state.isEmpty()) {
            Assert.fail("Function was restored without state - no checkpoint completed before.");
        }
        
        if (state.size() > 1) {
            Assert.fail("Function was restored with multiple states. unexpected scale-in");
        }

        Tuple2<Integer, BitSet> s = state.get(0);
        this.numElementsSoFar = s.f0;
        this.duplicateChecker.clear();
        this.duplicateChecker.or(s.f1);
    }
}
