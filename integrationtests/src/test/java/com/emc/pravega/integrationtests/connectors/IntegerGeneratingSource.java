/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests.connectors;

import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.List;

/**
 * A flink parallel source to generate test data and simulate different scenarios.
 */
public class IntegerGeneratingSource extends RichParallelSourceFunction<Integer> implements ListCheckpointed<Integer> {
    // The number of integers to be generated.
    private final int eventCount;

    // Whether to simulate a source failure. The source should only fail once so that the flink job can be completed
    // subsequently.
    private final boolean failOnce;

    // Offset of the previous snapshot.
    private int snapshotOffset = 0;

    // The current offset the source is generating at.
    private int currentOffset = 0;

    // Is this a recovered source instance.
    private boolean isRecovered = false;

    /**
     * Create the source instance.
     *
     * @param failOnce      Whether we need to simulate a failure.
     * @param eventCount    Number of integers in the sequence this source has to generate.
     */
    public IntegerGeneratingSource(final boolean failOnce, final int eventCount) {
        Preconditions.checkArgument(eventCount > 0);

        this.eventCount = eventCount;
        this.failOnce = failOnce;
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (this.currentOffset < this.eventCount) {
            if (this.failOnce && !this.isRecovered && this.snapshotOffset > 0 &&
                    (this.currentOffset - this.snapshotOffset) > 1) {
                throw new RuntimeException("Simulating source failure");
            }
            Thread.sleep(100);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(this.currentOffset);
                this.currentOffset++;
            }
        }
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        this.snapshotOffset = this.currentOffset;
        return Collections.singletonList(this.snapshotOffset);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        this.isRecovered = true;
        this.currentOffset = state.get(0);
    }

    @Override
    public void cancel() {
        // Source is self terminating.
    }
}
