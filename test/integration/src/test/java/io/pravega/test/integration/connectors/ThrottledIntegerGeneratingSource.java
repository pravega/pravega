/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.connectors;

import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * A Flink source that generates integers, but slows down until the first checkpoint has been completed.
 */
public class ThrottledIntegerGeneratingSource 
        extends RichParallelSourceFunction<Integer>
        implements ListCheckpointed<Integer>, CheckpointListener {

    /** Blocker when the generator needs to wait for the checkpoint to happen.
     * Eager initialization means it must be serializable */ 
    private final Object blocker = new Serializable() {};

    /** The total number of events to generate */
    private final int numEventsTotal;

    private final int latestPosForCheckpoint;

    /** The current position in the sequence of numbers */
    private int currentPosition = -1;

    private long lastCheckpointTriggered;

    private long lastCheckpointConfirmed;

    /** Flag to cancel the source. Must be volatile, because modified asynchronously */
    private volatile boolean running = true;


    public ThrottledIntegerGeneratingSource(final int numEventsTotal) {
        this(numEventsTotal, numEventsTotal / 2);
    }

    public ThrottledIntegerGeneratingSource(final int numEventsTotal, final int latestPosForCheckpoint) {
        Preconditions.checkArgument(numEventsTotal > 0);
        Preconditions.checkArgument(latestPosForCheckpoint >= 0 && latestPosForCheckpoint < numEventsTotal);

        this.numEventsTotal = numEventsTotal;
        this.latestPosForCheckpoint = latestPosForCheckpoint;
    }

    // ------------------------------------------------------------------------
    //  source
    // ------------------------------------------------------------------------

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {

        // each source subtask emits only the numbers where (num % parallelism == subtask_index)
        final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
        int current = this.currentPosition >= 0 ? this.currentPosition : getRuntimeContext().getIndexOfThisSubtask();

        while (this.running && current < this.numEventsTotal) {

            // throttle if no checkpoint happened so far
            if (this.lastCheckpointConfirmed < 1) {
                if (current < this.latestPosForCheckpoint) {
                    Thread.sleep(1);
                } else {
                    synchronized (blocker) {
                        while (this.running && this.lastCheckpointConfirmed < 1) {
                            blocker.wait();
                        }
                    }
                }
            }

            // emit the next element
            current += stepSize;
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(current);
                this.currentPosition = current;
            }
        }

        // after we are done, we need to wait for two more checkpoint to complete
        // before finishing the program - that is to be on the safe side that
        // the sink also got the "commit" notification for all relevant checkpoints
        // and committed the data to pravega

        // note: this indicates that to handle finite jobs with 2PC outputs more
        // easily, we need a primitive like "finish-with-checkpoint" in Flink

        final long lastCheckpoint;
        synchronized (ctx.getCheckpointLock()) {
            lastCheckpoint = this.lastCheckpointTriggered;
        }

        synchronized (this.blocker) {
            while (this.lastCheckpointConfirmed <= lastCheckpoint + 1) {
                this.blocker.wait();
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    // ------------------------------------------------------------------------
    //  snapshots
    // ------------------------------------------------------------------------

    @Override
    public List<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        this.lastCheckpointTriggered = checkpointId;

        return Collections.singletonList(this.currentPosition);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        this.currentPosition = state.get(0);

        // at least one checkpoint must have happened so fat
        this.lastCheckpointTriggered = 1L;
        this.lastCheckpointConfirmed = 1L;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        synchronized (blocker) {
            this.lastCheckpointConfirmed = checkpointId;
            blocker.notifyAll();
        }
    }
}
