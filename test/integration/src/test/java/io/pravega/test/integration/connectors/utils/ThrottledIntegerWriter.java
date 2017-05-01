/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.connectors.utils;

import io.pravega.stream.EventStreamWriter;
import org.apache.flink.core.testutils.CheckedThread;

/**
 * A data generator thread that generates an ordered sequence of integers.
 * 
 * <p>The thread starts throttled (sleeping a certain time per emitted element)
 * and will eventually block and not continue emitting further elements until
 * is is un-throttled. The purpose of that is to make sure that certain actions
 * can happen before all elements have been produced.
 */
public class ThrottledIntegerWriter extends CheckedThread implements AutoCloseable {

    private final EventStreamWriter<Integer> eventWriter;

    private final int numValues;

    private final int blockAtNum;

    private final int sleepPerElement;

    private final Object blocker = new Object();

    private volatile boolean throttled;

    private volatile boolean running;
    
    public ThrottledIntegerWriter(EventStreamWriter<Integer> eventWriter,
                                  int numValues, int blockAtNum, int sleepPerElement) {
        
        super("ThrottledIntegerWriter");

        this.eventWriter = eventWriter;
        this.numValues = numValues;
        this.blockAtNum = blockAtNum;
        this.sleepPerElement = sleepPerElement;
        
        this.running = true;
        this.throttled = true;
    }

    @Override
    public void go() throws Exception {
        // emit the sequence of values
        for (int i = 0; running && i < numValues; i++) {

            // throttle speed if still requested
            // if we reach the 'blockAtNum' element before being un-throttled,
            // we need to wait until we are un-throttled
            if (throttled) {
                if (i < blockAtNum) {
                    Thread.sleep(sleepPerElement);
                } else {
                    synchronized (blocker) {
                        while (running && throttled) {
                            blocker.wait();
                        }
                    }
                }
            }

            eventWriter.writeEvent(String.valueOf(i), i);
        }
    }

    public void unthrottle() {
        synchronized (blocker) {
            throttled = false;
            blocker.notifyAll();
        }
    }

    @Override
    public void close() {
        this.running = false;
        interrupt();
    }
}
