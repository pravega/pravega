package com.emc.logservice.logs;

import com.emc.logservice.core.BlockingDrainingQueue;
import com.emc.logservice.logs.operations.CompletableOperation;

/**
 * A queue for Log Operations.
 */
public class OperationQueue extends BlockingDrainingQueue<CompletableOperation> {
    public OperationQueue() {
        super();
    }
}
