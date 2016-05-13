package com.emc.logservice.Logs;

import com.emc.logservice.Core.BlockingDrainingQueue;
import com.emc.logservice.Logs.Operations.CompletableOperation;

/**
 * A queue for Log Operations.
 */
public class OperationQueue extends BlockingDrainingQueue<CompletableOperation> {
    public OperationQueue() {
        super();
    }
}
