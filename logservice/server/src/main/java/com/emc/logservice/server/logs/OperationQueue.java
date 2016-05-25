package com.emc.logservice.server.logs;

import com.emc.logservice.server.core.BlockingDrainingQueue;
import com.emc.logservice.server.logs.operations.CompletableOperation;

/**
 * A queue for Log Operations.
 */
public class OperationQueue extends BlockingDrainingQueue<CompletableOperation> {
    public OperationQueue() {
        super();
    }
}
