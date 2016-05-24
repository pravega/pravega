package com.emc.logservice.logs;

import com.emc.logservice.Container;
import com.emc.logservice.logs.operations.Operation;

/**
 * Defines a Sequential Log made of Log Operations.
 */
public interface OperationLog extends SequentialLog<Operation, Long>, Container {

}

