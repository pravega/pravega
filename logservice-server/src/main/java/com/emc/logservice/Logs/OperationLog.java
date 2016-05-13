package com.emc.logservice.Logs;

import com.emc.logservice.Logs.Operations.Operation;

/**
 * Defines a Sequential Log made of Log Operations.
 */
public interface OperationLog extends SequentialLog<Operation, Long>
{

}

