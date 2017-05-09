/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.IOException;

/**
 * The various types of commands that can be sent over the wire.
 * Each has two fields the first is a code that identifies it in the wire protocol. (This is the first thing written)
 * The second is a constructor method, that is used to decode commands of that type.
 * 
 * (Types below that are grouped into pairs where there is a corresponding request and reply.)
 */
public enum WireCommandType {
    HELLO(-127, WireCommands.Hello::readFrom),
    
    PADDING(-1, WireCommands.Padding::readFrom),

    PARTIAL_EVENT(-2, WireCommands.PartialEvent::readFrom),

    EVENT(0, null), // Is read manually.

    SETUP_APPEND(1, WireCommands.SetupAppend::readFrom),
    APPEND_SETUP(2, WireCommands.AppendSetup::readFrom),

    APPEND_BLOCK(3, WireCommands.AppendBlock::readFrom),
    APPEND_BLOCK_END(4, WireCommands.AppendBlockEnd::readFrom),
    CONDITIONAL_APPEND(5, WireCommands.ConditionalAppend::readFrom),

    DATA_APPENDED(7, WireCommands.DataAppended::readFrom),
    CONDITIONAL_CHECK_FAILED(8, WireCommands.ConditionalCheckFailed::readFrom),

    READ_SEGMENT(9, WireCommands.ReadSegment::readFrom),
    SEGMENT_READ(10, WireCommands.SegmentRead::readFrom),

    GET_STREAM_SEGMENT_INFO(11, WireCommands.GetStreamSegmentInfo::readFrom),
    STREAM_SEGMENT_INFO(12, WireCommands.StreamSegmentInfo::readFrom),
    
    GET_TRANSACTION_INFO(13, WireCommands.GetTransactionInfo::readFrom),
    TRANSACTION_INFO(14, WireCommands.TransactionInfo::readFrom),

    CREATE_SEGMENT(20, WireCommands.CreateSegment::readFrom),
    SEGMENT_CREATED(21, WireCommands.SegmentCreated::readFrom),

    CREATE_TRANSACTION(22, WireCommands.CreateTransaction::readFrom),
    TRANSACTION_CREATED(23, WireCommands.TransactionCreated::readFrom),

    COMMIT_TRANSACTION(24, WireCommands.CommitTransaction::readFrom),
    TRANSACTION_COMMITTED(25, WireCommands.TransactionCommitted::readFrom),

    ABORT_TRANSACTION(26, WireCommands.AbortTransaction::readFrom),
    TRANSACTION_ABORTED(27, WireCommands.TransactionAborted::readFrom),
    
    SEAL_SEGMENT(28, WireCommands.SealSegment::readFrom),
    SEGMENT_SEALED(29, WireCommands.SegmentSealed::readFrom),

    DELETE_SEGMENT(30, WireCommands.DeleteSegment::readFrom),
    SEGMENT_DELETED(31, WireCommands.SegmentDeleted::readFrom),

    UPDATE_SEGMENT_POLICY(32, WireCommands.UpdateSegmentPolicy::readFrom),
    SEGMENT_POLICY_UPDATED(33, WireCommands.SegmentPolicyUpdated::readFrom),

    WRONG_HOST(50, WireCommands.WrongHost::readFrom),
    SEGMENT_IS_SEALED(51, WireCommands.SegmentIsSealed::readFrom),
    SEGMENT_ALREADY_EXISTS(52, WireCommands.SegmentAlreadyExists::readFrom),
    NO_SUCH_SEGMENT(53, WireCommands.NoSuchSegment::readFrom),
    NO_SUCH_TRANSACTION(54, WireCommands.NoSuchTransaction::readFrom),

    KEEP_ALIVE(100, WireCommands.KeepAlive::readFrom);

    private final int code;
    private final WireCommands.Constructor factory;

    WireCommandType(int code, WireCommands.Constructor factory) {
        Preconditions.checkArgument(code <= 127 && code >= -127, "All codes should fit in a byte.");
        this.code = code;
        this.factory = factory;
    }

    public int getCode() {
        return code;
    }

    public WireCommand readFrom(DataInput in, int length) throws IOException {
        return factory.readFrom(in, length);
    }
}