/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin.commands;

import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.logs.DataFrame;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.LogAddress;
import java.util.Collection;
import java.util.List;
import lombok.Cleanup;
import lombok.val;

/**
 * Executes the recovery process for a particular container, using a no-op Cache and Storage.
 */
public class ContainerRecoverCommand extends ContainerCommand {
    /**
     * Creates a new instance of the ContainerRecoverCommand.
     *
     * @param args The arguments for the command.
     */
    ContainerRecoverCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        int containerId = getIntArg(0);
        @Cleanup
        val context = createContext();
        val readIndexConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ReadIndexConfig::builder);

        // TODO: add option for fencing/non-fencing recovery. For now, it will be with fencing.

        @Cleanup
        val bkLog = context.logFactory.createDurableDataLog(containerId);

        val recoveryState = new RecoveryState();
        val callbacks = new DebugRecoveryProcessor.OperationCallbacks(
                recoveryState::newOperation,
                op -> recoveryState.operationComplete(op, null),
                recoveryState::operationComplete);

        @Cleanup
        val rp = DebugRecoveryProcessor.create(containerId, bkLog, context.containerConfig, readIndexConfig, getCommandArgs().getState().getExecutor(), callbacks);
        try {
            rp.performRecovery();
            output("Recovery complete: %d DataFrame(s) containing %d Operation(s).",
                    recoveryState.dataFrameCount, recoveryState.operationCount);
        } catch (Exception ex) {
            output("Recovery FAILED: %d DataFrame(s) containing %d Operation(s).",
                    recoveryState.dataFrameCount, recoveryState.operationCount);
            ex.printStackTrace(getOut());
            Throwable cause = Exceptions.unwrap(ex);
            if (cause instanceof DataCorruptionException) {
                unwrapDataCorruptionException((DataCorruptionException) cause);
            }
        }
    }

    private void unwrapDataCorruptionException(DataCorruptionException dce) {
        Object[] context = dce.getAdditionalData();
        if (context == null || context.length == 0) {
            return;
        }
        for (int i = 0; i < context.length; i++) {
            Object c = context[i];
            output("Debug Info %d/%d:", i + 1, context.length);
            outputDebugObject(c, 1);
        }
    }

    private void outputDebugObject(Object c, int indent) {
        String prefix = Strings.repeat("\t", indent) + c.getClass().getSimpleName();
        if (c instanceof Collection<?>) {
            Collection<?> items = (Collection<?>) c;
            output("%s(%d):", prefix, items.size());
            for (Object o : items) {
                outputDebugObject(o, indent + 1);
            }
        } else if (c instanceof DataFrame.DataFrameEntry) {
            val dfe = (DataFrame.DataFrameEntry) c;
            output("%s: Address={%s}, First/LastRecordEntry=%s/%s, LastInDF=%s, DF.Offset/Length=%d/%d.",
                    prefix, dfe.getFrameAddress(), dfe.isFirstRecordEntry(), dfe.isLastRecordEntry(),
                    dfe.isLastEntryInDataFrame(), dfe.getData().arrayOffset(), dfe.getData().getLength());
        } else if (c instanceof DataFrame) {
            val df = (DataFrame) c;
            output("%s: Address={%s}, Length=%s.", prefix, df.getAddress(), df.getLength());
        } else if (c instanceof DurableDataLog.ReadItem) {
            val ri = (DurableDataLog.ReadItem) c;
            output("%s: Address={%s}, Length=%s.", prefix, ri.getAddress(), ri.getLength());
        } else {
            output("%s: %s.", prefix, c);
        }
    }

    static CommandDescriptor descriptor() {
        return new CommandDescriptor(ContainerCommand.COMPONENT, "recover",
                "Executes a local, non-invasive recovery for a SegmentContainer.",
                new ArgDescriptor("container-id", "Id of the SegmentContainer to recover."));
    }

    private class RecoveryState {
        private Operation currentOperation;
        private LogAddress currentAddress;
        private int dataFrameCount = 0;
        private int operationCount = 0;
        private int dataFrameUsedLength = 0;
        private int dataFrameTotalLength = 0;

        private void newOperation(Operation op, List<DataFrame.DataFrameEntry> frameEntries) {
            for (int i = 0; i < frameEntries.size(); i++) {
                DataFrame.DataFrameEntry e = frameEntries.get(i);
                if (this.currentAddress == null || this.currentAddress.getSequence() != e.getFrameAddress().getSequence()) {
                    if (this.currentAddress != null) {
                        output("End DataFrame: Length=%d/%d.\n", this.dataFrameUsedLength, this.dataFrameTotalLength);
                    }

                    output("Begin DataFrame: %s.", e.getFrameAddress());
                    this.dataFrameUsedLength = 0;
                    this.dataFrameCount++;
                }

                this.currentAddress = e.getFrameAddress();
                ByteArraySegment data = e.getData();
                this.dataFrameTotalLength = data.arrayOffset() + data.getLength();
                this.dataFrameUsedLength += data.getLength();
                String split = frameEntries.size() <= 1 ? "" : String.format(",#%d/%d", i + 1, frameEntries.size());
                output("\t@[%s,%s%s]: %s.", data.arrayOffset(), data.getLength(), split, op);
            }

            this.currentOperation = op;
            this.operationCount++;
        }

        private void operationComplete(Operation op, Throwable failure) {
            if (this.currentOperation == null || this.currentOperation.getSequenceNumber() != op.getSequenceNumber()) {
                output("Operation completion mismatch. Expected '%s', found '%s'.", this.currentOperation, op);
            }

            // We don't output anything for non-failed operations.
            if (failure != null) {
                output("\tOperation '%s' FAILED recovery.", this.currentOperation);
                failure.printStackTrace(getOut());
            }
        }
    }
}
