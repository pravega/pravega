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

import io.pravega.segmentstore.server.logs.DataFrame;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.LogAddress;
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

        val rp = DebugRecoveryProcessor.create(containerId, bkLog, context.containerConfig, readIndexConfig, getCommandArgs().getState().getExecutor(), callbacks);
        try {
            rp.performRecovery();
            output("Recovery complete: %d DataFrame(s) containing %d Operation(s).",
                    recoveryState.dataFrameCount, recoveryState.operationCount);
        } catch (Exception ex) {
            output("Recovery FAILED: %d DataFrame(s) containing %d Operation(s).",
                    recoveryState.dataFrameCount, recoveryState.operationCount);
            ex.printStackTrace(getOut());
        }
    }

    static CommandDescriptor descriptor() {
        return new CommandDescriptor(ContainerCommand.COMPONENT, "recover",
                "Executes a local recovery for a SegmentContainer.",
                new ArgDescriptor("container-id", "Id of the SegmentContainer to recover."));
    }

    private class RecoveryState {
        private Operation currentOperation;
        private LogAddress currentAddress;
        private int dataFrameCount = 0;
        private int operationCount = 0;
        private int currentUsedLength = 0;
        private int currentTotalLength = 0;

        private void newOperation(Operation op, List<DataFrame.DataFrameEntry> frameEntries) {
            for (int i = 0; i < frameEntries.size(); i++) {
                DataFrame.DataFrameEntry e = frameEntries.get(i);
                if (this.currentAddress == null || this.currentAddress.getSequence() != e.getFrameAddress().getSequence()) {
                    if (this.currentAddress != null) {
                        output("End DataFrame: %s; Length=%d/%d.\n", this.currentAddress,
                                this.currentUsedLength, this.currentTotalLength);
                    }

                    output("Begin DataFrame: %s.", e.getFrameAddress());
                    this.currentUsedLength = 0;
                    this.dataFrameCount++;
                }

                this.currentAddress = e.getFrameAddress();
                this.currentTotalLength = e.getData().arrayOffset() + e.getData().getLength();
                this.currentUsedLength += e.getData().getLength();
                String split = frameEntries.size() <= 1 ? "" : String.format(",#%d/%d", i + 1, frameEntries.size());
                output("\t@[%s,%s%s]: %s.", e.getData().arrayOffset(), e.getData().getLength(), split, op);
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
