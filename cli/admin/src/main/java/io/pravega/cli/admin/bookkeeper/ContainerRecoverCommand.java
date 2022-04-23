/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.logs.DataFrame;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
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
    public ContainerRecoverCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        int containerId = getIntArg(0);
        performRecovery(containerId);
    }

    @VisibleForTesting
    public void performRecovery(int containerId) throws Exception {
        @Cleanup
        val context = createContext();
        val readIndexConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ReadIndexConfig::builder);

        // We create a special "read-only" BK log that will not be doing fencing or otherwise interfere with an active
        // container. As a result, due to the nature of BK, it is possible that it may not contain all the latest writes
        // since the Bookies may not have yet synchronized the LAC on the last (active ledger).
        @Cleanup
        val log = context.logFactory.createDebugLogWrapper(containerId);
        val bkLog = log.asReadOnly();

        val recoveryState = new RecoveryState();
        val callbacks = new DebugRecoveryProcessor.OperationCallbacks(
                recoveryState::newOperation,
                op -> true, // We want to perform the actual recovery.
                op -> recoveryState.operationComplete(op, null),
                recoveryState::operationComplete);

        @Cleanup
        val rp = DebugRecoveryProcessor.create(containerId, bkLog, context.containerConfig, readIndexConfig, getCommandArgs().getState().getExecutor(), callbacks, true);
        try {
            rp.performRecovery();
            output("Recovery complete: %d DataFrame(s) containing %d Operation(s).",
                    recoveryState.dataFrameCount, recoveryState.operationCount);
        } catch (Exception ex) {
            output("Recovery FAILED: %d DataFrame(s) containing %d Operation(s) were able to be recovered.",
                    recoveryState.dataFrameCount, recoveryState.operationCount);
            ex.printStackTrace(getOut());
            Throwable cause = Exceptions.unwrap(ex);
            if (cause instanceof DataCorruptionException) {
                unwrapDataCorruptionException((DataCorruptionException) cause);
            }
            if (throwWhenExceptionFound()) {
                throw ex;
            }
        }
    }

    @VisibleForTesting
    void unwrapDataCorruptionException(DataCorruptionException dce) {
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
        } else if (c instanceof DataFrameRecord.EntryInfo) {
            val dfe = (DataFrameRecord.EntryInfo) c;
            output("%s: Address={%s}, Length=%s, LastInDF=%s, DF.Offset/Length=%d/%d.",
                    prefix, dfe.getFrameAddress(), dfe.isLastEntryInDataFrame(), dfe.getFrameOffset(), dfe.getLength());
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

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "recover",
                "Executes a local, non-invasive recovery for a SegmentContainer.",
                new ArgDescriptor("container-id", "Id of the SegmentContainer to recover."));
    }

    protected void outputRecoveryInfo(String message, Object... args) {
        output(message, args);
    }

    protected boolean throwWhenExceptionFound() {
        return false;
    }

    //region RecoveryState

    @VisibleForTesting
    class RecoveryState {
        private Operation currentOperation;
        private int dataFrameCount = 0;
        private int operationCount = 0;
        private int currentFrameUsedLength = 0;

        @VisibleForTesting
        void newOperation(Operation op, List<DataFrameRecord.EntryInfo> frameEntries) {
            for (int i = 0; i < frameEntries.size(); i++) {
                DataFrameRecord.EntryInfo e = frameEntries.get(i);
                if (this.currentFrameUsedLength == 0) {
                    outputRecoveryInfo("Begin DataFrame: %s.", e.getFrameAddress());
                    this.dataFrameCount++;
                }

                this.currentFrameUsedLength += e.getLength();
                String split = frameEntries.size() <= 1 ? "" : String.format(",#%d/%d", i + 1, frameEntries.size());
                outputRecoveryInfo("\t@[%s,%s%s]: %s.", e.getFrameOffset(), e.getLength(), split, op);

                if (e.isLastEntryInDataFrame()) {
                    int totalLength = e.getFrameOffset() + e.getLength();
                    outputRecoveryInfo("End DataFrame: Length=%d/%d.\n", this.currentFrameUsedLength, totalLength);
                    this.currentFrameUsedLength = 0;
                }
            }

            this.currentOperation = op;
            this.operationCount++;
        }

        @VisibleForTesting
        void operationComplete(Operation op, Throwable failure) {
            if (this.currentOperation == null || this.currentOperation.getSequenceNumber() != op.getSequenceNumber()) {
                outputRecoveryInfo("Operation completion mismatch. Expected '%s', found '%s'.", this.currentOperation, op);
            }

            // We don't output anything for non-failed operations.
            if (failure != null) {
                outputRecoveryInfo("\tOperation '%s' FAILED recovery.", this.currentOperation);
                failure.printStackTrace(getOut());
            }
        }
    }

    //endregion
}
