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
package io.pravega.cli.admin.dataRecovery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DataFrameBuilder;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.DebugBookKeeperLogWrapper;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * This command provides an administrator with the basic primitives to manipulate a DurableLog with damaged entries.
 * The workflow of this command is as follows:
 * 1. Checks if the Original Log is disabled (exit otherwise).
 * 2. Reads the original damaged DurableLog and creates a backup copy of it for safety reasons.
 * 3. Validate and buffer all the edits from admin to be done on the original DurableLog data (i.e., skip, delete,
 * replace operations). All these changes are then written on a Repair Log (i.e., original DurableLog data + admin changes).
 * 4. With the desired updates written in the Repair Log, the admin can replace the original DurableLog metadata by the
 * Repair Log's one. This will make the DurableLog for the Segment Container under repair to point to the Repair Log data.
 * 5. The backed-up data for the originally damaged DurableLog can be reused to create a new Repair Log or discarded if
 * the Segment Container recovers as expected.
 */
public class DurableDataLogRepairCommand extends DataRecoveryCommand {

    private final static Duration TIMEOUT = Duration.ofSeconds(10);

    /**
     * Creates a new instance of the DurableLogRepairCommand class.
     *
     * @param args The arguments for the command.
     */
    public DurableDataLogRepairCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        int containerId = getIntArg(0);
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);
        @Cleanup
        val zkClient = createZKClient();
        @Cleanup
        DurableDataLogFactory dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, getCommandArgs().getState().getExecutor());
        dataLogFactory.initialize();

        // Open the Original Log in read-only mode.
        @Cleanup
        val originalDataLog = dataLogFactory.createDebugLogWrapper(containerId);

        // Check if the Original Log is disabled.
        var origMetadata = originalDataLog.fetchMetadata();
        if (origMetadata.isEnabled()) {
            output("Original DurableLog is enabled. Repairs can only be done on disabled logs, exiting.");
            return;
        }

        // Make sure that the reserved id for Backup log is free before making any further progress.
        boolean createNewBackupLog = true;
        if (existsBackupLog(dataLogFactory)) {
            output("We found data in the Backup log, probably from a previous repair operation (or someone else running the same command at the same time). " +
                    "You have three options: 1) Delete existing Backup Log and start a new repair process, " +
                    "2) Keep existing Backup Log and re-use it for the current repair (i.e., skip creating a new Backup Log), " +
                    "3) Quit.");
            switch (getIntUserInput("Select an option: [1|2|3]")) {
                case 1:
                    // Delete everything related to the old Backup Log.
                    try (DebugDurableDataLogWrapper backupDataLogDebugLogWrapper = dataLogFactory.createDebugLogWrapper(dataLogFactory.getBackupLogId())) {
                        backupDataLogDebugLogWrapper.deleteDurableLogMetadata();
                    }
                    break;
                case 2:
                    // Keeping existing Backup Log, so not creating a new one.
                    createNewBackupLog = false;
                    break;
                default:
                    output("Not doing anything with existing Backup Log this time.");
                    return;
            }
        }

        // Create a new Backup Log if there wasn't any or if we removed the existing one.
        if (createNewBackupLog) {
            createBackupLog(dataLogFactory, containerId, originalDataLog);
        }
        int backupLogReadOperations = validateBackupLog(dataLogFactory, containerId, originalDataLog, createNewBackupLog);

        // Get user input of operations to skip, replace, or delete.
        List<LogEditOperation> durableLogEdits = getDurableLogEditsFromUser();
        // Show the edits to be committed to the original durable log so the user can confirm.
        output("The following edits will be used to edit the Original Log:");
        durableLogEdits.forEach(e -> output(e.toString()));

        output("Original DurableLog has been backed up correctly. Ready to apply admin-provided changes to the Original Log.");
        if (!confirmContinue()) {
            output("Not editing Original DurableLog this time. A Backup Log has been left during the process and you " +
                    "will find it the next time this command gets executed.");
            return;
        }

        // Ensure that the Repair Log is going to start from a clean state.
        output("Deleting existing medatadata from Repair Log (if any)");
        try (val editedLogWrapper = dataLogFactory.createDebugLogWrapper(dataLogFactory.getRepairLogId())) {
            editedLogWrapper.deleteDurableLogMetadata();
        } catch (DurableDataLogException e) {
            if (e.getCause() instanceof KeeperException.NoNodeException) {
                output("Repair Log does not exist, so nothing to delete.");
            } else {
                outputError("Error happened while attempting to cleanup Repair Log metadata.");
                outputException(e);
            }
        }

        // Create a new Repair log to store the result of edits applied to the Original Log and instantiate the processor
        // that will write the edited contents into the Repair Log.
        try (DurableDataLog editedDataLog = dataLogFactory.createDurableDataLog(dataLogFactory.getRepairLogId());
             EditingLogProcessor logEditState = new EditingLogProcessor(editedDataLog, durableLogEdits, getCommandArgs().getState().getExecutor());
             DurableDataLog backupDataLog = dataLogFactory.createDebugLogWrapper(dataLogFactory.getBackupLogId()).asReadOnly()) {
            editedDataLog.initialize(TIMEOUT);
            readDurableDataLogWithCustomCallback(logEditState, dataLogFactory.getBackupLogId(), backupDataLog);
            Preconditions.checkState(!logEditState.isFailed);
            // After the edition has completed, we need to disable it before the metadata overwrite.
            editedDataLog.disable();
        } catch (Exception ex) {
            outputError("There have been errors while creating the edited version of the DurableLog.");
            outputException(ex);
            throw ex;
        }

        // Validate the contents of the newly created Repair Log.
        int editedDurableLogOperations = validateRepairLog(dataLogFactory, backupLogReadOperations, durableLogEdits);

        // Overwrite the original DurableLog metadata with the edited DurableLog metadata.
        try (val editedLogWrapper = dataLogFactory.createDebugLogWrapper(dataLogFactory.getRepairLogId())) {
            output("Original DurableLog Metadata: " + originalDataLog.fetchMetadata());
            output("Edited DurableLog Metadata: " + editedLogWrapper.fetchMetadata());
            long origEpoch = origMetadata.getEpoch();
            originalDataLog.forceMetadataOverWrite(editedLogWrapper.fetchMetadata());
            originalDataLog.overrideEpochInMetadata(origEpoch);
            output("New Original DurableLog Metadata (after replacement): " + originalDataLog.fetchMetadata());
        }

        // Read the edited contents that are now reachable from the original log id.
        try (val editedLogWrapper = dataLogFactory.createDebugLogWrapper(dataLogFactory.getRepairLogId())) {
            int finalEditedLogReadOps = readDurableDataLogWithCustomCallback((op, list) ->
                    output("Original Log Operations after repair: " + op), containerId, editedLogWrapper.asReadOnly());
            output("Original DurableLog operations read (after editing): " + finalEditedLogReadOps);
            Preconditions.checkState(editedDurableLogOperations == finalEditedLogReadOps, "Repair Log operations not matching before (" +
                    editedDurableLogOperations + ") and after the metadata overwrite (" + finalEditedLogReadOps + ")");
        } catch (Exception ex) {
            outputError("Problem reading Original DurableLog after editing.");
            outputException(ex);
        }

        output("Process completed successfully! (You still need to enable the Durable Log so Pravega can use it)");
    }

    private int validateRepairLog(DurableDataLogFactory dataLogFactory, int backupLogReadOperations, List<LogEditOperation> durableLogEdits) throws Exception {
        @Cleanup
        DurableDataLog editedDebugDataLogReadOnly = dataLogFactory.createDebugLogWrapper(dataLogFactory.getRepairLogId()).asReadOnly();
        int editedDurableLogOperations = readDurableDataLogWithCustomCallback((op, list) ->
                output("Repair Log Operations: " + op), dataLogFactory.getRepairLogId(), editedDebugDataLogReadOnly);
        output("Edited DurableLog Operations read: " + editedDurableLogOperations);
        long expectedEditedLogOperations = backupLogReadOperations +
                durableLogEdits.stream().filter(edit -> edit.type.equals(LogEditType.ADD_OPERATION)).count() -
                durableLogEdits.stream().filter(edit -> edit.type.equals(LogEditType.DELETE_OPERATION))
                        .map(edit -> edit.finalOperationId - edit.initialOperationId).reduce(Long::sum).orElse(0L);
        Preconditions.checkState( expectedEditedLogOperations == editedDurableLogOperations, "Expected (" + expectedEditedLogOperations +
                ") and actual (" + editedDurableLogOperations + ") operations in Edited Log do not match");
        return editedDurableLogOperations;
    }

    private int validateBackupLog(DurableDataLogFactory dataLogFactory, int containerId, DebugDurableDataLogWrapper originalDataLog,
                                    boolean createNewBackupLog) throws Exception {
        // Validate that the Original and Backup logs have the same number of operations.
        int operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback((a, b) -> { }, containerId, originalDataLog.asReadOnly());
        @Cleanup
        val validationBackupDataLog = dataLogFactory.createDebugLogWrapper(dataLogFactory.getBackupLogId());
        @Cleanup
        val validationBackupDataLogReadOnly = validationBackupDataLog.asReadOnly();
        int backupLogReadOperations = readDurableDataLogWithCustomCallback((a, b) ->
                output("Reading: " + a), dataLogFactory.getBackupLogId(), validationBackupDataLogReadOnly);
        output("Original DurableLog operations read: " + operationsReadFromOriginalLog +
                ", Backup DurableLog operations read: " + backupLogReadOperations);

        // Ensure that the Original log contains the same number of Operations than the Backup log upon a new log read.
        Preconditions.checkState(!createNewBackupLog || operationsReadFromOriginalLog == backupLogReadOperations,
                "Operations read from Backup Log (" + backupLogReadOperations + ") differ from Original Log ones (" + operationsReadFromOriginalLog + ") ");
        return backupLogReadOperations;
    }

    /**
     * Returns whether it exists a {@link DurableDataLog} with the reserved id for Backup Log (BACKUP_LOG_ID).
     *
     * @param dataLogFactory Factory to instantiate {@link DurableDataLog} instances.
     * @return Whether there is metadata for an existing Backup Log.
     * @throws Exception If there is an error initializing the {@link DurableDataLog}.
     */
    private boolean existsBackupLog(DurableDataLogFactory dataLogFactory) throws Exception {
        try (DebugDurableDataLogWrapper backupDataLogDebugLogWrapper = dataLogFactory.createDebugLogWrapper(dataLogFactory.getBackupLogId())) {
            return backupDataLogDebugLogWrapper.fetchMetadata() != null;
        }
    }

    /**
     * Copies the contents of the input containerId and {@link DebugBookKeeperLogWrapper} on a new log with id BACKUP_LOG_ID.
     *
     * @param dataLogFactory Factory to instantiate {@link DurableDataLog} instances.
     * @param containerId Container id for the source log.
     * @param originalDataLog Source log.
     * @throws Exception If there is an error during backing up process.
     */
    private void createBackupLog(DurableDataLogFactory dataLogFactory, int containerId, DebugDurableDataLogWrapper originalDataLog) throws Exception {
        // Create a new Backup Log to store the Original Log contents.
        @Cleanup
        DurableDataLog backupDataLog = dataLogFactory.createDurableDataLog(dataLogFactory.getBackupLogId());
        backupDataLog.initialize(TIMEOUT);

        // Instantiate the processor that will back up the Original Log contents into the Backup Log.
        int operationsReadFromOriginalLog;
        try (BackupLogProcessor backupLogProcessor = new BackupLogProcessor(backupDataLog, getCommandArgs().getState().getExecutor());
             DurableDataLog originalDataLogReadOnly = originalDataLog.asReadOnly()) {
            operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback(backupLogProcessor, containerId, originalDataLogReadOnly);
            // The number of processed operation should match the number of read operations from DebugRecoveryProcessor.
            checkBackupLogAssertions(backupLogProcessor.getBeforeCommit().get(), backupLogProcessor.getCommitSuccess().get(),
                    operationsReadFromOriginalLog, backupLogProcessor.isFailed);
        } catch (Exception e) {
            outputError("There have been errors while creating the Backup Log.");
            throw e;
        }
    }

    /**
     * Performs some basic correctness checks on a backup log.
     *
     * @param beforeCommitCalls Number of executions of beforeCommit callbacks during the processing.
     * @param commitSuccessCalls Number of executions of commitSuccess callbacks during the processing.
     * @param operationsReadFromOriginalLog Number of {@link Operation}s read from the Original Log.
     * @param isFailed Whether the {@link BackupLogProcessor} has found any failure during processing.
     */
    @VisibleForTesting
    void checkBackupLogAssertions(long beforeCommitCalls, long commitSuccessCalls, long operationsReadFromOriginalLog, boolean isFailed) {
        Preconditions.checkState(beforeCommitCalls == commitSuccessCalls, "BackupLogProcessor has different number of processed (" + beforeCommitCalls +
                ") and successful operations (" + commitSuccessCalls + ")");
        Preconditions.checkState(commitSuccessCalls == operationsReadFromOriginalLog, "BackupLogProcessor successful operations (" + commitSuccessCalls +
                ") differs from Original Log operations (" + operationsReadFromOriginalLog + ")");
        Preconditions.checkState(!isFailed, "BackupLogProcessor has failed");
    }

    /**
     * Verifies that the list of {@link LogEditOperation} is correct and complies with a set of rules.
     *
     * @param durableLogEdits List of {@link LogEditOperation} to check.
     */
    @VisibleForTesting
    void checkDurableLogEdits(List<LogEditOperation> durableLogEdits) {
        long previousInitialId = Long.MIN_VALUE;
        long previousFinalId = Long.MIN_VALUE;
        LogEditType previousEditType = null;
        for (LogEditOperation logEditOperation: durableLogEdits) {
            // All LogEditOperations should target sequence numbers larger than 0.
            Preconditions.checkState(logEditOperation.getInitialOperationId() > 0);
            // For delete edits, the last id should be strictly larger than the initial id.
            Preconditions.checkState(logEditOperation.getType() != LogEditType.DELETE_OPERATION
                    || logEditOperation.getInitialOperationId() < logEditOperation.getFinalOperationId());
            // Next operation should start at a higher sequence number (i.e., we cannot have and "add" and a "replace"
            // edits for the same sequence number). The only exception are consecutive add edits.
            if (previousEditType != null) {
                boolean consecutiveAddEdits = previousEditType == LogEditType.ADD_OPERATION
                        && logEditOperation.getType() == LogEditType.ADD_OPERATION;
                Preconditions.checkState(consecutiveAddEdits || logEditOperation.getInitialOperationId() > previousInitialId);
                // If the previous Edit Operation was Delete, then the next Operation initial sequence number should be > than
                // the final sequence number of the Delete operation.
                Preconditions.checkState(previousEditType != LogEditType.DELETE_OPERATION || logEditOperation.getInitialOperationId() >= previousFinalId);
            }
            // Check that Add Edit Operations have non-null payloads.
            Preconditions.checkState(logEditOperation.getType() == LogEditType.DELETE_OPERATION || logEditOperation.getNewOperation() != null);
            previousEditType = logEditOperation.getType();
            previousInitialId = logEditOperation.getInitialOperationId();
            previousFinalId = logEditOperation.getFinalOperationId();
        }
    }

    /**
     * Guides the users to a set of options for creating {@link LogEditOperation}s that will eventually modify the
     * contents of the Original Log.
     *
     * @return List of {@link LogEditOperation}s.
     */
    @VisibleForTesting
    List<LogEditOperation> getDurableLogEditsFromUser() {
        List<LogEditOperation> durableLogEdits = new ArrayList<>();
        boolean finishInputCommands = false;
        while (!finishInputCommands) {
            try {
                final String operationTpe = getStringUserInput("Select edit action on DurableLog: [delete|add|replace]");
                switch (operationTpe) {
                    case "delete":
                        long initialOpId = getLongUserInput("Initial operation id to delete? (inclusive)");
                        long finalOpId = getLongUserInput("Final operation id to delete? (exclusive)");
                        durableLogEdits.add(new LogEditOperation(LogEditType.DELETE_OPERATION, initialOpId, finalOpId, null));
                        break;
                    case "add":
                        initialOpId = getLongUserInput("At which Operation sequence number would you like to add new Operations?");
                        do {
                            durableLogEdits.add(new LogEditOperation(LogEditType.ADD_OPERATION, initialOpId, initialOpId, createUserDefinedOperation()));
                            output("You can add more Operations at this sequence number or not.");
                        } while (confirmContinue());
                        break;
                    case "replace":
                        initialOpId = getLongUserInput("What Operation sequence number would you like to replace?");
                        durableLogEdits.add(new LogEditOperation(LogEditType.REPLACE_OPERATION, initialOpId, initialOpId, createUserDefinedOperation()));
                        break;
                    default:
                        output("Invalid operation, please select one of [delete|add|replace]");
                }
                checkDurableLogEdits(durableLogEdits);
            } catch (NumberFormatException ex) {
                outputError("Wrong input argument.");
                outputException(ex);
            } catch (IllegalStateException ex) {
                // Last input was incorrect, so remove it.
                output("Last Log Edit Operation did not pass the checks, removing it from list of edits.");
                durableLogEdits.remove(durableLogEdits.size() - 1);
            } catch (Exception ex) {
                outputError("Some problem has happened.");
                outputException(ex);
            }
            output("You can continue adding edits to the original DurableLog.");
            finishInputCommands = !confirmContinue();
        }

        return durableLogEdits;
    }

    /**
     * Guides the user to generate a new {@link Operation} that will eventually modify the Original Log.
     *
     * @return New {@link Operation} to be added in the Original Log.
     */
    @VisibleForTesting
    Operation createUserDefinedOperation() {
        Operation result;
        final String operations = "[DeleteSegmentOperation|MergeSegmentOperation|MetadataCheckpointOperation|" +
                "StorageMetadataCheckpointOperation|StreamSegmentAppendOperation|StreamSegmentMapOperation|" +
                "StreamSegmentSealOperation|StreamSegmentTruncateOperation|UpdateAttributesOperation]";
        switch (getStringUserInput("Type one of the following Operations to instantiate: " + operations)) {
            case "DeleteSegmentOperation":
                long segmentId = getLongUserInput("Input Segment Id for DeleteSegmentOperation:");
                result = new DeleteSegmentOperation(segmentId);
                long offset = getLongUserInput("Input Segment Offset for DeleteSegmentOperation:");
                ((DeleteSegmentOperation) result).setStreamSegmentOffset(offset);
                break;
            case "MergeSegmentOperation":
                long targetSegmentId = getLongUserInput("Input Target Segment Id for MergeSegmentOperation:");
                long sourceSegmentId = getLongUserInput("Input Source Segment Id for MergeSegmentOperation:");
                result = new MergeSegmentOperation(targetSegmentId, sourceSegmentId, createAttributeUpdateCollection());
                offset = getLongUserInput("Input Segment Offset for MergeSegmentOperation:");
                ((MergeSegmentOperation) result).setStreamSegmentOffset(offset);
                break;
            case "MetadataCheckpointOperation":
                result = new MetadataCheckpointOperation();
                ((MetadataCheckpointOperation) result).setContents(createOperationContents());
                break;
            case "StorageMetadataCheckpointOperation":
                result = new StorageMetadataCheckpointOperation();
                ((StorageMetadataCheckpointOperation) result).setContents(createOperationContents());
                break;
            case "StreamSegmentAppendOperation":
                segmentId = getLongUserInput("Input Segment Id for StreamSegmentAppendOperation:");
                offset = getLongUserInput("Input Segment Offset for StreamSegmentAppendOperation:");
                result = new StreamSegmentAppendOperation(segmentId, offset, createOperationContents(), createAttributeUpdateCollection());
                break;
            case "StreamSegmentMapOperation":
                segmentId = getLongUserInput("Input Segment Id of the Segment: ");
                result = new StreamSegmentMapOperation(createSegmentProperties());
                ((StreamSegmentMapOperation) result).setStreamSegmentId(segmentId);
                break;
            case "StreamSegmentSealOperation":
                segmentId = getLongUserInput("Input Segment Id for StreamSegmentSealOperation:");
                result = new StreamSegmentSealOperation(segmentId);
                offset = getLongUserInput("Input Segment Offset for StreamSegmentSealOperation:");
                ((StreamSegmentSealOperation) result).setStreamSegmentOffset(offset);
                break;
            case "StreamSegmentTruncateOperation":
                segmentId = getLongUserInput("Input Segment Id for StreamSegmentTruncateOperation:");
                offset = getLongUserInput("Input Offset for StreamSegmentTruncateOperation:");
                result = new StreamSegmentTruncateOperation(segmentId, offset);
                break;
            case "UpdateAttributesOperation":
                segmentId = getLongUserInput("Input Segment Id for UpdateAttributesOperation:");
                result = new UpdateAttributesOperation(segmentId, createAttributeUpdateCollection());
                break;
            default:
                output("Invalid operation, please select one of " + operations);
                throw new UnsupportedOperationException();
        }
        return result;
    }

    /**
     * Provides two ways of creating the payload of {@link Operation}s with binary content (MetadataCheckpointOperation,
     * StorageMetadataCheckpointOperation, StreamSegmentAppendOperation): i) zero, which means to provide a content of
     * a defined length consisting of just 0s, ii) file, which will read the contents of a specified file and use it as
     * payload for the {@link Operation}.
     *
     * @return Binary contents for the {@link Operation}.
     */
    @VisibleForTesting
    ByteArraySegment createOperationContents() {
        ByteArraySegment content = null;
        do {
            try {
                switch (getStringUserInput("You are about to create the content for the new Operation. " +
                        "The available options are i) generating 0s as payload (zero), " +
                        "ii) load the contents from a provided file (file), iii) quit: [zero|file|quit]")) {
                    case "zero":
                        int contentLength = getIntUserInput("Input length of the Operation content: ");
                        content = new ByteArraySegment(new byte[contentLength]);
                        break;
                    case "file":
                        String path = getStringUserInput("Input the path for the file to use as Operation content:");
                        content = new ByteArraySegment(Files.readAllBytes(Path.of(path)));
                        break;
                    case "quit":
                        throw new AbortedUserOperation();
                    default:
                        output("Wrong option. Please, select one of the following options: [zero|file]");
                }
            } catch (AbortedUserOperation ex) {
                output("Content generation operation aborted by user.");
                throw ex;
            } catch (Exception ex) {
                outputError("Some problem has happened.");
                outputException(ex);
            }
        } while (content == null);
        return content;
    }

    /**
     * Method to create a {@link SegmentProperties} object to fill a new {@link StreamSegmentMapOperation}.
     *
     * @return New {@link SegmentProperties} object with user-defined content.
     */
    @VisibleForTesting
    SegmentProperties createSegmentProperties() {
        String segmentName = getStringUserInput("Input the name of the Segment: ");
        long offset = getLongUserInput("Input the offset of the Segment: ");
        long length = getLongUserInput("Input the length of the Segment: ");
        long storageLength = getLongUserInput("Input the storage length of the Segment: ");
        boolean sealed = getBooleanUserInput("Is the Segment sealed? [true/false]: ");
        boolean sealedInStorage = getBooleanUserInput("Is the Segment sealed in storage? [true/false]: ");
        boolean deleted = getBooleanUserInput("Is the Segment deleted? [true/false]: ");
        boolean deletedInStorage = getBooleanUserInput("Is the Segment deleted in storage? [true/false]: ");
        output("You are about to start adding Attributes to the SegmentProperties instance.");
        boolean finishInputCommands = !confirmContinue();
        Map<AttributeId, Long> attributes = new HashMap<>();
        while (!finishInputCommands) {
            output("Creating an AttributeUpdateCollection for this operation.");
            try {
                AttributeId attributeId = AttributeId.fromUUID(UUID.fromString(getStringUserInput("Input UUID for this Attribute: ")));
                long value = getLongUserInput("Input the Value for this Attribute:");
                attributes.put(attributeId, value);
            } catch (NumberFormatException ex) {
                outputError("Wrong input argument.");
                outputException(ex);
            } catch (Exception ex) {
                outputError("Some problem has happened.");
                outputException(ex);
            }
            output("You can continue adding AttributeUpdates to the AttributeUpdateCollection.");
            finishInputCommands = !confirmContinue();
        }
        long lastModified = getLongUserInput("Input last modified timestamp for the Segment (milliseconds): ");
        return StreamSegmentInformation.builder().name(segmentName).startOffset(offset).length(length).storageLength(storageLength)
                .sealed(sealed).deleted(deleted).sealedInStorage(sealedInStorage).deletedInStorage(deletedInStorage)
                .attributes(attributes).lastModified(new ImmutableDate(lastModified)).build();
    }

    /**
     * Method to create a {@link AttributeUpdateCollection} object to fill the {@link Operation}s that require it.
     *
     * @return New {@link AttributeUpdateCollection} object with user-defined content.
     */
    @VisibleForTesting
    AttributeUpdateCollection createAttributeUpdateCollection() {
        AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
        output("You are about to start adding AttributeUpdates to the AttributeUpdateCollection.");
        boolean finishInputCommands = !confirmContinue();
        while (!finishInputCommands) {
            output("Creating an AttributeUpdateCollection for this operation.");
            try {
                AttributeId attributeId = AttributeId.fromUUID(UUID.fromString(getStringUserInput("Input UUID for this AttributeUpdate: ")));
                AttributeUpdateType type = AttributeUpdateType.get((byte) getIntUserInput("Input AttributeUpdateType for this AttributeUpdate" +
                        "(0 (None), 1 (Replace), 2 (ReplaceIfGreater), 3 (Accumulate), 4(ReplaceIfEquals)): "));
                long value = getLongUserInput("Input the Value for this AttributeUpdate:");
                long comparisonValue = getLongUserInput("Input the comparison Value for this AttributeUpdate:");
                attributeUpdates.add(new AttributeUpdate(attributeId, type, value, comparisonValue));
            } catch (NumberFormatException ex) {
                outputError("Wrong input argument.");
                outputException(ex);
            } catch (Exception ex) {
                outputError("Some problem has happened.");
                outputException(ex);
            }
            output("You can continue adding AttributeUpdates to the AttributeUpdateCollection.");
            finishInputCommands = !confirmContinue();
        }
        return attributeUpdates;
    }

    /**
     * Reads a {@link DurableDataLog} associated with a container id and runs the callback on each {@link Operation}
     * read from the log.
     *
     * @param callback Callback to be run upon each {@link Operation} read.
     * @param containerId Container id to read from.
     * @param durableDataLog {@link DurableDataLog} of the Container to be read.
     * @return Number of {@link Operation}s read.
     * @throws Exception If there is a problem reading the {@link DurableDataLog}.
     */
    @VisibleForTesting
    int readDurableDataLogWithCustomCallback(BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> callback,
                                                     int containerId, DurableDataLog durableDataLog) throws Exception {
        val logReaderCallbacks = new DebugRecoveryProcessor.OperationCallbacks(
                callback,
                op -> false, // We are not interested on doing actual recovery, just reading the operations.
                null,
                null);
        val containerConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);
        val readIndexConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ReadIndexConfig::builder);
        @Cleanup
        val rp = DebugRecoveryProcessor.create(containerId, durableDataLog,
                containerConfig, readIndexConfig, getCommandArgs().getState().getExecutor(), logReaderCallbacks, false);
        int operationsRead = rp.performRecovery();
        output("Number of operations read from DurableLog: " + operationsRead);
        return operationsRead;
    }

    /**
     * This class provides the basic logic for reading from a {@link DurableDataLog} and writing the read {@link Operation}s
     * to another {@link DurableDataLog}. Internally, it uses a {@link DataFrameBuilder} to write to the target {@link DurableDataLog}
     * and performs one write at a time, waiting for the previous write to complete before issuing the next one. It also
     * provides counters to inform about the state of the processing as well as closing the resources.
     */
    abstract class AbstractLogProcessor implements BiConsumer<Operation, List<DataFrameRecord.EntryInfo>>, AutoCloseable {

        protected final Map<Long, CompletableFuture<Void>> operationProcessingTracker = new ConcurrentHashMap<>();
        @NonNull
        protected final DataFrameBuilder<Operation> dataFrameBuilder;
        @Getter
        protected boolean isFailed = false;
        @Getter
        private final AtomicBoolean closed = new AtomicBoolean();
        @Getter
        private final AtomicInteger beforeCommit = new AtomicInteger();
        @Getter
        private final AtomicInteger commitSuccess = new AtomicInteger();
        @Getter
        private final AtomicInteger commitFailure = new AtomicInteger();
        private final AtomicLong sequenceNumber = new AtomicLong(Long.MIN_VALUE);

        AbstractLogProcessor(DurableDataLog durableDataLog, ScheduledExecutorService executor) {
            DataFrameBuilder.Args args = new DataFrameBuilder.Args(
                    a -> this.beforeCommit.getAndIncrement(),
                    b -> {
                        this.operationProcessingTracker.get(b.getLastFullySerializedSequenceNumber()).complete(null);
                        this.commitSuccess.getAndIncrement();
                    },
                    (c, d) -> {
                        this.operationProcessingTracker.get(d.getLastFullySerializedSequenceNumber()).complete(null);
                        this.isFailed = true; // Consider a single failed write as a failure in the whole process.
                        this.commitFailure.getAndIncrement();
                    },
                    executor);
            this.dataFrameBuilder = new DataFrameBuilder<>(durableDataLog, OperationSerializer.DEFAULT, args);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                this.dataFrameBuilder.flush();
                this.dataFrameBuilder.close();
                this.operationProcessingTracker.clear();
            }
        }

        /**
         * Writes an {@link Operation} to the {@link DataFrameBuilder} and wait for the {@link DataFrameBuilder.Args}
         * callbacks are invoked after the operation is written to the target {@link DurableDataLog}.
         *
         * @param operation {@link Operation} to be written and completed before continue with further writes.
         * @throws IOException If there is a problem writing the {@link Operation} to the target {@link DurableDataLog}.
         */
        protected void writeAndConfirm(Operation operation) throws IOException {
            sequenceNumber.compareAndSet(Long.MIN_VALUE, operation.getSequenceNumber());
            // We only consider writing operations with sequence number higher than the expected one.
            if (operation.getSequenceNumber() >= sequenceNumber.get()) {
                trackOperation(operation);
                this.dataFrameBuilder.append(operation);
                this.dataFrameBuilder.flush();
                waitForOperationCommit(operation);
                sequenceNumber.incrementAndGet();
            } else {
                outputError("Skipping (i.e., not writing) Operation with wrong Sequence Number: " + operation);
            }
        }

        private void trackOperation(Operation operation) {
            this.operationProcessingTracker.put(operation.getSequenceNumber(), new CompletableFuture<>());
        }

        private void waitForOperationCommit(Operation operation) {
            this.operationProcessingTracker.get(operation.getSequenceNumber()).join();
            this.operationProcessingTracker.remove(operation.getSequenceNumber());
        }
    }

    /**
     * Writes all the {@link Operation}s passed in the callback to the target {@link DurableDataLog}.
     */
    class BackupLogProcessor extends AbstractLogProcessor {

        BackupLogProcessor(DurableDataLog backupDataLog, ScheduledExecutorService executor) {
            super(backupDataLog, executor);
        }

        @Override
        public void accept(Operation operation, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                output("Backing up: " + operation);
                writeAndConfirm(operation);
            } catch (Exception e) {
                outputError("Error writing Operation to Backup Log " + operation);
                outputException(e);
                isFailed = true;
            }
        }
    }

    /**
     * Given a list of sorted {@link LogEditOperation}, writes to the target {@link DurableDataLog} the {@link Operation}s
     * passed in the callback plus the result of applying the {@link LogEditOperation}.
     */
    class EditingLogProcessor extends AbstractLogProcessor {
        private final List<LogEditOperation> durableLogEdits;
        private long newSequenceNumber = 1; // Operation sequence numbers start by 1.
        private int editIndex = 0;

        EditingLogProcessor(DurableDataLog editedDataLog, @NonNull List<LogEditOperation> durableLogEdits, ScheduledExecutorService executor) {
            super(editedDataLog, executor);
            this.durableLogEdits = durableLogEdits;
        }

        @Override
        public void accept(Operation operation, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                // Nothing to edit, just write the Operations from the original log to the edited one.
                if (!hasEditsToApply(operation)) {
                    operation.resetSequenceNumber(newSequenceNumber++);
                    writeAndConfirm(operation);
                } else {
                    // Edits to a DurableLog are sorted by their initial operation id and they are removed once they
                    // have been applied. The only case in which we can find a DurableLog Operation with a sequence
                    // number lower than the next DurableLog edit is that the data corruption issue we are trying to
                    // repair induces duplication of DataFrames.
                    if (checkDuplicateOperation(operation)) {
                        // Skip processing of this operation.
                        return;
                    }
                    // We have edits to do.
                    LogEditOperation logEdit = this.durableLogEdits.get(this.editIndex);
                    switch (logEdit.getType()) {
                        case DELETE_OPERATION:
                            // A Delete Edit Operation on a DurableLog consists of not writing the range of Operations
                            // between its initial (inclusive) and final (exclusive) operation id.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          delete 2-4
                            //          Result Log = [1, 2 (4), 3 (5)] (former operation sequence number)
                            applyDeleteEditOperation(operation, logEdit);
                            break;
                        case ADD_OPERATION:
                            // An Add Edit Operation on a DurableLog consists of appending the desired Operation
                            // encapsulated in the Add Edit Operation before the actual Operation contained in the log.
                            // Note that we may want to add multiple new Operations at a specific position before the
                            // actual one.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          add 2, opA, opB, opC
                            //          Result Log = [1, 2 (opA), 3 (opB), 4 (opC), 5 (2), 6 (3), 7 (4), 8 (5)] (former operation sequence number)
                            long currentInitialAddOperationId = logEdit.getInitialOperationId();
                            do {
                                applyAddEditOperation(logEdit);
                                output("Completed Add Edit Operation on DurableLog: " + logEdit);
                                if (this.editIndex >= this.durableLogEdits.size()) {
                                    // Last Add Edit Operation was the last thing to do.
                                    break;
                                }
                                logEdit = this.durableLogEdits.get(this.editIndex);
                                // Only continue if we have consecutive Add Edit Operations that refer to the same sequence number.
                            } while (logEdit.getType().equals(LogEditType.ADD_OPERATION)
                                    && logEdit.getInitialOperationId() == currentInitialAddOperationId);
                            // After all the additions are done, add the current log operation.
                            operation.resetSequenceNumber(newSequenceNumber++);
                            writeAndConfirm(operation);
                            break;
                        case REPLACE_OPERATION:
                            // A Replace Edit Operation on a DurableLog consists of deleting the current Operation and
                            // adding the new Operation encapsulated in the Replace Edit Operation.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          replace 2, opA
                            //          Result Log = [1, 2 (opA), 3, 4, 5] (former operation sequence number)
                            applyAddEditOperation(logEdit);
                            output("Completed Replace Edit Operation on DurableLog: " + logEdit);
                            break;
                        default:
                            outputError("Unknown DurableLog edit type, deleting edit operation: " + durableLogEdits.get(0).getType());
                            durableLogEdits.remove(0);
                    }
                }
            } catch (Exception e) {
                outputError("Error serializing operation " + operation);
                outputException(e);
                isFailed = true;
            }
        }

        /**
         * Checks whether the current {@link Operation} to edit is a duplicate.
         *
         * @param operation {@link Operation} to check.
         * @return Whether the current {@link Operation} to edit is a duplicate or not.
         */
        @VisibleForTesting
        boolean checkDuplicateOperation(Operation operation) {
            if (operation.getSequenceNumber() < durableLogEdits.get(this.editIndex).getInitialOperationId()) {
                outputError("Found an Operation with a lower sequence number than the initial" +
                        "id of the next edit to apply. This may be symptom of a duplicated DataFrame and will" +
                        "also duplicate the associated edit: " + operation);
                return true;
            }
            return false;
        }

        /**
         * Adds an {@link Operation}s to the target log and increments the edit index.
         *
         * @param logEdit @{@link LogEditOperation} of type {@link LogEditType#ADD_OPERATION} to be added to the target log.
         * @throws IOException If there is an error applying the "add" {@link LogEditOperation}.
         */
        private void applyAddEditOperation(LogEditOperation logEdit) throws IOException {
            logEdit.getNewOperation().setSequenceNumber(newSequenceNumber++);
            writeAndConfirm(logEdit.getNewOperation());
            this.editIndex++;
        }

        /**
         * Skips all the {@link Operation} from the original logs encompassed between the {@link LogEditOperation}
         * initial (inclusive) and final (exclusive) ids. When the last applicable delete has been applied, the edit
         * index is increased.
         *
         * @param operation {@link Operation} read from the log.
         * @param logEdit @{@link LogEditOperation} of type {@link LogEditType#DELETE_OPERATION} that defines the sequence
         *                numbers of the {@link Operation}s to do not write to the target log.
         */
        private void applyDeleteEditOperation(Operation operation, LogEditOperation logEdit) {
            output("Deleting operation from DurableLog: " + operation);
            if (logEdit.getFinalOperationId() == operation.getSequenceNumber() + 1) {
                // Once reached the end of the Delete Edit Operation range, go for the next edit.
                this.editIndex++;
                output("Completed Delete Edit Operation on DurableLog: " + logEdit);
            }
        }

        /**
         * Decides whether there are edits to apply on the log for the specific sequence id of the input {@link Operation}.
         *
         * @param op {@link Operation} to check if there are edits to apply.
         * @return Whether there are edits to apply to the log at the specific position of the input {@link Operation}.
         */
        private boolean hasEditsToApply(Operation op) {
            if (this.editIndex == this.durableLogEdits.size()) {
                return false;
            }
            LogEditType editType = this.durableLogEdits.get(this.editIndex).getType();
            long editInitialOpId = this.durableLogEdits.get(this.editIndex).getInitialOperationId();
            long editFinalOpId = this.durableLogEdits.get(this.editIndex).getFinalOperationId();
            return editInitialOpId == op.getSequenceNumber()
                    || (editType.equals(LogEditType.DELETE_OPERATION)
                        && editInitialOpId <= op.getSequenceNumber()
                        && editFinalOpId >= op.getSequenceNumber());
        }
    }

    /**
     * Available types of editing operations we can perform on a {@link DurableDataLog}.
     */
    enum LogEditType {
        DELETE_OPERATION,
        ADD_OPERATION,
        REPLACE_OPERATION
    }

    /**
     * Information encapsulated by each edit to the target log.
     */
    @Data
    static class LogEditOperation {
        private final LogEditType type;
        private final long initialOperationId;
        private final long finalOperationId;
        private final Operation newOperation;

        @Override
        public boolean equals(Object objToCompare) {
            if (!(objToCompare instanceof LogEditOperation)) {
                return false;
            }
            if (objToCompare == this) {
                return true;
            }
            LogEditOperation opToCompare = (LogEditOperation) objToCompare;
            return this.type == opToCompare.getType()
                    && this.initialOperationId == opToCompare.getInitialOperationId()
                    && this.finalOperationId == opToCompare.getFinalOperationId()
                    && (this.type == LogEditType.DELETE_OPERATION || compareOperations(opToCompare.getNewOperation()));
        }

        private boolean compareOperations(Operation newOperationToCompare) {
            if (this.newOperation == newOperationToCompare) {
                return true;
            }
            if (this.newOperation == null || newOperationToCompare == null) {
                return false;
            }
            // Compare the main parts of an Operation for considering it equal.
            return this.newOperation.getSequenceNumber() == newOperationToCompare.getSequenceNumber()
                    && this.newOperation.getType() == newOperationToCompare.getType();
        }

        @Override
        public int hashCode() {
            return Long.hashCode(initialOperationId) + Long.hashCode(finalOperationId) + (type == null ? 0 : type.hashCode());
        }
    }

    static class AbortedUserOperation extends RuntimeException {
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-repair", "Allows to repair DurableLog " +
                "damaged/corrupted Operations.",
                new ArgDescriptor("container-id", "Id of the Container to repair."));
    }
}
