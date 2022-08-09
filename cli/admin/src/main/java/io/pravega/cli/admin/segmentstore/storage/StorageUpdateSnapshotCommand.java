/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin.segmentstore.storage;

import com.google.common.base.Preconditions;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.storage.chunklayer.SystemJournal;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import lombok.val;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


/**
 * This command in short helps us to update a storage journal snapshot file. Storage today stores metadata about the 4 special metadata segments
 * in something called as journals. There are also snapshots of these journals created on reaching a fixed size and stored as a journal snapshot file. A snapshot is
 * a self contained  state of all the 4 special metadata segments.
 * There can be scnearios where one might want to change/update the state stored in these journal files. For e.g there could be situations where data stored in one of the
 * special segment's attribute index is corrupted and we use recovery tools to help fix this corruption. Further on fixing this corruption, there arises a need to update
 * the metadata of this fixed attribute index in the cluster to help recover the cluster. That's where a command like this can be useful to
 * help us perform such updates on the journal files themselves.
 */
public class StorageUpdateSnapshotCommand extends StorageCommand {

    public static final String INTERNAL_CONTAINER_PREFIX = "_system/containers/";

    private static final SystemJournal.SystemSnapshotRecord.Serializer SYSTEM_SNAPSHOT_SERIALIZER = new SystemJournal.SystemSnapshotRecord.Serializer();
    private static final String SNAPSHOT = "snapshot";
    private static final String EPOCH_SPLITTER = ".E-";
    private static final String OFFSET_SPLITTER = "O-";
    private static final AtomicInteger CHUNK_COUNT = new AtomicInteger();
    private static final AtomicLong SEGMENT_LENGTH = new AtomicLong();

    /**
     * Creates a new instance of the StorageUpdateSnapshotCommand.
     *
     * @param args The arguments for the command.
     */
    public StorageUpdateSnapshotCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(4);

        final String segmentChunkPath = getArg(0);
        final String journalPath = getArg(1);
        final String latestSnapshot = getArg(2);
        final String outputJournalPath = getArg(3);

        File journalFile = new File(journalPath);
        File latestSnapshotFile = new File(latestSnapshot);
        Preconditions.checkState(journalFile.isFile(), "journal-path provided should point to a valid journal file");
        Preconditions.checkState(latestSnapshotFile.isFile(), "snapshot file path provided should point to a valid file");
        //check whether we are deserializing a snapshot file or a journal index file
        JournalDeserializer deserializer = journalFile.getName().contains(SNAPSHOT) ? new JournalSnapshotDeserializer() : new JournalFileDeserializer();
        //list all segment chunks sorted based on epoch and offset
        File[] segmentChunkFiles = new File(segmentChunkPath).listFiles();
        assert segmentChunkFiles != null;
        Preconditions.checkState(segmentChunkFiles.length > 0, "No segment chunks found");
        List<File> sortedSegmentChunkFiles = Arrays.stream(segmentChunkFiles).
                filter(File::isFile).
                sorted(new FileComparator()).
                collect(Collectors.toList());
        byte[] journalBytesRead = Files.readAllBytes(journalFile.toPath());
        val journalRecords = deserializer.deserialize(journalBytesRead);
        SystemJournal.SystemSnapshotRecord systemSnapshot = null;
        // There could be other type of records like ChunkAddedRecord when using a JournalFileDeserializer
        // Make sure we are picking the SystemSnapShotRecord in it.
        for (SystemJournal.SystemJournalRecord record : journalRecords) {
            if (record instanceof SystemJournal.SystemSnapshotRecord) {
                systemSnapshot = (SystemJournal.SystemSnapshotRecord) record;
                break;
            }
        }
        Preconditions.checkNotNull(systemSnapshot, "No SystemSnapshots found");
        updateSystemSnapShotRecord(systemSnapshot, sortedSegmentChunkFiles);
        Files.write(Paths.get(outputJournalPath + latestSnapshotFile.getName()), SYSTEM_SNAPSHOT_SERIALIZER.serialize(systemSnapshot).array());
        output("SystemSnapshot Journal file has been created successfully at " + outputJournalPath);
    }

    private interface JournalDeserializer {
        Collection<SystemJournal.SystemJournalRecord> deserialize(byte[] bytes) throws Exception;
    }

    private static class JournalSnapshotDeserializer implements JournalDeserializer {
        public Collection<SystemJournal.SystemJournalRecord> deserialize(byte[] bytes) throws IOException {
            val systemSnapshotRecord = SYSTEM_SNAPSHOT_SERIALIZER.deserialize(bytes);
            return Collections.singletonList(systemSnapshotRecord);
        }
    }

    private static class JournalFileDeserializer implements JournalDeserializer {
        private static final SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer SYSTEM_JOURNAL_BATCH_SERIALIZER = new SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer();

        public Collection<SystemJournal.SystemJournalRecord> deserialize(byte[] bytes) throws IOException {
            val journalRecordBatch = SYSTEM_JOURNAL_BATCH_SERIALIZER.deserialize(bytes);
            return journalRecordBatch.getSystemJournalRecords();
        }
    }

    private static class FileComparator implements Comparator<File>, Serializable {
        @Override
        public int compare(File f1, File f2) {
            String[] file1 = f1.getName().split(EPOCH_SPLITTER);
            String[] file2 = f2.getName().split(EPOCH_SPLITTER);
            String file1Epoch = file1[file1.length - 1].split("-")[0];
            String file2Epoch = file2[file2.length - 1].split("-")[0];
            if (Long.parseLong(file1Epoch) != Long.parseLong(file2Epoch)) {
                return Long.valueOf(file1Epoch).compareTo(Long.valueOf(file2Epoch));
            }
            file1 = f1.getName().split(OFFSET_SPLITTER);
            file2 = f2.getName().split(OFFSET_SPLITTER);
            return Long.valueOf(file1[file1.length - 1].split("\\.")[0]).compareTo(Long.valueOf(file2[file2.length - 1].split("\\.")[0]));
        }
    }

    /**
     * Update the system snapshot record with the segment chunk details.
     *
     * @param systemSnapshot The snapshot to be updated
     * @param chunkFiles     the segment chunks whose properties need to
     *                       be updated in the passed system snapshot.
     */
    private void updateSystemSnapShotRecord(SystemJournal.SystemSnapshotRecord systemSnapshot, List<File> chunkFiles) {
        Collection<SystemJournal.SegmentSnapshotRecord> records = systemSnapshot.getSegmentSnapshotRecords();
        List<ChunkMetadata> chunks = generateChunks(chunkFiles);
        SystemJournal.SegmentSnapshotRecord recordToBeSaved;
        Iterator<SystemJournal.SegmentSnapshotRecord> segmentIterator = records.iterator();
        while (segmentIterator.hasNext()) {
            SystemJournal.SegmentSnapshotRecord record = segmentIterator.next();
            if (isSegmentBeingEdited(record.getSegmentMetadata().getName(), chunkFiles.get(0).getName())) {
                output("Updating SystemSnapshot with relevant data.");
                record.getSegmentMetadata().setLength(SEGMENT_LENGTH.get());
                record.getSegmentMetadata().setChunkCount(CHUNK_COUNT.get());
                record.getSegmentMetadata().setStartOffset(0);
                record.getSegmentMetadata().setFirstChunk(chunks.get(0).getName());
                record.getSegmentMetadata().setFirstChunkStartOffset(deriveStartOffset(chunks.get(0).getName()));
                record.getSegmentMetadata().setLastChunk(chunks.get(chunks.size() - 1).getName());
                record.getSegmentMetadata().setLastChunkStartOffset(deriveStartOffset(chunks.get(chunks.size() - 1).getName()));
                record.getSegmentMetadata().setLastModified(System.currentTimeMillis());
                recordToBeSaved = record.toBuilder().segmentMetadata(record.getSegmentMetadata()).chunkMetadataCollection(chunks).build();
                segmentIterator.remove();
                records.add(recordToBeSaved);
                break;
            }
        }
    }

    /**
     * Helps determine the segment pointed to by the chunks refered in this command
     * is the same as the one we have from journal file.
     */
    private boolean isSegmentBeingEdited(String segmentFromJournal, String segmentFromChunks) {
        String[] segmentParts = segmentFromJournal.split("\\/");
        segmentFromJournal = segmentParts[segmentParts.length - 1];
        if (segmentFromJournal.split(EPOCH_SPLITTER)[0].equalsIgnoreCase(segmentFromChunks.split(EPOCH_SPLITTER)[0])) {
            return true;
        }
        return false;
    }

    /**
     * Helps generate chunks metdata from the referred
     * chunk files passed.
     */
    private List<ChunkMetadata> generateChunks(List<File> chunkFiles) {
        List<ChunkMetadata> chunks = new ArrayList<>();
        ChunkMetadata.ChunkMetadataBuilder chunkBuilder = ChunkMetadata.builder();
        AtomicReference<ChunkMetadata> previousChunk = new AtomicReference<>();
        chunkFiles.forEach(file -> {
            CHUNK_COUNT.incrementAndGet();
            SEGMENT_LENGTH.addAndGet(file.length());
            ChunkMetadata ch = chunkBuilder.name(INTERNAL_CONTAINER_PREFIX + file.getName()).length(file.length()).build();
            if (previousChunk.get() != null) {
                previousChunk.get().setNextChunk(INTERNAL_CONTAINER_PREFIX + ch.getName());
            }
            ch.setActive(true);
            previousChunk.set(ch);
            chunks.add(ch);
        });
        previousChunk.get().setNextChunk(null);
        return chunks;
    }

    private long deriveStartOffset(String chunkName) {
        Preconditions.checkArgument(chunkName != null, "Chunk is null");
        Preconditions.checkArgument(chunkName.length() > 0, "Chunk Name has length 0");
        return Long.parseLong(chunkName.split(OFFSET_SPLITTER)[1].split("\\.")[0]);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "update-latest-journal-snapshot", "Updates the latest journal snapshot with the details provided",
                new ArgDescriptor("segment-chunk-path", "Directory where all segment chunks of the metadata segment to be updated are saved."),
                new ArgDescriptor("journal-path", "Path where the latest journal file is stored.(same as latest-snapshot if the latest journal file happens to be a snapshot)"),
                new ArgDescriptor("latest-snapshot", "Path where the latest snapshot file is stored."),
                new ArgDescriptor("output-directory", "Directory where the updated snapshot should be saved"));
    }

}
