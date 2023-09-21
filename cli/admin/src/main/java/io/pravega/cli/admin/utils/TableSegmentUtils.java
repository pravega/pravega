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
package io.pravega.cli.admin.utils;


import com.google.common.base.Preconditions;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.tables.EntrySerializer;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableSegmentUtils {

    /**
     * Retrieve Pravega Operations from chunk files.
     * NOTE: This method is the driver for parsing the raw bytes in Segment chunk files and converting them to Put/Delete Table Entries.
     * Caller of this method has to make sure that the Chunk files passed to this method are sorted
     * in the correct order based on their epoch and offsets.
     * @param chunkFiles Chunks list of chunk files to be parsed.
     * @return List of Pravega operations.
     * @throws IOException if any exception while parsing raw chunk files.
     */
    public static List<TableSegmentUtils.TableSegmentOperation> getOperationsFromChunks(List<File> chunkFiles) throws IOException {
        byte[] partialEntryFromLastChunk = null;
        int unprocessedBytesFromLastChunk = 0;
        List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations = new ArrayList<>();
        for (File f : chunkFiles) {
            Preconditions.checkState((unprocessedBytesFromLastChunk > 0 && partialEntryFromLastChunk != null) || (unprocessedBytesFromLastChunk == 0 && partialEntryFromLastChunk == null));
            ByteArraySegment chunkData = new ByteArraySegment(getBytesToProcess(partialEntryFromLastChunk, f));
            unprocessedBytesFromLastChunk = scanAllEntriesInTableSegmentChunks(chunkData, tableSegmentOperations);
            if (unprocessedBytesFromLastChunk > 0) {
                partialEntryFromLastChunk = chunkData.getReader(chunkData.getLength() - unprocessedBytesFromLastChunk, unprocessedBytesFromLastChunk).readAllBytes();
            } else {
                partialEntryFromLastChunk = null;
            }
        }
        return tableSegmentOperations;
    }

    /**
     * Scans the bytes read from Segment chunk files using a windowed approach.
     * Scans one byte at a time assuming its the start of Table Entry,
     * If a valid Table Entry is found, moves the window by the size of the Table Entry found.
     * @return Returns unprocessed bytes to caller, to be used as part of the next window to scan.
     */
    private static int scanAllEntriesInTableSegmentChunks(ByteArraySegment byteArraySegment, List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations) {
        EntrySerializer serializer = new EntrySerializer();
        int processedBytes = 0;
        int unprocessedBytesFromLastChunk = 0;
        while (processedBytes < byteArraySegment.getLength()) {
            try {
                ByteArraySegment slice = byteArraySegment.slice(processedBytes, byteArraySegment.getLength() - processedBytes);
                EntrySerializer.Header header = serializer.readHeader(slice.getBufferViewReader());
                // If the header has been parsed correctly, then we can proceed.
                int valueLength = header.getValueLength() < 0 ? 0 : header.getValueLength(); // In case of a removal, use 0 instead of -1.
                int totalEntryLength = EntrySerializer.HEADER_LENGTH + header.getKeyLength() + valueLength;
                byte[] keyBytes = slice.slice(EntrySerializer.HEADER_LENGTH, header.getKeyLength()).getReader().readNBytes(header.getKeyLength());

                // Add the operation to the list of operations to replay later on (PUT or DELETE).
                if (valueLength == 0) {
                    tableSegmentOperations.add(new TableSegmentUtils.DeleteOperation(TableSegmentEntry.unversioned(keyBytes, new byte[0])));
                } else {
                    byte[] valueBytes = slice.slice(EntrySerializer.HEADER_LENGTH + header.getKeyLength(), header.getValueLength()).getReader().readNBytes(header.getValueLength());
                    tableSegmentOperations.add(new TableSegmentUtils.PutOperation(TableSegmentEntry.unversioned(keyBytes, valueBytes)));
                }
                // Full entry read, so reset unprocessed bytes.
                Preconditions.checkState(unprocessedBytesFromLastChunk < totalEntryLength, "Some bytes are missing to process.");
                unprocessedBytesFromLastChunk = 0;
                processedBytes += totalEntryLength;
            } catch (IOException | RuntimeException e) {
                processedBytes++;
                unprocessedBytesFromLastChunk++;
            }
        }
        return unprocessedBytesFromLastChunk;
    }

    /**
     * Calls the actual Files api to read the bytes from Segment Chunk files.
     * Also takes into account any partial bytes from the last operation of the caller.
     */
    private static byte[] getBytesToProcess(byte[] partialEntryFromLastChunk, File f) throws IOException {
        byte[] bytesToProcess;
        if (partialEntryFromLastChunk != null && partialEntryFromLastChunk.length > 0) {
            bytesToProcess = Arrays.copyOf(partialEntryFromLastChunk, (int) (partialEntryFromLastChunk.length + f.length()));
            byte[] currentChunkBytes = Files.readAllBytes(f.toPath());
            System.arraycopy(currentChunkBytes, 0, bytesToProcess, partialEntryFromLastChunk.length, currentChunkBytes.length);
        } else {
            bytesToProcess = Files.readAllBytes(f.toPath());
        }
        return bytesToProcess;
    }

    @Getter
    public static abstract class TableSegmentOperation {
        protected TableSegmentEntry contents;
    }

    /**
     * Class to represent a Put Operation for the
     * derived TableEntry from parsing Chunk data.
     */
    public static class PutOperation extends TableSegmentUtils.TableSegmentOperation {
        protected PutOperation(TableSegmentEntry contents) {
            this.contents = contents;
        }
    }

    /**
     * Class to represent a Delete Operation for the
     * derived TableEntry from parsing Chunk data.
     */
    public static class DeleteOperation extends TableSegmentUtils.TableSegmentOperation {
        protected DeleteOperation(TableSegmentEntry contents) {
            this.contents = contents;
        }
    }
}
