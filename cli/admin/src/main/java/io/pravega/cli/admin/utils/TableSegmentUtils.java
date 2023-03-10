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
import org.apache.commons.lang.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableSegmentUtils {

    /**
     * Retrieve Pravega Operations from chunk files.
     * @param chunkFiles Chunks list of chunk files to be parsed.
     * @return List of Pravega operations.
     * @throws IOException if any exception while parsing raw chunk files.
     */
    public static List<TableSegmentUtils.TableSegmentOperation>  getOperationsFromChunks(List<File> chunkFiles) throws IOException {
        byte[] partialEntryFromLastChunk = null;
        int unprocessedBytesFromLastChunk = 0;
        List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations = new ArrayList<>();
        for (File f : chunkFiles) {
            Preconditions.checkState((unprocessedBytesFromLastChunk > 0 && partialEntryFromLastChunk != null) || (unprocessedBytesFromLastChunk == 0 && partialEntryFromLastChunk == null));
            ByteArraySegment chunkData = new ByteArraySegment(getBytesToProcess(partialEntryFromLastChunk, f));
            unprocessedBytesFromLastChunk = scanAllEntriesInTableSegmentChunks(chunkData, tableSegmentOperations);
            partialEntryFromLastChunk = unprocessedBytesFromLastChunk > 0 ? chunkData.getReader(chunkData.getLength() - unprocessedBytesFromLastChunk, unprocessedBytesFromLastChunk).readAllBytes() : null;
        }
        return tableSegmentOperations;
    }

    /**
     * Retrieve Pravega Operations from raw bytes.
     * @param byteArraySegments byte arrays to be parsed and converted to Operations.
     * @return List of Pravega operations
     * @throws IOException if any exception while parsing raw chunk files.
     */
    public static List<TableSegmentUtils.TableSegmentOperation> getOperationsFromBytes(List<ByteArraySegment> byteArraySegments) throws IOException {
        byte[] partialEntryFromLastChunk = null;
        List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations = new ArrayList<>();
        int unprocessedBytesFromLastChunk = 0;
        for (ByteArraySegment byteArray : byteArraySegments) {
            ByteArraySegment allBytes = new ByteArraySegment(byteArray.array());
            if (partialEntryFromLastChunk != null) {
                allBytes = new ByteArraySegment(ArrayUtils.addAll(partialEntryFromLastChunk, byteArray.array()));
            }
            unprocessedBytesFromLastChunk = scanAllEntriesInTableSegmentChunks(allBytes, tableSegmentOperations);
            partialEntryFromLastChunk = unprocessedBytesFromLastChunk > 0 ? allBytes.getReader(allBytes.getLength() - unprocessedBytesFromLastChunk, unprocessedBytesFromLastChunk).readAllBytes() : null;
        }
        return tableSegmentOperations;
    }

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
                byte[] valueBytes = valueLength == 0 ? new byte[0] : slice.slice(EntrySerializer.HEADER_LENGTH + header.getKeyLength(), header.getValueLength()).getReader().readNBytes(header.getValueLength());

                // Add the operation to the list of operations to replay later on (PUT or DELETE).
                tableSegmentOperations.add(valueBytes.length == 0 ? new TableSegmentUtils.DeleteOperation(TableSegmentEntry.unversioned(keyBytes, valueBytes)) : new TableSegmentUtils.PutOperation(TableSegmentEntry.unversioned(keyBytes, valueBytes)));
                // Full entry read, so reset unprocessed bytes.
                Preconditions.checkState(unprocessedBytesFromLastChunk < totalEntryLength, "Some bytes are missing to process.");
                unprocessedBytesFromLastChunk = 0;
                processedBytes += totalEntryLength;
                if (tableSegmentOperations.size() % 100 == 0) {
                //output("Progress of scanning data chunk: " + ((processedBytes * 100.0) / byteArraySegment.getLength()));
                }
            } catch (IOException | RuntimeException e) {
                processedBytes++;
                unprocessedBytesFromLastChunk++;
                //outputError("Exception while processing data. Unprocessed bytes: " + unprocessedBytesFromLastChunk, e);
            }
        }

        return unprocessedBytesFromLastChunk;
    }

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

    public static class PutOperation extends TableSegmentUtils.TableSegmentOperation {
        protected PutOperation(TableSegmentEntry contents) {
            this.contents = contents;
        }
    }

    public static class DeleteOperation extends TableSegmentUtils.TableSegmentOperation {
        protected DeleteOperation(TableSegmentEntry contents) {
            this.contents = contents;
        }
    }
}
