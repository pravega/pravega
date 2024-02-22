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
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;

import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * This class contains various utils methods useful for administration of LTS.
 */
@Slf4j
@AllArgsConstructor
@Data
public class UtilsWrapper {
    private static final SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer BATCH_SERIALIZER = new SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer();

    @NonNull
    private ChunkedSegmentStorage chunkedSegmentStorage;

    private int bufferSize;

    @NonNull
    private Duration timeout;

    /**
     * Evicts all eligible entries from buffer cache and all entries from guava cache.
     * This should be invoked after directly changing the metadata in table segment to ignore cached values.
     *
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *         If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<Void> evictMetadataCache() {
        return CompletableFuture.runAsync(() -> {
            val metadataStore = (BaseMetadataStore) chunkedSegmentStorage.getMetadataStore();
            metadataStore.evictAllEligibleEntriesFromBuffer();
            metadataStore.evictFromCache();
        }, chunkedSegmentStorage.getExecutor());
    }

    /**
     * Evict entire {@link ReadIndexCache}.
     * This should be invoked after directly changing the metadata in table segment to ignore cached values.
     *
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *         If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<Void> evictReadIndexCache() {
        return CompletableFuture.runAsync(() -> {
            chunkedSegmentStorage.getReadIndexCache().getSegmentsReadIndexCache().invalidateAll();
            chunkedSegmentStorage.getReadIndexCache().getIndexEntryCache().invalidateAll();
        }, chunkedSegmentStorage.getExecutor());
    }

    /**
     * Evict {@link ReadIndexCache} for given segment.
     * This should be invoked after directly changing the metadata in table segment to ignore cached values.
     *
     * @param segmentName Name of the segment.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *         If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<Void> evictReadIndexCacheForSegment(String segmentName) {
        Preconditions.checkNotNull(segmentName, "segmentName");
        return CompletableFuture.runAsync(() -> chunkedSegmentStorage.getReadIndexCache().remove(segmentName), chunkedSegmentStorage.getExecutor());
    }

    /**
     * Copy the contents of given segment to provided {@link OutputStream}.
     *
     * @param segmentName Name of the segment.
     * @param outputStream Instance of {@link OutputStream} to copy to.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *         If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<Void> copyFromSegment(String segmentName, OutputStream outputStream) {
        Preconditions.checkNotNull(segmentName, "segmentName");
        Preconditions.checkNotNull(outputStream, "outputStream");
        byte[] buffer = new byte[bufferSize];

        return chunkedSegmentStorage.getStreamSegmentInfo(segmentName, timeout)
                .thenComposeAsync(info -> {
                    val bytesRemaining = new AtomicLong(info.getLength() - info.getStartOffset());
                    val offsetToRead = new AtomicLong(info.getStartOffset());
                    return chunkedSegmentStorage.openRead(segmentName)
                            .thenComposeAsync(handle -> Futures.loop(
                                    () -> bytesRemaining.get() > 0,
                                    () -> chunkedSegmentStorage.read(handle,
                                                    offsetToRead.get(),
                                                    buffer,
                                                    0,
                                                    Math.toIntExact(Math.min(bytesRemaining.get(), buffer.length)),
                                                    timeout)
                                            .thenComposeAsync(bytesRead -> {
                                                bytesRemaining.addAndGet(-bytesRead);
                                                offsetToRead.addAndGet(bytesRead);
                                                try {
                                                    outputStream.write(buffer, 0, bytesRead);
                                                    return completedFuture(null);
                                                } catch (IOException e) {
                                                    return CompletableFuture.failedFuture(e);
                                                }
                                            }, chunkedSegmentStorage.getExecutor()),
                                    chunkedSegmentStorage.getExecutor()),
                            chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor());

    }

    /**
     * Copy the contents of given chunk to provided {@link OutputStream}.
     *
     * @param chunkName Name of the chunk.
     * @param outputStream Instance of {@link OutputStream} to copy to.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *         If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<Void> copyFromChunk(String chunkName, OutputStream outputStream) {
        Preconditions.checkNotNull(chunkName, "chunkName");
        Preconditions.checkNotNull(outputStream, "outputStream");

        byte[] buffer = new byte[bufferSize];

        return chunkedSegmentStorage.getChunkStorage().getInfo(chunkName)
                .thenComposeAsync(info -> {
                    val bytesRemaining = new AtomicLong(info.getLength());
                    val offsetToRead = new AtomicLong(0);
                    return chunkedSegmentStorage.getChunkStorage().openRead(chunkName)
                            .thenComposeAsync(handle -> Futures.loop(
                                    () -> bytesRemaining.get() > 0,
                                    () -> chunkedSegmentStorage.getChunkStorage().read(handle,
                                                    offsetToRead.get(),
                                                    Math.toIntExact(Math.min(bytesRemaining.get(), buffer.length)),
                                                    buffer,
                                                    0)
                                            .thenComposeAsync(bytesRead -> {
                                                bytesRemaining.addAndGet(-bytesRead);
                                                offsetToRead.addAndGet(bytesRead);
                                                try {
                                                    outputStream.write(buffer, 0, bytesRead);
                                                    return completedFuture(null);
                                                } catch (Exception e) {
                                                    return CompletableFuture.failedFuture(e);
                                                }
                                            }, chunkedSegmentStorage.getExecutor()),
                                        chunkedSegmentStorage.getExecutor()),
                                    chunkedSegmentStorage.getExecutor());
                }, chunkedSegmentStorage.getExecutor());

    }

    /**
     * Overwrites the given chunk on the storage with given data.
     *
     * @param chunkName Name of the chunk to overwrite.
     * @param inputStream {@link InputStream} which contains data to write.
     * @param length number of bytes to write.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *         If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<Void> overwriteChunk(String chunkName, InputStream inputStream, int length) {
        Preconditions.checkNotNull(chunkName, "chunkName");
        Preconditions.checkNotNull(inputStream, "inputStream");
        return chunkedSegmentStorage.getChunkStorage().openWrite(chunkName)
                .thenComposeAsync(deleteHandle -> chunkedSegmentStorage.getChunkStorage().delete(deleteHandle), chunkedSegmentStorage.getExecutor())
                .thenRunAsync(() -> chunkedSegmentStorage.getChunkStorage().createWithContent(chunkName, length, inputStream), chunkedSegmentStorage.getExecutor());

    }

    /**
     * Returns the list of {@link ExtendedChunkInfo} which contain data about all chunks for the segment.
     *
     * @param streamSegmentName Name of the segment.
     * @param checkStorage Whether to retrieve information from underlying {@link ChunkStorage}.
     * @return A CompletableFuture that, when completed, will contain a list of {@link ExtendedChunkInfo} objects associated with the segment.
     *  If the operation failed, it will be completed with the appropriate exception.
     */
    public CompletableFuture<List<ExtendedChunkInfo>> getExtendedChunkInfoList(String streamSegmentName, boolean checkStorage) {
        Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
        val infoList = new Vector<ExtendedChunkInfo>();
        return chunkedSegmentStorage.executeSerialized(() -> chunkedSegmentStorage.tryWith(
                chunkedSegmentStorage.getMetadataStore().beginTransaction(true, streamSegmentName),
                txn ->  txn.get(streamSegmentName)
                            .thenComposeAsync(storageMetadata -> {
                                val segmentMetadata = (SegmentMetadata) storageMetadata;
                                segmentMetadata.checkInvariants();
                                val iterator = new ChunkIterator(chunkedSegmentStorage.getExecutor(), txn, segmentMetadata);
                                val startOffset = new AtomicLong(segmentMetadata.getFirstChunkStartOffset());
                                iterator.forEach((metadata, name) -> {
                                    infoList.add(ExtendedChunkInfo.builder()
                                                    .chunkName(name)
                                                    .startOffset(startOffset.get())
                                                    .lengthInMetadata(metadata.getLength())
                                            .build());
                                    startOffset.addAndGet(metadata.getLength());
                                });
                                return completedFuture(infoList);
                            }, chunkedSegmentStorage.getExecutor())
                            .thenComposeAsync(v -> {
                                    val futures = new ArrayList<CompletableFuture<Void>>();
                                    if (checkStorage) {
                                        for (val info : infoList) {
                                            futures.add(
                                                chunkedSegmentStorage.getChunkStorage().exists(info.getChunkName())
                                                    .thenComposeAsync(doesExist -> {
                                                        if (doesExist) {
                                                            return chunkedSegmentStorage.getChunkStorage().getInfo(info.getChunkName())
                                                                    .thenAcceptAsync(chunkInfo -> {
                                                                        info.setLengthInStorage(chunkInfo.getLength());
                                                                        info.setExistsInStorage(true);
                                                                    }, chunkedSegmentStorage.getExecutor());
                                                        } else {
                                                            return completedFuture(null);
                                                        }
                                                    }, chunkedSegmentStorage.getExecutor()));
                                        }
                                    }
                                    return Futures.allOf(futures);
                                }, chunkedSegmentStorage.getExecutor())
                            .thenApplyAsync(vv -> infoList, chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor()), streamSegmentName);
    }

    /**
     * Performs sanity operations on chunk like create chunk, write to the chunk, check if the chunk exists, read back contents to the chunk and delete the chunk.
     *
     * @param chunkName Name of the chunk.
     * @param dataSize dataSize of the bytes to read.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    public CompletableFuture<Void> checkChunkSegmentStorageSanity(String chunkName, int dataSize) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(chunkName), "chunk name must not be null or empty");
        Preconditions.checkArgument(dataSize >= 0, "dataSize should be positive integer");
        byte[] testData = new byte[dataSize];
        byte[] readData = new byte[dataSize];
        InputStream inputStream = new ByteArrayInputStream(testData);
        AtomicBoolean isCreated = new AtomicBoolean();

        return chunkedSegmentStorage.getChunkStorage().createWithContent(chunkName, testData.length, inputStream)
                .thenComposeAsync(v -> {
                    isCreated.set(true);
                    return chunkedSegmentStorage.getChunkStorage().getInfo(chunkName);
                }, chunkedSegmentStorage.getExecutor())
                .thenComposeAsync(chunkInfo -> chunkedSegmentStorage.getChunkStorage().openWrite(chunkName)
                        .thenComposeAsync(chunkHandle -> chunkedSegmentStorage.getChunkStorage().exists(chunkName), chunkedSegmentStorage.getExecutor())
                        .thenAcceptAsync(doesExists -> Preconditions.checkState(doesExists, "The given chunk doesn't exist!"), chunkedSegmentStorage.getExecutor())
                        .thenComposeAsync(v -> chunkedSegmentStorage.getChunkStorage().read(ChunkHandle.readHandle(chunkName), 0, readData.length, readData, 0), chunkedSegmentStorage.getExecutor())
                        .thenAcceptAsync(bytesRead -> {
                            Preconditions.checkState(bytesRead == dataSize, "Bytes read are not equal to dataSize.");
                            Preconditions.checkState(Arrays.equals(testData, readData), "The arrays after reading the bytes are not equal.");
                        }, chunkedSegmentStorage.getExecutor())
                        .thenComposeAsync(v -> chunkedSegmentStorage.getChunkStorage().delete(ChunkHandle.writeHandle(chunkName)), chunkedSegmentStorage.getExecutor()),
                chunkedSegmentStorage.getExecutor())
                .handleAsync((v, e) -> {
                    if (isCreated.get()) {
                     chunkedSegmentStorage.getChunkStorage().delete(ChunkHandle.writeHandle(chunkName));
                    }
                    if (e != null) {
                        throw new CompletionException(Exceptions.unwrap(e));
                    }
                    return v;
                }, chunkedSegmentStorage.getExecutor());
    }
}
