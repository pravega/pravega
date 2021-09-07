package io.pravega.storage.chunk;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.shared.StaticChunkObjectKeyGenerator;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.shared.NameUtils.SEGMENT_FILE_PREFIX;

@Slf4j
public class ChunkObjectPrefetcher {
    protected LinkedBlockingQueue<Future<ChunkObject>> chunkPrefetchQueue;

    private final ECSChunkStorage storage;

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "chunk-prefetch");
    public ChunkObjectPrefetcher(ECSChunkStorage storage, int capacity) {
        this.storage = storage;
        this.chunkPrefetchQueue = new LinkedBlockingQueue<>(capacity);
    }

    public void init(){
        executor.submit(new PrefetchChunkTask());
    }

    private class PrefetchChunkTask implements Callable<ChunkObject> {


        @Override
        public ChunkObject call() throws InterruptedException {
            while (true) {
                try {
                    if(chunkPrefetchQueue.remainingCapacity() > 0) {
                        chunkPrefetchQueue.offer(executor.submit(new createChunkTask()));
                    }
                } catch (Throwable t) {
                    log.error("met exception", t);
                }
            }
        }
    }

    private class createChunkTask implements Callable<ChunkObject> {


        @Override
        public ChunkObject call() throws ChunkStorageException {

            return createNewChunk();
        }

        private ChunkObject createNewChunk() throws ChunkStorageException {
            try {
                String chunkId = StaticChunkObjectKeyGenerator.randomChunkObjectKey(SEGMENT_FILE_PREFIX);
                S3Client s3Client = storage.getNextClient();
                storage.preCreate(chunkId, s3Client);
                return ChunkObject.builder().chunkId(chunkId).client(s3Client).build();
            } catch (Exception e){
                log.error("met exception when create chunk", e);
                throw e;
            }
        }
    }
}
