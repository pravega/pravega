package io.pravega.storage.chunk;

import lombok.Builder;
import software.amazon.awssdk.services.s3.S3Client;

@Builder
public class ChunkObject {

    private final String chunkId;

    private final S3Client client;
}
