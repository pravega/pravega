package io.pravega.shared;

import com.google.common.base.Preconditions;

import java.util.concurrent.ThreadLocalRandom;

public class ChunkObjectKeyGenerator {
    public static String randomChunkObjectKey() {
        long containerId = ThreadLocalRandom.current().nextLong() & 0x0fffffff;
        long blobId = ThreadLocalRandom.current().nextLong();
        short simId = (short) ThreadLocalRandom.current().nextInt(Short.MAX_VALUE);
        return String.format("%X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(int containerId) {
        long blobId = ThreadLocalRandom.current().nextLong();
        short simId = (short) ThreadLocalRandom.current().nextInt(Short.MAX_VALUE);
        return String.format("%X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(int containerId, int simId) {
        long blobId = ThreadLocalRandom.current().nextLong();
        return String.format("%X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(int containerId, int simId, long blobId) {
        return String.format("%X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(short type, int containerId, int simId, long blobId) {
        return String.format("%03X", type) + String.format("%04X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(String name) {
        Preconditions.checkArgument(name.length() == 27, "name should be 27 chars");
        return String.format("%s", name.subSequence(0, 7)) + String.format("/%s", name.subSequence(7, 11)) + String.format("/%s", name.subSequence(11, 27));
    }
}
