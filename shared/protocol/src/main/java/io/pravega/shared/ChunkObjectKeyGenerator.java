package io.pravega.shared;

import com.google.common.base.Preconditions;

import java.util.concurrent.ThreadLocalRandom;

public class ChunkObjectKeyGenerator {
    public static String randomChunkObjectKey() {
        long containerId = ThreadLocalRandom.current().nextLong() & 0x0fffffff;
        long blobId = ThreadLocalRandom.current().nextLong();
        short simId = (short) ThreadLocalRandom.current().nextInt(Short.MAX_VALUE);
        return String.format("%07X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }



    public static String randomChunkObjectKey(short type) {
        Preconditions.checkArgument(type <= 0xf, "type should not larger than 0xf");
        long containerId = ThreadLocalRandom.current().nextLong() & 0x00ffffff;
        long blobId = ThreadLocalRandom.current().nextLong();
        short simId = (short) ThreadLocalRandom.current().nextInt(Short.MAX_VALUE);
        return String.format("%01X", type) + String.format("%06X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(int containerId, int simId) {
        long blobId = ThreadLocalRandom.current().nextLong();
        return String.format("%07X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(int containerId, int simId, long blobId) {
        return String.format("%07X", containerId) + String.format("/%1$04X", simId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(short type, int containerId, int simId, long blobId) {
        Preconditions.checkArgument(type <= 0xf, "type should not larger than 0xf");
        Preconditions.checkArgument(simId <= 0x00ffffff, "simId should not larger than 0x00ffffff");
        Preconditions.checkArgument(containerId <= 0xffff, "simId should not larger than 0xffff");
        return String.format("%01X", type) + String.format("%06X", simId) + String.format("/%1$04X", containerId) + String.format("/%1$016X", blobId);
    }

    public static String randomChunkObjectKey(short type, String name) {
        Preconditions.checkArgument(type <= 0xf, "type should not larger than 0xf");
        Preconditions.checkArgument(name.length() == 26, "name should be 27 chars");
        return String.format("%01X", type) + String.format("%s", name.subSequence(0, 6)) + String.format("/%s", name.subSequence(6, 10)) + String.format("/%s", name.subSequence(10, 26));
    }
}
