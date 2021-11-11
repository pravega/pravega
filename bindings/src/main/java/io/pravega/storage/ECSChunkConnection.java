package io.pravega.storage;

import java.io.InputStream;

public interface ECSChunkConnection {

    boolean connect();

    boolean putObject(String bucket, String makeKey, long offset, int length, InputStream data);

    boolean putObject(String bucket, String makeKey,  InputStream data);

    boolean putObject(String bucket, String key, byte[] objectContent, long objectSize);

    boolean getObject(String bucket, String key, long fromOffset, int length, byte[] buffer, int bufferOffset);

    boolean deleteObject(String bucket, String key);

    void close();

    String name();
}
