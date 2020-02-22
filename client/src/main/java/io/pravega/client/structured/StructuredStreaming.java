package io.pravega.client.structured;

import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.stream.Serializer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface StructuredStreaming {
  
    public interface KeySerializer<K> extends Serializer<K> {
        public int MAX_SIZE_BYTES = 1024;
    }
    
    public interface ValueSerializer<V> extends Serializer<V> {
        public int MAX_SIZE_BYTES = 512 * 1024;
    }
    
    public interface Id {
        Id getParent();
        byte[] toBytes();        
    }
    
    public interface MetadataStream<T> {
        T next();
        Location getLocation();
    }
    
    public interface Location {
        byte[] toBytes();
    }
    
    public CompletableFuture<Void> createStructuredStream(String scope, String name);
    
    public StructuredStreamWriterFactory getStructuredStreamWriterFactory(String scope, String name);
    
    public interface StructuredStreamWriterFactory {
        public StructuredStreamWriter getChildStream(String name);
        public StructuredStreamWriter createChildStream(String name);
        public void deleteChildStream(String name);
        public List<String> listChildStreams();
    }
    
    public interface StructuredStreamWriter {
        public ByteStreamWriter newPayloadWriter();
        public void writePayload(ByteStreamWriter payload, Map<String, byte[]> metadata); //Passed ByteStreamWriter must have been created by this instance
        public void dropPayload(ByteStreamWriter payload);
    }

}
