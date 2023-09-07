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
package io.pravega.client.admin;

import com.google.common.annotations.Beta;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.TransactionInfo;

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.List;

/**
 * Used to create, delete, and manage Streams and ReaderGroups.
 */
public interface StreamManager extends AutoCloseable {
    /**
     * Creates a new instance of StreamManager.
     *
     * @param controller The Controller URI.
     * @return Instance of Stream Manager implementation.
     */
    static StreamManager create(URI controller) {
        return create(ClientConfig.builder().controllerURI(controller).build());
    }

    /**
     * Creates a new instance of StreamManager.
     *
     * @param clientConfig Configuration for the client connection to Pravega.
     * @return Instance of Stream Manager implementation.
     */
    static StreamManager create(ClientConfig clientConfig) {
        return new StreamManagerImpl(clientConfig);
    }

    /**
     * Creates a new stream
     * <p>
     * Note: This method is idempotent assuming called with the same name and config. This method
     * may block.
     *
     * @param scopeName  The name of the scope to create this stream in.
     * @param streamName The name of the stream to be created.
     * @param config The configuration the stream should use.
     * @return True if stream is created
     */
    boolean createStream(String scopeName, String streamName, StreamConfiguration config);

    /**
     * Change the configuration for an existing stream.
     * <p>
     * Note:
     * This method is idempotent assuming called with the same name and config.
     * This method may block.
     *
     * @param scopeName  The name of the scope to create this stream in.
     * @param streamName The name of the stream who's config is to be changed.
     * @param config     The new configuration.
     * @return True if stream configuration is updated
     */
    boolean updateStream(String scopeName, String streamName, StreamConfiguration config);

    /**
     * Truncate stream at given stream cut.
     * This method may block.
     *
     * @param scopeName  The name of the scope to create this stream in.
     * @param streamName The name of the stream who's config is to be changed.
     * @param streamCut  The stream cut to truncate at.
     * @return True if stream is truncated at given truncation stream cut.
     */
    boolean truncateStream(String scopeName, String streamName, StreamCut streamCut);

    /**
     * Seal an existing stream.
     *
     * @param scopeName  The name of the scope to create this stream in.
     * @param streamName The name of the stream which has to be sealed.
     * @return True if stream is sealed
     */
    boolean sealStream(String scopeName, String streamName);
    
    /**
     * Deletes the provided stream. No more events may be written or read.
     * Resources used by the stream will be freed.
     *
     * @param scopeName  The name of the scope to create this stream in.
     * @param toDelete The name of the stream to be deleted.
     * @return True if stream is deleted
     */
    boolean deleteStream(String scopeName, String toDelete);

    /**
     * Gets an iterator for all scopes. 
     *
     * @return Iterator to iterate over all scopes. 
     */
    Iterator<String> listScopes();

    /**
     * Creates a new scope.
     *
     * @param scopeName  The name of the scope to create this stream in.
     * @return True if scope is created
     */
    boolean createScope(String scopeName);

    /**
     * Checks if a scope exists. 
     *
     * @param scopeName  The name of the scope to check.
     * @return True if scope exists.
     */
    boolean checkScopeExists(String scopeName);

    /**
     * Gets an iterator for all streams in scope. 
     * 
     * @param scopeName The name of the scope for which to list streams in.
     * @return Iterator of Stream to iterator over all streams in scope. 
     */
    Iterator<Stream> listStreams(String scopeName);

    /**
     * Gets an iterator to list all streams with the provided tag.
     *
     * @param scopeName The name of the scope for which to list streams in.
     * @param tagName The name of the tag.
     *
     * @return Iterator of Stream to iterator over all streams in scope with the provided tag.
     */
    Iterator<Stream> listStreams(String scopeName, String tagName);

    /**
     * Gets the Tags associated with a stream.
     *
     * @param scopeName Scope name.
     * @param streamName Stream name.
     * @return Tags associated with the stream.
     */
    Collection<String> getStreamTags(String scopeName, String streamName);

    /**
     * Checks if a stream exists in scope. 
     *
     * @param scopeName  The name of the scope to check the stream in.
     * @param streamName  The name of the stream to check.
     * @return True if stream exists.
     */
    boolean checkStreamExists(String scopeName, String streamName);

    /**
     * Deletes an existing scope. The scope must contain no
     * stream. This is same as calling {@link #deleteScope(String, boolean)} with deleteStreams flag set to false. 
     *
     * @param scopeName  The name of the scope to delete.
     * @return True if scope is deleted
     */
    boolean deleteScope(String scopeName);

    /**
     * Deletes scope by listing and deleting all streams in scope. This method is not atomic and if new streams are added 
     * to the scope concurrently, the attempt to delete the scope may fail. Deleting scope is idempotent and failure to 
     * delete scope is retry-able.  
     *
     * @param scopeName  The name of the scope to delete.
     * @param forceDelete To list and delete streams, key-value tables and reader groups in scope before attempting to delete scope.
     * @return True if scope is deleted, false otherwise. 
     * @throws DeleteScopeFailedException is thrown if this method is unable to seal and delete a stream.
     *
     * @deprecated As of Pravega release 0.11.0, replaced by {@link #deleteScopeRecursive(String)}.
     */
    @Deprecated
    boolean deleteScope(String scopeName, boolean forceDelete) throws DeleteScopeFailedException;

    /**
     * Deletes scope by listing and deleting all streams/RGs/KVTs in scope.
     * New streams/RGs/KVTs can not be added to the scope if this
     * method is called.
     *
     * @param scopeName  The name of the scope to delete.
     * @return True if scope is deleted, false otherwise.
     * @throws DeleteScopeFailedException is thrown if this method is unable to seal and delete a stream.
     */
    boolean deleteScopeRecursive(String scopeName) throws DeleteScopeFailedException;

    /**
     * Re-read an event that was previously read, by passing the pointer returned from
     * {@link EventRead#getEventPointer()}.
     * This does not affect the current position of any reader.
     * <p>
     * This is a non-blocking call. Passing an EventPointer of a stream that has been deleted or data truncated away it will throw exception.
     * @param pointer It is an EventPointer obtained from the result of a previous readNextEvent call.
     * @param deserializer The Serializer
     * @param <T> The type of the Event
     * @return A future for the provided EventPointer of the fetch call. If an exception occurred, it will be completed with the causing exception.
     * Notable exception is {@link io.pravega.client.segment.impl.NoSuchEventException}
     */
    <T> CompletableFuture<T> fetchEvent(EventPointer pointer, Serializer<T> deserializer);

    /**
     * List most recent completed (COMMITTED/ABORTED) transactions.
     * It will return transactionId, transaction status and stream.
     *
     * @param stream The name of the stream for which to list transactionInfo.
     * @return List of TransactionInfo.
     */
    List<TransactionInfo> listCompletedTransactions(Stream stream);

    /**
     * Fetch information about a given Stream {@link StreamInfo} from server asynchronously.
     * This includes {@link StreamCut}s pointing to the current HEAD and TAIL of the Stream and the current {@link StreamConfiguration}.
     * Call join() on future object to get {@link StreamInfo}.
     *
     * @param scopeName The scope of the stream.
     * @param streamName The stream name.
     * @return A future representing {@link StreamInfo} that will be completed when server responds.
     */
    @Beta
    CompletableFuture<StreamInfo> fetchStreamInfo(String scopeName, String streamName);

    /**
     * Closes the stream manager.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();

    /**
     * Fetch the distance between two streamCuts in bytes.
     *
     * @param stream The stream corresponding with the streamCuts.
     * @param fromStreamCut start streamCut.
     * @param toStreamCut end streamCut.
     * @return A future containing {@link Long} value which is the distance between two streamCuts in bytes.
     */
    CompletableFuture<Long> getDistanceBetweenTwoStreamCuts(Stream stream, StreamCut fromStreamCut, StreamCut toStreamCut);
}
