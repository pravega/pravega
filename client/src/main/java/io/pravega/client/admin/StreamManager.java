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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import java.net.URI;
import java.util.Iterator;

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
    public static StreamManager create(URI controller) {
        return create(ClientConfig.builder().controllerURI(controller).build());
    }

    /**
     * Creates a new instance of StreamManager.
     *
     * @param clientConfig Configuration for the client connection to Pravega.
     * @return Instance of Stream Manager implementation.
     */
    public static StreamManager create(ClientConfig clientConfig) {
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
     * @param deleteStreams To list and delete streams in scope before attempting to delete scope. 
     * @return True if scope is deleted, false otherwise. 
     * @throws DeleteScopeFailedException is thrown if this method is unable to seal and delete a stream.  
     */
    boolean deleteScope(String scopeName, boolean deleteStreams) throws DeleteScopeFailedException;

    /**
     * Get information about a given Stream, {@link StreamInfo}.
     * This includes {@link StreamCut}s pointing to the current HEAD and TAIL of the Stream.
     *
     * @param scopeName The scope of the stream.
     * @param streamName The stream name.
     * @return stream information.
     */
    @Beta
    StreamInfo getStreamInfo(String scopeName, String streamName);

    /**
     * Closes the stream manager.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
