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
package io.pravega.client.stream;

import com.google.common.annotations.Beta;
import io.pravega.client.stream.notifications.ReaderGroupNotificationListener;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A reader group is a collection of readers that collectively read all the events in the
 * stream. The events are distributed among the readers in the group such that each event goes
 * to only one reader.
 *
 * The readers in the group may change over time. Readers are added to the group by calling
 * {@link io.pravega.client.EventStreamClientFactory#createReader(String, String, Serializer, ReaderConfig)}
 * and are removed by calling {@link #readerOffline(String, Position)}
 */
public interface ReaderGroup extends ReaderGroupNotificationListener, AutoCloseable {

    /**
     * Returns metrics for this reader group.
     * 
     * @return a ReaderGroupMetrics object for this reader group.
     */
    ReaderGroupMetrics getMetrics();
    
    /**
     * Returns the scope of the stream which the group is associated with.
     *
     * @return A scope string
     */
    String getScope();

    /**
     * Cancels the outStanding checkpoints.
     */

    void cancelOutstandingCheckpoints();

    /**
     * Returns the name of the group.
     *
     * @return Reader group name
     */
    String getGroupName();

    /**
     * Initiate a checkpoint. This causes all readers in the group to receive a special
     * {@link EventRead} that contains the provided checkpoint name. This can be used to provide an
     * indication to them that they should persist their state. Once all of the readers have
     * received the notification and resumed reading the future will return a {@link Checkpoint}
     * object which contains the StreamCut of the reader group at the time they received the
     * checkpoint. This can be used to reset the group to this point in the stream by calling
     * {@link ReaderGroup#resetReaderGroup(ReaderGroupConfig)} if the checkpoint fails or the result cannot be
     * obtained an exception will be set on the future.
     * 
     * This method can be called and a new checkpoint can be initiated while another is still in
     * progress if they have different names. If this method is called again before the
     * checkpoint has completed with the same name the future returned to the second caller will
     * refer to the same checkpoint object as the first.
     * 
     * @param checkpointName The name of the checkpoint (For identification purposes)
     * @param backgroundExecutor A threadPool that can be used to poll for the completion of the
     *            checkpoint.
     * @return A future Checkpoint object that can be used to restore the reader group to this
     *         position.
     */
    CompletableFuture<Checkpoint> initiateCheckpoint(String checkpointName, ScheduledExecutorService backgroundExecutor);

    /**
     * Initiate a checkpoint. This causes all readers in the group to receive a special
     * {@link EventRead} that contains the provided checkpoint name. This can be used to provide an
     * indication to them that they should persist their state. Once all of the readers have
     * received the notification and resumed reading the future will return a {@link Checkpoint}
     * object which contains the StreamCut of the reader group at the time they received the
     * checkpoint. This can be used to reset the group to this point in the stream by calling
     * {@link ReaderGroup#resetReaderGroup(ReaderGroupConfig)} if the checkpoint fails or the result cannot be
     * obtained an exception will be set on the future.
     *
     * This method can be called and a new checkpoint can be initiated while another is still in
     * progress if they have different names. If this method is called again before the
     * checkpoint has completed with the same name the future returned to the second caller will
     * refer to the same checkpoint object as the first.
     *
     * Client internal thread pool executor is used to poll for the completion of the checkpoint
     *
     * @param checkpointName The name of the checkpoint (For identification purposes)
     * @return A future Checkpoint object that can be used to restore the reader group to this
     *         position.
     */
    CompletableFuture<Checkpoint> initiateCheckpoint(String checkpointName);

    /**
     * Reset a reader group with the provided {@link ReaderGroupConfig}.
     *
     * <p>- The stream(s) that are part of the reader group
     * can be specified using {@link ReaderGroupConfig.ReaderGroupConfigBuilder#stream(String)},
     * {@link ReaderGroupConfig.ReaderGroupConfigBuilder#stream(String, StreamCut)} and
     * {@link ReaderGroupConfig.ReaderGroupConfigBuilder#stream(String, StreamCut, StreamCut)}.</p>
     * <p>- To reset a reader group to a given checkpoint use
     * {@link ReaderGroupConfig.ReaderGroupConfigBuilder#startFromCheckpoint(Checkpoint)} api.</p>
     * <p>- To reset a reader group to a given StreamCut use
     * {@link ReaderGroupConfig.ReaderGroupConfigBuilder#startFromStreamCuts(Map)}.</p>
     *
     * All existing readers will have to call
     * {@link io.pravega.client.EventStreamClientFactory#createReader(String, String, Serializer, ReaderConfig)}.
     * If they continue to read events they will eventually encounter an {@link ReinitializationRequiredException}.
     *
     * @param config The new configuration for the ReaderGroup.
     * To use a different checkpoint, set the `startingStreamCuts` on the `ReaderGroupConfig` from a streamcut 
     * obtained from a {@link Checkpoint} or {@link ReaderGroup#initiateCheckpoint(String, ScheduledExecutorService)}.
     */
    void resetReaderGroup(ReaderGroupConfig config);

    /**
     * Reset a reader group to successfully completed last checkpoint.
     * Successfully completed last checkpoint can be the last checkpoint created when automatic checkpointing is 
     * enabled as a part of {@link ReaderGroupConfig} or manually created by calling {@link ReaderGroup#initiateCheckpoint(String, ScheduledExecutorService)}
     * If there is no successfully completed Last checkpoint present then this call reset the reader group to the original streamcut from `ReaderGroupConfig`.
     */
    void resetReaderGroup();

    /**
     * Invoked when a reader that was added to the group is no longer consuming events. This will
     * cause the events that were going to that reader to be redistributed among the other
     * readers. Events after the lastPosition provided will be (re)read by other readers in the
     * {@link ReaderGroup}.
     *
     * Note that this method is automatically invoked by {@link EventStreamReader#close()}
     *
     * @param readerId The id of the reader that is offline.
     * @param lastPosition The position of the last event that was successfully processed by the
     *        reader.
     */
    void readerOffline(String readerId, Position lastPosition);

    /**
     * Returns a set of readerIds for the readers that are considered to be online by the group.
     * i.e. {@link io.pravega.client.EventStreamClientFactory#createReader(String, String, Serializer, ReaderConfig)}
     * was called but {@link #readerOffline(String, Position)} was not called subsequently.
     *
     * @return Set of active reader IDs of the group
     */
    Set<String> getOnlineReaders();

    /**
     * Returns the set of scoped stream names which was used to configure this group.
     *
     * @return Set of streams for this group.
     */
    Set<String> getStreamNames();

    /**
     * Returns a {@link StreamCut} for each stream that this reader group is reading from.
     * The stream cut corresponds to the last checkpointed read offsets of the readers, and
     * it can be used by the application as reference to such a position.
     * A more precise {@link StreamCut}, with the latest read offsets can be obtained using
     * {@link ReaderGroup#generateStreamCuts(ScheduledExecutorService)} API.
     *
     * @return Map of streams that this group is reading from to the corresponding cuts.
     */
    Map<Stream, StreamCut> getStreamCuts();

    /**
     * Generates a {@link StreamCut} after co-ordinating with all the readers using
     * {@link io.pravega.client.state.StateSynchronizer}. A {@link StreamCut} is
     * generated by using the latest segment read offsets returned by the readers
     * along with unassigned segments (if any).
     *
     * The configuration {@link ReaderGroupConfig#groupRefreshTimeMillis} decides
     * the maximum delay by which the readers return the latest read offsets of their
     * assigned segments.
     * <p>
     * The {@link StreamCut} generated by this API can be used by the application as a
     * reference to a position in the stream. This is guaranteed to be greater than or
     * equal to the position of the readers at the point of invocation of the API. The
     * {@link StreamCut}s generated can be used to perform bounded processing of the Stream
     * by configuring a {@link ReaderGroup} with a {@link ReaderGroupConfig} where the
     * {@link StreamCut}s are specified as the lower bound and/or upper bounds using the
     * apis
     * {@link ReaderGroupConfig.ReaderGroupConfigBuilder#stream(Stream, StreamCut, StreamCut)}
     * or {@link ReaderGroupConfig.ReaderGroupConfigBuilder#stream(Stream, StreamCut)} or
     * {@link ReaderGroupConfig.ReaderGroupConfigBuilder#startFromStreamCuts(Map)}.
     * <p>
     * Note: Generating a precise {@link StreamCut}, for example a {@link StreamCut} pointing to
     * end of Q1 across all segments, is difficult as it depends on the configuration
     * {@link ReaderGroupConfig#groupRefreshTimeMillis} which decides the duration by
     * which all the readers running on different machines/ processes respond with their
     * latest read offsets. Hence, the {@link StreamCut} would point to a position in the
     * {@link Stream} which might include events from Q2. The application thus would need to
     * filter out such additional events.
     *
     * @param backgroundExecutor A thread pool that will be used to poll if the
     *                           positions from all the readers have been fetched.
     * @return A future to a Map of Streams (that this group is reading from) to
     * its corresponding cuts.
     */
    @Beta
    CompletableFuture<Map<Stream, StreamCut>> generateStreamCuts(ScheduledExecutorService backgroundExecutor);

    /**
     * Update Retention Stream-Cut for Streams in this Reader Group.
     * See {@link ReaderGroupConfig.StreamDataRetention#MANUAL_RELEASE_AT_USER_STREAMCUT}
     * @param streamCuts A Map with a Stream-Cut for each Stream.
     *                   StreamCut indicates position in the Stream till which data has been consumed and can be deleted.
     */
    void updateRetentionStreamCut(Map<Stream, StreamCut> streamCuts);

    /**
     * Returns current distribution of number of segments assigned to each reader in the reader group. 
     *
     * @return an instance of ReaderSegmentDistribution which describes the distribution of segments to readers 
     * including unassigned segments.   
     */
    ReaderSegmentDistribution getReaderSegmentDistribution();

    /**
     * Closes the reader group, freeing any resources associated with it.
     */
    @Override
    public void close();
}
