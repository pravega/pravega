/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.stream;

import io.pravega.ClientFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A reader group is a collection of readers that collectively read all the events in the
 * stream. The events are distributed among the readers in the group such that each event goes
 * to only one reader.
 *
 * The readers in the group may change over time. Readers are added to the group by calling
 * {@link ClientFactory#createReader(String, String, Serializer, ReaderConfig)} and are removed by
 * calling {@link #readerOffline(String, Position)}
 */
public interface ReaderGroup {

    /**
     * Returns the scope of the stream which the group is associated with.
     *
     * @return A scope string
     */
    String getScope();

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
     * received the notification, a {@link Checkpoint} object will be returned. This can be used to
     * reset the group to this point in the stream by calling
     * {@link #resetReadersToCheckpoint(Checkpoint)} if the checkpoint fails or the result cannot be
     * obtained an exception will be set on the future.
     * 
     * This method can be called and a new checkpoint can be initiated while another is still in
     * progress if they have different names. If this method is is called again before the
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
     * Given a Checkpoint, restore the reader group to the provided checkpoint. All readers in the
     * group will encounter a {@link ReinitializationRequiredException} and when they rejoin the
     * group they will resume from the position the provided checkpoint was taken. (The mapping of
     * segments to readers may not be the same, and the current readers need not be the same ones as
     * existed at the time of the checkpoint.)
     * 
     * @param checkpoint The checkpoint to restore to.
     */
    void resetReadersToCheckpoint(Checkpoint checkpoint);
    
    /**
     * Updates a reader group. All existing readers will have to call
     * {@link ClientFactory#createReader(String, String, Serializer, ReaderConfig)} . If they continue to read
     * events they will eventually encounter an {@link ReinitializationRequiredException}.
     * 
     * Readers connecting to the group will start from the point defined in the config, exactly as though it
     * were a new reader group.
     * 
     * @param config The configuration for the new ReaderGroup.
     * @param streamNames The name of the streams the reader will read from.
     */
    void alterConfig(ReaderGroupConfig config, Set<String> streamNames);
    
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
     * i.e. {@link ClientFactory#createReader(String, String, Serializer, ReaderConfig)} was called but
     * {@link #readerOffline(String, Position)} was not called subsequently.
     *
     * @return Set of active reader IDs of the group
     */
    Set<String> getOnlineReaders();
}
