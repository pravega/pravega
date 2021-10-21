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
package io.pravega.client.stream.notifications;

import java.util.concurrent.ScheduledExecutorService;

/**
 * ReaderGroup notification listener interface. This has the list of notifications supported by ReaderGroup.
 */
public interface ReaderGroupNotificationListener {

    /**
     * Get a segment notifier for a given reader group.
     * <br>
     * A segment notifier is triggered when the total number of segments managed by the ReaderGroup changes.
     * During a scale operation segments can be split into multiple or merge into some other segment causing the total
     * number of segments to change. The total number of segments can also change when configuration of the
     * reader group is changed, for example modify the configuration of a reader group to add/remove a stream.
     * <P>
     * Note:
     * <br>
     * * In case of a seal stream operation the segments are sealed and the segment has no successors. In this case
     * the notifier is not triggered.
     *
     * @param executor executor on which the listeners run.
     * @return Observable of type SegmentNotification.
     *
     */
    Observable<SegmentNotification> getSegmentNotifier(final ScheduledExecutorService executor);

    /**
     * Get an end of data notifier for a given reader group.
     * <br>
     * An end of data notifier is triggered when the readers have read all the data of the stream(s) managed by the
     * reader group. This is useful to process the stream data with a batch job where the application wants to read data
     * of sealed stream(s).
     * <P>
     * Note:
     * <br>
     * * In case of a reader group managing streams, where not all streams are sealed, then EndOfDataNotification notifier
     * is never triggered since readers continue reading the unsealed stream once it has completed reading the sealed
     * stream.
     *
     * @param executor executor on which the listeners run.
     * @return Observable of type EndOfDataNotification.
     */
    Observable<EndOfDataNotification> getEndOfDataNotifier(final ScheduledExecutorService executor);
}
