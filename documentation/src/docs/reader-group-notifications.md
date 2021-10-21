<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Working with Pravega: ReaderGroup Notifications

The ReaderGroup api supports different types of notifications. Currently, we
have two types implemented, but we plan to add more over time.
The types we currently support are the following:

1. **Segment Notification**

A segment notification is triggered when the total number of segments managed by the
reader group changes. During a scale operation segments can be split into
multiple or merged into some other segment causing the total number of segments
to change. The total number of segments can also change when the configuration
of the reader group changes, for example, when it adds or removes a stream.

The method for subscribing to segment notifications is shown below
```java
@Cleanup
ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory,
        connectionFactory);
groupManager.createReaderGroup(GROUP_NAME, ReaderGroupConfig.builder().
        .stream(Stream.of(SCOPE, STREAM))
        .build());

groupManager.getReaderGroup(GROUP_NAME).getSegmentNotifier(executor).registerListener(segmentNotification -> {
    int numOfReaders = segmentNotification.getNumOfReaders();
    int segments = segmentNotification.getNumOfSegments();
    if (numOfReaders < segments) {
        //Scale up number of readers based on application capacity
    } else {
        //More readers available time to shut down some
    }
});

```
The application can register a listener to be notified of `SegmentNotification` using
the `registerListener` api. This api takes
`io.pravega.client.stream.notifications.Listener` as a parameter. Here the
application can add custom logic to change the set of online readers according
to the number of segments. For example, if the number of segments increases,
then application might consider increasing the number of online readers. If the
number of segments instead decreases according to a segment notification, then the
application might want to change the set of online readers accordingly.

2. **EndOfData Notification**

An end of data notifier is triggered when the readers have read all the data of
the stream(s) managed by the reader group. This is useful to process the stream
data with a batch job where the application wants to read data of sealed
stream(s).

The method for subscribing to end of data notifications is shown below
```java
@Cleanup
ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory,
        connectionFactory);
groupManager.createReaderGroup(GROUP_NAME, ReaderGroupConfig.builder()
        .stream(Stream.of(SCOPE, SEALED_STREAM))
        .build());

groupManager.getReaderGroup(GROUP_NAME).getEndOfDataNotifier(executor).registerListener(notification -> {
    //custom action e.g: close all readers
});

```
The application can register a listener to be notified of `EndOfDataNotification` using
the `registerListener` api. This api takes
`io.pravega.client.stream.notifications.Listener` as a parameter. Here the
application can add custom logic that can be invoked once all the data of the
sealed streams are read.
