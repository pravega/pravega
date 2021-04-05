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
# Watermarking

Stream-processing applications may require a time-bounded set of metrics to make a calculation, make a decision, or
commit a transaction. Pravega watermarking provides a way for applications to define time and know with some certainty
that it has a complete set of data up to a watermark. All events earlier than a watermark can be processed, acted upon,
or committed.

In Pravega stream processing, data ingress is typically handled by multiple Pravega Writers, and data processing is
typically handled by multiple Pravega Readers. In a parallel processing scenario, events can ingress out of order (even
though final order is guaranteed). Out of order ingress and the possibility of delayed Writers make it difficult for
a Reader application to know when a complete set of events exists for any given time window. How long should a Reader
wait before deciding that it has all of the events for a defined time window?  Watermarking satisfies that question.

In general, watermarking works like this:

* The concept of time is defined by the application.
* Pravega Writers report their time and positions in the stream (or use a feature that automatically timestamps events).  
* The Pravega controller periodically consolidates the reports into watermarks. The watermark defines both time and a
stream position.
* Pravega Readers request time windows and use them to navigate the stream. The lower bound of a returned time window is
the current watermark.
* The application can process data with some certainty that the data is complete up to the watermarked time and position.       

## Concepts related to watermarking

### The watermark

The controller produces data structures called watermarks. A watermark has three values:

* `lowerTimeBound` -  a lower bound time corresponding to the streamcut.  A watermark makes a weak claim that all events in
the stream after the `watermark.streamcut` have time greater than `watermark.lowerbound`.
* `upperTimeBound` -  an upper bound time corresponding to the streamcut. A watermark makes a weak suggestion that all
events it saw up to `watermark.streamcut` have an upper bound time of watermark.upperBound.
* `streamcut(position)` - an upper bound on positions from all writers with respect to the lower bound time.

A streamcut is a logical partition of a stream. It corresponds to a `segment to offset` mapping in the stream. Since a
stream is partitioned into multiple segments, a stream cut divides the stream such that you can perform bounded
processing before or after that stream cut. Since stream cut spans multiple segments, offsets in each segment correspond
to individual events written within those segments.

### Time window

A Reader requests a time window. A Time window contains two watermarks.

* `lowerTimeBound`
* `upperTimeBound`

In the following example, the two watermarks are W1 and W2.

```java
TimeWindow T1 = {W1.lowerbound, W2.upperbound}
```  

Pravega makes a weak claim that all events between `W1.streamCut` and `W2.streamCut` have times that fall in the
timewindow `T1`.

### Time  

The concept of time is defined by the application. The Pravega APIs provide the means for Writers to note the time and
for Readers to request the watermarked window of time, but the definition of time itself is controlled by the
application.

The time may indicate the time the data was written to the stream (ingestion time). Time may alternatively be equated
to event time (the time the event occurred).  Time can denote wall clock time, some other traditional time, or an
arbitrary value, like a file number that advances as the application ingests file data.

Time has two requirements:

* Time is a variable of type `long`.  
* Time must increase monotonically. That is, if time is 6, then a time of 5 is earlier, and a time of 7
is later.

## How Pravega Writers report time

A Writer reports time corresponding to a position. A position identifies the location within a stream where the Writer
last wrote something.

There are three ways for Pravega Writers to report time.

* Explicitly note the time  
* Note time on transaction commit
* Automatically note wall clock time

### Explicitly note time

The following API method notes time. The position is captured with time.

```java
writer.noteTime(long timestamp);  
```

A Writer can note time after every event write, after writing `n` number of events, or use it periodically to indicate
the time and position.

Here is example usage:

```java       
EventStreamWriter<EventType> writer = clientFactory.createEventWriter(stream, serializer,EventWriterConfig.builder().build());

      //... write events ...

writer.noteTime(currentTime);
```

### Note time on transaction commit

For transactional writes, the commit call can supply the timestamp. The following Writer method passes the time to the
Controller as part of the commit.
```java
void commit(long timestamp) throws TxnFailedException;
```

For example:

```java
Transaction<EventType> txn = writer.beginTxn();

      //... write events to transaction.

txn.commit(txnTimestamp);
```

During processing, all transaction segments are merged into parent segments. This can happen for a batch of
transactions. As they are merged, the merge offsets are recorded by the controller and composed into a position object
for where the transaction write completed. Within a committed batch of transactions, writer-specific times are calculated
and reported. The watermarking workflow uses these reported times and positions in its next periodic cycle to compute
the watermark bounds.

### Automatically attach wall clock time

You can set the `automaticallyNoteTime` option to true when a Writer is created. This option configures the Writer to
automatically attach a system time notation to every event. The option essentially attaches the ingest time to each
event. With this option turned on, no other calls to note the time are required.

Here is an example that configures automatic timestamps on events:

```java
EventStreamWriter<EventType> writer = clientFactory.createEventWriter(stream, serializer,
        EventWriterConfig.builder().automaticallyNoteTime(true).build());
```


## How Pravega Readers request watermark windows  

Multiple Pravega Readers working in parallel each have different current locations in the stream. The Controller
coordinates the Reader positions and can return a window that indicates where in the stream all of the readers are.

The following Reader API method requests the current watermark window.

`TimeWindow window = reader.getTimeWindow();`

The `TimeWindow` is returned as:

```java
public class TimeWindow {
    private final long lowerTimeBound;
    private final long upperTimeBound;
}
```

where:

* `lowerTimeBound` of the  window is the watermark. The Controller asserts that all readers are done reading all events
earlier than the watermark.
*  `upperTimeBound` is an arbitrary assignment that can help applications keep track of a moving window of time.

The lower bound is most important as it corresponds to the low watermark. It takes into account the position of all
of the readers and represents the time before which all events have been read, under the assumption that ingestion
respected this order. Events of course can arrive out of order, in which case the low watermark becomes only a strong
indication that all the events with a lower timestamp have been read. When using this time window for window
aggregation, there is a point in which any given window needs to be declared closed, so that processing can occur. For
example, say that we want to count occurrences every hour. When the lower time bound advances to the hour, the
application may choose to immediately close the window and perform the count or wait longer. The choice is up to the
application.

If a reader asks for a time window from a stream whose writers are not generating time stamps, then `null is returned.

The TimeWindow reflects the current position in the stream. It can be called following every `readNextEvent()` call if
desired, or just periodically to provide support for grouping events into windows.

## Preventing a stalled watermark

If a Pravega Writer goes down or is delayed and stops writing events, the Readers wait for possible delayed events from
that Writer. As a result, watermarks stop advancing.  While it is desirable for Readers to wait for delayed events, at
some point it makes sense to stop waiting. For example, perhaps the Writer is offline and is not coming back. The
controller must be allowed to emit a best attempt watermark so processing can continue.

To prevent Readers from waiting indefinitely for a Writer, you can configure the `timestampAggregationTimeout` parameter
on a stream. This parameter configures the amount of time after which a Writer that has not been heard from will be
excluded from the time window calculation.  Set this parameter on a stream when the stream is defined.

## Integration with applications

Typically, an application running a Pravega reader interested in processing with time obtains the low watermark from
the time window. The application then needs downstream operators to process the watermark. For example, Apache Flink
is a stream processing engine that can propagate watermarks very well. We have a Pravega Flink connector that can get
a watermark from the Pravega Reader with the `AssignerWithTimeWindows` interface, which integrates the Pravega watermark
feature with the periodic watermark in Apache Flink. All Flink applications using Pravega can gain the benefits of event
time or ingest time watermarking using all the standard Apache Flink APIs.


Watermarking support in Pravega is useful in general in event time windowing applications. The following example
application gets a Pravega watermark and uses Apache Flink to process it. It reads data from Pravega and calculates an
average in every event-time 10 minutes. The watermark is important to provide the signal in real time to start and close
every 10-minute window.  The example is [here](https://github.com/pravega/pravega-samples/blob/master/flink-connector-examples/src/main/java/io/pravega/example/flink/watermark/EventTimeAverage.java).
A more in-depth description of the the example is [here](https://github.com/pravega/pravega-samples/blob/master/flink-connector-examples/doc/watermark/README.md).
