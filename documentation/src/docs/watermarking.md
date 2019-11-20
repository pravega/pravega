# Watermarking




Watermarking establishes points in time (watermarks) where Pravega asserts that all earlier events are written and read. All events earlier than the watermark can be processed, acted upon, or committed. The application does not need to wait for any more earlier events for the watermarked window of time.  

In real-time stream processing, ingress is typically handled by multiple Pravega writers and reading is typically handled by mulitple Pravega readers. In this heavily parallel processing scenario, the Pravega controllers coordinate the positions of multiple writers and readers in the stream. Even so, events can ingress out of order and be read out of order, making it difficult for the application to know when a complete set of events exists for real-time calculations, decisionmaking, or transactional actions. How long should an application wait before deciding that it has all of the events for a defined time window?  Watermarking satisfies that question.

Watermarking depends on optional features in Pravega writers and readers.
* Pravega Writers must note time on each event (or use an optional feature that automatically timestamps events).
* Pravega Readers request a time window. The lower bound of that window is the watermark. The application can then act on or process events before the watermark, knowing that the data is complete up to the last watermark.   

Note: The watermark is a best ability mark. There is always the possibility that a reader is unusually delayed and recovers and processes an event that falls earlier than a watermark (earlier than a closed window of time). In such a case, it is up to the application to determine a recourse.

Uses of watermarking include applications that require a time bounded   set of metrics to make a calculation or make a decision. Transactional applications are sometimes time-bound.

The Flink API offers watermarking features. The Pravega watermarking feature mirrors the Flink functionality,   enabling an easier coding experience for integrating Pravega and Flink applications.

## More information

Read more about the Flink watermarking feature and example use cases <here>.
Read more about the Pravega watermarking design and usage in the Pravega blog <here>.

## About time

Watermarking is an optional feature in Pravega. If implemented, the concept of time is defined by the application. The APIs provide  the means for writers to note the time  and for readers to request the watermarked window of time, but the definition of  time itself is  controlled by the application.

The time may indicate the time the data was written to the stream (ingestion time).  Time may alternatively be equated to event time (the time the event occurred).  Time can denote wall clock time, some other traditional time, or an arbitrary value, like 6.    

Time has two requirements:

* Time is a variable of type long.  
* Time must increase in value (not decrease). That is, if time is 6, then a time of 5 is earlier, and a time of 7 is later.

## How Pravega Writers note the time

There are three ways for Pravega Writers to attach time to events.
* Explicitly note the time  
* Note time on transaction commit
* Automatically note wall clock time

### Explicitly note time

The following writer API method notes time.

`writer.noteTime(long timestamp);`

You may call the method after every event write, or use it periodically to indicate  the time below which all events are written.

Here is an example:       

      EventStreamWriter<EventType> writer = clientFactory.createEventWriter(stream, serializer,
                EventWriterConfig.builder().build());

      //... write events ...

      writer.noteTime(currentTime);

### Note time on transaction commit

For transactional writes, the commit call can supply the timestamp. The following Writer API method passes the time to the Controller as part of the commit. The  commit simultaneously commits the data and supplies the corresponding timestamp.

`void commit(long timestamp) throws TxnFailedException;`

For example:

    Transaction<EventType> txn = writer.beginTxn();
      //... write events to transaction.
    txn.commit(txnTimestamp);

### Automatically attach wall clock time

The `automaticallyNoteTime` option when a  Writer is created configures the   writer to automatically attach a wall clock time notation to every event. This option essentially attaches the ingest time to each event. With this option turned on, no other calls to note the time are required.

Here is the call to configure automatic timestamps on events:

    EventStreamWriter<EventType> writer = clientFactory.createEventWriter(stream, serializer,
                EventWriterConfig.builder().automaticallyNoteTime(true).build());


## How Pravega Readers request watermark windows  

Multiple Pravega Readers working in parallel each have different current locations in the stream. The Controller coordinates the Reader positions and can return a window that indicates where in the stream all of the readers are.  

The following Reader API method requests the current watermark window.

`TimeWindow window = reader.getTimeWindow();`

The `TimeWindow` is returned as:

    @Data
    public class TimeWindow {
        private final long lowerTimeBound;
        private final long upperTimeBound;
    }

where:
* `lowerTimeBound` of the  window is the watermark. The Controller asserts that all readers are done reading all events earlier than the watermark.
*  `upperTimeBound` is an arbitrary assignment that can help applications keep track of a moving window of time.

The lower bound is most important. The lower bound is labeled the watermark. It takes into account the position of all of the readers and represents the time before which all events are "guaranteed" to have been read. Events can arrive out of order but the watermark would indicate which window each event aggregrates into. At some point, a window needs to be declared closed, so that processing can occur.  Events can also arrive late, after its window has been closed. The action on such an occurrence is up to the application.

If a reader asks for a time window from a stream whose writers are not generating time stamps, then null is returned.  

The TimeWindow reflects the current position in the stream. It can be called following every `readNextEvent()` call if desired, or just periodically to provide support for grouping events into windows.

### Reader Example

Here is an example of a Reader application that...

   >>>>MISSING: I think a reader example that simply requests a window, and does something useful as a result (computes an average or something) and prints a message stating average accurate up to <watermarked lower bound>. I don't understand what the example in the blog is doing. It contains some Flink calls, I think, which complicates the concepts.

### Preventing a stalled watermark

It could happen that one of the Pravega Writers goes down or is delayed and stops writing events. In that case, it is desirable for readers to wait for the delayed events to arrive, and the watermarks stop advancing. At some point, however, it  makes sense to stop waiting (maybe the writer is offline and is not coming back). At some point, the controller must be allowed to emit a best attempt watermark, and let processing continue.   

The variable in the above scenario is the wait time--how long to wait for a missing writer?    To prevent waiting indefinitely for a writer,   configure the    `timestampAggregationTimeout` parameter on  each stream. This parameter configures the time after which a writer that has not been heard from will be excluded from the time window calculation.    

The following example shows how to set the timeout value on a stream:
 >>>> MISSING: need example using the `timestampAggregationTimeout` parameter in a call
