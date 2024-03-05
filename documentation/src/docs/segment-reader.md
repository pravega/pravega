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
# Introduction to Segment Reader
Instead of processing all the data of stream, segment reader provides a way to process the data from a specific segment. To
create a segment reader, user needs to initialize segment reader manager.
# How to initialize Segment Reader Manager
Segment reader manager provides a way to manage segment reader. `getSegmentReaders` API returns list of SegmentReader based 
on the provided StreamCut. For StreamCut `StreamCut.UNBOUNDED`, it fetches head StreamCut and then returns list of SegmentReader.
```java
URI controllerURI = URI.create("tcp://localhost:9090");

ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();

SegmentReaderManager<String> segmentReaderManager = SegmentReaderManager.create(clientConfig, serializer);
List<SegmentReader<String>> segmentReaderList = segmentReaderManager.getSegmentReaders(Stream.of(scope, stream), StreamCut.UNBOUNDED).join();
```
Once done with `SegmentReaderManager`, user needs to close this segment reader manager. No further methods may be called after close.
```java
segmentReaderManager.close();
```
# Segment Reader
SegmentReader exposes following APIs :
```java
T read(long timeoutMillis) throws TruncatedDataException, EndOfSegmentException;

CompletableFuture<Void> isAvailable();

SegmentReaderSnapshot getSnapshot();
```
`read` API provides a way to read next available event from given segment. If there are no events currently available,
this will block up for timeoutMillis waiting for them to arrive. In the case if timeoutMillis is reached, it will return null.
If the segment has been truncated beyond the current offset and the data cannot be read then it will return `TruncatedDataException`.
In the next read call, segment reader will automatically fetch head streamcut and start processing from there. If segment 
is sealed and reader reached at the end of segment then it will return `EndOfSegmentException`.

`isAvailable` API returns a future that will be completed when there is data available to be read. 

`getSnapshot` API returns the current snapshot of segment reader. It will provide information about segment reader's position,
segment itself and whether the reader has processed all events within the segment.
```java
//Try to read all the data from segment.
while (true) {
    try {
        reader.read(timeout);
    } catch (EndOfSegmentException e) {
        break;
    } catch (TruncatedDataException e) {
        //Truncated data found.
    }
}
```
Once done with `SegmentReader`, user needs to close this segment reader. No further methods may be called after close.
```java
segmentReader.close();
```


