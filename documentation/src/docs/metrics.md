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

# Pravega Metrics

* [Metrics Interfaces and Examples Usage](#metrics-interfaces-and-examples-usage)
   - [Metrics Service Provider — Interface StatsProvider](#metrics-service-provider--interface-statsprovider)
   - [Metric Logger — Interface StatsLogger](#metric-logger--interface-statslogger)
   - [Metric Sub Logger — OpStatsLogger](#metric-sub-logger--opstatslogger)
   - [Metric Logger — Interface DynamicLogger](#metric-logger--interface-dynamiclogger)
* [Example for Starting a Metric Service](#example-for-starting-a-metric-service)
   - [Example for Dynamic Counter and OpStatsLogger(Timer)](#example-for-dynamic-counter-and-opstatsloggertimer)
   - [Example for Dynamic Gauge](#example-for-dynamic-gauge)
   - [Example for Dynamic Meter](#example-for-dynamic-meter)
* [Metric Registries and Configurations](#metric-registries-and-configurations)
* [Creating Own Metrics](#creating-own-metrics)
* [Metrics Naming Conventions](#metrics-naming-conventions)
* [Available Metrics and Their Names](#available-metrics-and-their-names)
   - [Metrics in JVM](#metrics-in-jvm)
   - [Metrics in Segment Store Service](#metrics-in-segment-store-service)
   - [Metrics in Controller Service](#metrics-in-controller-service)
* [Resources](#resources)

In the Pravega Metrics Framework, we use [Micrometer Metrics](https://micrometer.io/docs) as the underlying library, and provide our own API to make it easier to use.

# Metrics Interfaces and Examples Usage

- `StatsProvider`: The Statistics Provider which provides the whole Metric service.

- `StatsLogger`: The Statistics Logger is where the required Metrics ([Counter](https://micrometer.io/docs/concepts#_counters)/[Gauge](https://micrometer.io/docs/concepts#_gauges)/[Timer](https://micrometer.io/docs/concepts#_timers)/[Distribution Summary](https://micrometer.io/docs/concepts#_distribution_summaries)) are registered.

- `OpStatsLogger`: The Operation Statistics Logger is a sub-metric for the complex ones ([Timer](https://micrometer.io/docs/concepts#_timers)/[Distribution Summary](https://micrometer.io/docs/concepts#_distribution_summaries)). It is included in `StatsLogger` and `DynamicLogger`.


## Metrics Service Provider — Interface StatsProvider

Pravega Metric Framework is initiated using the `StatsProvider` interface: it provides the _start_ and _stop_ methods for the Metric service. It also provides `startWithoutExporting()` for testing purpose, which only stores metrics in memory without exporting them to external systems. Currently we have support for [InfluxDB](https://www.influxdata.com/), [Prometheus](https://prometheus.io), and [StatsD](https://github.com/b/statsd_spec) registries.

[StatsProvider](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/StatsProvider.java)

- `start()`: Initializes the [MetricRegistry](https://micrometer.io/docs/concepts#_registry) and Reporters for our Metric service.
- `startWithoutExporting()`: Initializes `SimpleMeterRegistry` that holds the latest value of each Meter in memory and does not export the data anywhere, typically for unit tests.
- `close()`: Shuts down the Metric Service.
- `createStatsLogger()`: Create a `StatsLogger` instance which is used to register and return metric objects. Application code could then perform metric operations directly with the returned metric objects.
- `createDynamicLogger()`: Creates a Dynamic Logger.

## Metric Logger — Interface StatsLogger

This interface can be used to register the required metrics for simple types like [Counter](https://micrometer.io/docs/concepts#_counters) and [Gauge](https://micrometer.io/docs/concepts#_gauges) and some complex statistics type of Metric like `OpStatsLogger`, through which we provide [Timer](https://micrometer.io/docs/concepts#_timers) and
[Distribution Summary](https://micrometer.io/docs/concepts#_distribution_summaries).

[StatsLogger](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/StatsLogger.java)

- `createStats()`: Register and get a `OpStatsLogger`, which is used for complex type of metrics. Notice the optional metric tags.
- `createCounter()`: Register and get a [Counter](https://micrometer.io/docs/concepts#_counters) Metric.
- `createMeter()`: Create and register a [Meter](https://micrometer.io/docs/concepts#_meters) Metric.
- `registerGauge()`: Register a [Gauge](https://micrometer.io/docs/concepts#_gauges) Metric.
- `createScopeLogger()`: Create the `StatsLogger` under the given scope name.

## Metric Sub Logger — OpStatsLogger

`OpStatsLogger` can be used if the user is interested in measuring the latency of operations like `CreateSegment` and `ReadSegment`. Further, we could use it to record the _number of operation_ and _time/duration_ of each operation.

[OpStatsLogger](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/OpStatsLogger.java)

- `reportSuccessEvent()`: Used to track the [Timer](https://micrometer.io/docs/concepts#_timers) of a successful operation and will record the latency in nanoseconds in the required metric.
- `reportFailEvent()`: Used to track the [Timer](https://micrometer.io/docs/concepts#_timers) of a failed operation and will record the latency in nanoseconds in required metric.  
- `reportSuccessValue()`: Used to track the [Histogram](https://micrometer.io/docs/concepts#_histograms_and_percentiles) of a success value.
- `reportFailValue()`: Used to track the [Histogram](https://micrometer.io/docs/concepts#_histograms_and_percentiles) of a failed value.
- `toOpStatsData()`:  Used to support the [JMX](https://metrics.dropwizard.io/3.1.0/manual/core/#jmx) Reporters and unit tests.
- `clear`: Used to clear the stats for this operation.

## Metric Logger — Interface DynamicLogger

The following is an example of a simple interface that exposes only the simple type metrics: ([Counter](https://micrometer.io/docs/concepts#_counters)/[Gauge](https://micrometer.io/docs/concepts#_gauges)/[Meter](https://micrometer.io/docs/concepts#_meters)).

[DynamicLogger](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/DynamicLogger.java)

 - `incCounterValue()`: Increases the [Counter](https://micrometer.io/docs/concepts#_counters) with the given value. Notice the optional metric tags.
 - `updateCounterValue()`: Updates the [Counter](https://micrometer.io/docs/concepts#_counters) with the given value.
 - `freezeCounter()`: Notifies that, the [Counter](https://micrometer.io/docs/concepts#_counters) will not be updated.
 - `reportGaugeValue()`: Reports the [Gauge](https://micrometer.io/docs/concepts#_gauges) value.
 - `freezeGaugeValue()`: Notifies that, the [Gauge](https://micrometer.io/docs/concepts#_gauges) value will not be updated.
 - `recordMeterEvents()`: Records the occurrences of a given number of events in [Meter](https://micrometer.io/docs/concepts#_meters).


# Example for Starting a Metric Service

This example is from `io.pravega.segmentstore.server.host.ServiceStarter`. The code for this example can be found [here](https://github.com/pravega/pravega/blob/master/segmentstore/server/host/src/main/java/io/pravega/segmentstore/server/host/ServiceStarter.java). It starts Pravega Segment Store service and the Metrics Service is started as a sub service.

## Example for Dynamic Counter and OpStatsLogger(Timer)

This is an example from `io.pravega.segmentstore.server.host.stat.SegmentStatsRecorderImpl.java`. The code for this example can be found [here](https://github.com/pravega/pravega/blob/master/segmentstore/server/host/src/main/java/io/pravega/segmentstore/server/host/stat/SegmentStatsRecorderImpl.java). In the class `PravegaRequestProcessor`, we have registered two metrics:

- one Timer (`createStreamSegment`)
- one dynamic counter (`dynamicLogger`)

From the above example, we can see the required steps to register and use dynamic counter:

 1. Get a dynamic logger from MetricsProvider:
    ```
     DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();
    ```
2. Increase the counter by providing metric base name and optional tags associated with the metric.
    ```
     DynamicLogger dl = getDynamicLogger();
     dl.incCounterValue(globalMetricName(SEGMENT_WRITE_BYTES), dataLength);
     ...
     dl.incCounterValue(SEGMENT_WRITE_BYTES, dataLength, segmentTags(streamSegmentName));
    ```
Here `SEGMENT_WRITE_BYTES` is the base name of the metric. Below are the two metrics associated with it:

- The global Counter which has no tags associated.
- A Segment specific Counter which has a list of Segment tags associated.

Note that, the `segmentTags` is a method to generate tags based on fully qualified Segment name.

The following are the required steps to register and use `OpStatsLogger(Timer)`:


1. Get a `StatsLogger` from `MetricsProvider`.

   ```
     StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger("segmentstore");
   ```
2. Register all the desired metrics through `StatsLogger`.
   ```
     @Getter(AccessLevel.PROTECTED)
     final OpStatsLogger createStreamSegment = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);
   ```
3. Use these metrics within code at the appropriate places where the values should be collected and recorded.

   ```
     getCreateStreamSegment().reportSuccessEvent(elapsed);
   ```
 Here `SEGMENT_CREATE_LATENCY` is the name of this metric, and `createStreamSegment` is the metric object, which tracks operations of `createSegment` and we will get the time (i.e. time taken by each operation and other numbers computed based on them) for each `createSegment` operation happened.

## Example for Dynamic Gauge

This is an example from `io.pravega.controller.metrics.StreamMetrics`. In this class, we report
a Dynamic Gauge which represents the open Transactions of a Stream. The code for this example can be found [here](https://github.com/pravega/pravega/blob/master/controller/src/main/java/io/pravega/controller/metrics/StreamMetrics.java).

## Example for Dynamic Meter

This is an example from `io.pravega.segmentstore.server.SegmentStoreMetrics`. The code for this example can be found [here](https://github.com/pravega/pravega/blob/master/segmentstore/server/src/main/java/io/pravega/segmentstore/server/SegmentStoreMetrics.java). In the class `SegmentStoreMetrics`, we report a Dynamic Meter which represents the Segments created with a particular container.


# Metric Registries and Configurations

With Micrometer, each meter registry is responsible for both storage and exporting of metrics objects.
In order to have a unified interface, Micrometer provides the `CompositeMeterRegistry` for the application to interact with, `CompositeMeterRegistry` will forward metric operations to all the concrete registries bounded to it.

Note that when metrics service `start()`, initially only a global registry (of type `CompositeMeterRegistry`) is provided, which will bind concrete registries (e.g. statsD, Influxdb) based on the configurations. If no registry is switched on in `config`, metrics service throws error to prevent the global registry runs into no-op mode.

Mainly for testing purpose, metrics service can also `startWithoutExporting()`, where a `SimpleMeterRegistry` is bound to the global registry. `SimpleMeterRegistry` holds memory only storage but does not export metrics, makes it ideal for tests to verify metrics objects.

Currently Pravega supports the following:
- StatsD registry in `Telegraf` flavor.
- Dimensional metrics data model (or metric tags).
- UDP as Communication protocol.
- Direct InfluxDB connection.

The reporter could be configured using the `MetricsConfig`. Please refer to the [example](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/MetricsConfig.java).


# Creating Own Metrics

1. When starting a Segment Store/Controller Service, start a Metric Service as a sub service. Please check [`ServiceStarter.start()`](#example-for-starting-a-metric-service)

   ```java
    public class AddMetrics {
           MetricsProvider.initialize(Config.METRICS_CONFIG);
           statsProvider.start(metricsConfig);
           statsProvider = MetricsProvider.getMetricsProvider();
           statsProvider.start();

   ```

2. Create a new `StatsLogger` instance through the `MetricsProvider.createStatsLogger(String loggerName)`, and register metric using name, e.g. `STATS_LOGGER.createCounter(String name)`; and then update the metric object as appropriately in the code.

   ```java
    static final StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger(); // <--- 1
    DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();

     static class Metrics { // < --- 2
        //Using Stats Logger
        static final String CREATE_STREAM = "stream_created";
        static final OpStatsLogger CREATE_STREAM = STATS_LOGGER.createStats(CREATE_STREAM);
        static final String SEGMENT_CREATE_LATENCY = "segmentstore.segment.create_latency_ms";
        static final OpStatsLogger createStreamSegment = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);

        //Using Dynamic Logger
        static final String SEGMENT_READ_BYTES = "segmentstore.segment.read_bytes";  //Dynamic Counter
        static final String OPEN_TRANSACTIONS = "controller.transactions.opened";    //Dynamic Gauge
        ...
      }

    //to report success or increment
    Metrics.CREATE_STREAM.reportSuccessValue(1); // < --- 3
    Metrics.createStreamSegment.reportSuccessEvent(timer.getElapsed());
    dynamicLogger.incCounterValue(Metrics.SEGMENT_READ_BYTES, 1);
    dynamicLogger.reportGaugeValue(OPEN_TRANSACTIONS, 0);

    //in case of failure
    Metrics.CREATE_STREAM.reportFailValue(1);
    Metrics.createStreamSegment.reportFailEvent(timer.getElapsed());

    //to freeze
    dynamicLogger.freezeCounter(Metrics.SEGMENT_READ_BYTES);
    dynamicLogger.freezeGaugeValue(OPEN_TRANSACTIONS);
    }

   ```
# Metrics Naming Conventions

All metric names are in the following format:

```
Metrics Prefix + Component Origin + Sub-Component (or Abstraction) + Metric Base Name
```
1. **Metric Prefix**: By default `pravega` is configurable.

2. **Component Origin**: Indicates which component generates the metric, such as `segmentstore`, `controller`.

3. **Sub-Component (or Abstraction)**: Indicates the second level component or abstraction, such as `cache`, `transaction`, `storage`.

4. **Metric Base Name**: Indicates the `read_latency_ms`, `create_count`.

For example:
 ```
     pravega.segmentstore.segment.create_latency_ms
  ```
Following are some common combinations of component and sub-components (or abstractions) being used:

- `segmentstore.segment`: Metrics for individual Segments
- `segmentstore.storage`: Metrics related to long-term storage (Tier 2)
- `segmentstore.bookkeeper`: Metrics related to Bookkeeper (Tier 1)
- `segmentstore.container`: Metrics for Segment Containers
- `segmentstore.thread_pool`: Metrics for Segment Store thread pool
- `segmentstore.cache`: Cache-related metrics
- `controller.stream`: Metrics for operations on Streams (e.g., number of streams created)
- `controller.segments`: Metrics about Segments, per Stream (e.g., count, splits, merges)
- `controller.transactions`: Metrics related to Transactions (e.g., created, committed, aborted)
- `controller.retention`: Metrics related to data retention, per Stream (e.g., frequency, size of truncated data)
- `controller.hosts`: Metrics related to Pravega servers in the cluster (e.g., number of servers, failures)
- `controller.container`: Metrics related to Container lifecycle (e.g., failovers)

Following are the two types of metrics:

1. **Global Metric**: `_global` metrics are reporting global values per component (Segment Store or Controller) instance, and further aggregation logic is needed if looking for Pravega cluster globals.
For instance, `STORAGE_READ_BYTES` can be classified as a Global metric.

2. **Object-based Metric**: Sometimes, we need to report metrics only based on specific objects, such as Streams or Segments. This kind of metrics use metric name as a base name in the file and are "dynamically" created based on the objects to be measured.
For instance, in `CONTAINER_APPEND_COUNT` we actually report multiple metrics, one per each
`containerId` measured, with different container tag (e.g. `["containerId", "3"]`).

There are cases in which we may want both a _Global_ and _Object-based_ versions for the same metric. For example, regarding `SEGMENT_READ_BYTES` we publish the Global version of it by adding `_global` suffix to the base name
```
  segmentstore.segment.read_bytes_global
```
to track the globally total number of bytes read, as well as the per-segment version of it by using the same base name and also supplying additional Segment tags to report in a finer granularity the events read per Segment.

```
segmentstore.segment.read_bytes, ["scope", "...", "stream", "...", "segment", "...", "epoch", "..."])
```

# Available Metrics and Their Names

## Metrics in JVM

   ```
    jvm_gc_live_data_size
    jvm_gc_max_data_size
    jvm_gc_memory_allocated
    jvm_gc_memory_prompted
    jvm_gc_pause
    jvm_memory_committed
    jvm_memory_max
    jvm_memory_used
    jvm_threads_daemon
    jvm_threads_live
    jvm_threads_peak
    jvm_threads_states    

   ```
## Metrics in Segment Store Service

- Segment Store Read/Write latency of storage operations ([Histograms](https://micrometer.io/docs/concepts#_histograms_and_percentiles)):

   ```
      segmentstore.segment.create_latency_ms
      segmentstore.segment.read_latency_ms
      segmentstore.segment.write_latency_ms

  ```

- Segment Store global and per-segment Read/Write Metrics ([Counters](https://micrometer.io/docs/concepts#_counters)):

   ```
      // Global counters
         segmentstore.segment.read_bytes_global
         segmentstore.segment.write_bytes_global
         segmentstore.segment.write_events_global

      // Per segment counters - all with tags {"scope", $scope, "stream", $stream, "segment", $segment, "epoch", $epoch}

        segmentstore.segment.write_bytes
        segmentstore.segment.read_bytes
        segmentstore.segment.write_events
  ```

- Segment Store cache Read/Write latency Metrics ([Histogram](https://micrometer.io/docs/concepts#_histograms_and_percentiles)):

  ```
    segmentstore.cache.insert_latency_ms
    segmentstore.cache.get_latency
  ```

- Segment Store cache Read/Write Metrics ([Counters](https://micrometer.io/docs/concepts#_counters)):

  ```
    segmentstore.cache.write_bytes
    segmentstore.cache.read_bytes
  ```

- Segment Store cache size ([Gauge](https://micrometer.io/docs/concepts#_gauges)) and generation spread ([Histogram](https://micrometer.io/docs/concepts#_histograms_and_percentiles)) Metrics:

  ```
    segmentstore.cache.size_bytes
    segmentstore.cache.gen
  ```

- Tier 1 Storage `DurableDataLog` Read/Write latency and queuing Metrics ([Histogram](https://micrometer.io/docs/concepts#_histograms_and_percentiles)):

  ```
    segmentstore.bookkeeper.total_write_latency_ms
    segmentstore.bookkeeper.write_latency_ms
    segmentstore.bookkeeper.write_queue_size
    segmentstore.bookkeeper.write_queue_fill
  ```

- Tier 1 Storage `DurableDataLog` Read/Write ([Counter](https://micrometer.io/docs/concepts#_counters)) and per-container ledger count Metrics ([Gauge](https://micrometer.io/docs/concepts#_gauges)):

  ```
    segmentstore.bookkeeper.write_bytes
    segmentstore.bookkeeper.bookkeeper_ledger_count - with tag {"container", $containerId}

  ```

- Tier 2 Storage Read/Write latency Metrics ([Histogram](https://micrometer.io/docs/concepts#_histograms_and_percentiles)):

  ```
    segmentstore.storage.read_latency_ms
    segmentstore.storage.write_latency_ms
  ```

- Tier 2 Storage Read/Write data and file creation Metrics ([Counters](https://micrometer.io/docs/concepts#_counters)):

  ```
    segmentstore.storage.read_bytes
    segmentstore.storage.write_bytes
    segmentstore.storage.create_count
  ```

- Segment Store container-specific operation Metrics:

  ```
    // Histograms - all with tags {"container", $containerId}

    segmentstore.container.process_operations.latency_ms
    segmentstore.container.process_operations.batch_size
    segmentstore.container.operation_queue.size
    segmentstore.container.operation_processor.in_flight
    segmentstore.container.operation_queue.wait_time
    segmentstore.container.operation_processor.delay_ms
    segmentstore.container.operation_commit.latency_ms
    segmentstore.container.operation.latency_ms
    segmentstore.container.operation_commit.metadata_txn_count
    segmentstore.container.operation_commit.memory_latency_ms

    // Gauge
    segmentstore.container.operation.log_size
  ```

- Segment Store operation processor ([Counter](https://micrometer.io/docs/concepts#_counters)) Metrics  - all with tags {"container", $containerId}.

  ```
    // Counters/Meters
    segmentstore.container.append_count
    segmentstore.container.append_offset_count
    segmentstore.container.update_attributes_count
    segmentstore.container.get_attributes_count
    segmentstore.container.read_count
    segmentstore.container.get_info_count
    segmentstore.container.create_segment_count
    segmentstore.container.delete_segment_count
    segmentstore.container.merge_segment_count
    segmentstore.container.seal_count
    segmentstore.container.truncate_count
  ```

- Segment Store active Segments ([Gauge](https://micrometer.io/docs/concepts#_gauges)) and thread pool status ([Histogram](https://micrometer.io/docs/concepts#_histograms_and_percentiles)) Metrics:
  ```
    // Gauge - with tags {"container", $containerId}

    segmentstore.active_segments

    // Histograms
    segmentstore.thread_pool.queue_size
    segmentstore.thread_pool.active_threads
  ```

## Metrics in Controller Service


- Controller Stream operation latency Metrics ([Histograms](https://micrometer.io/docs/concepts#_histograms_and_percentiles)):
  ```
    controller.stream.created_latency_ms
    controller.stream.sealed_latency_ms
    controller.stream.deleted_latency_ms
    controller.stream.updated_latency_ms
    controller.stream.truncated_latency_ms
  ```

- Controller global and per-Stream operation Metrics ([Counters](https://micrometer.io/docs/concepts#_counters)):
  ```
    controller.stream.created
    controller.stream.create_failed_global
    controller.stream.create_failed - with tags {"scope", $scope, "stream", $stream}

    controller.stream.sealed
    controller.stream.seal_failed_global
    controller.stream.seal_failed - with tags {"scope", $scope, "stream", $stream}


    controller.stream.deleted
    controller.stream.delete_failed_global
    controller.stream.delete_failed - with tags {"scope", $scope, "stream", $stream}


    controller.stream.updated_global
    controller.stream.updated - with tags {"scope", $scope, "stream", $stream}

    controller.stream.update_failed_global
    controller.stream.update_failed - with tags {"scope", $scope, "stream", $stream}


    controller.stream.truncated_global
    controller.stream.truncated - with tags {"scope", $scope, "stream", $stream}
    controller.stream.truncate_failed_global
    controller.stream.truncate_failed - with tags {"scope", $scope, "stream", $stream}

  ```

- Controller Stream retention frequency ([Counter](https://micrometer.io/docs/concepts#_counters)) and truncated size ([Gauge](https://micrometer.io/docs/concepts#_gauges)) Metrics:
  ```
    controller.retention.frequency - with tags {"scope", $scope, "stream", $stream}

    controller.retention.truncated_size - with tags {"scope", $scope, "stream", $stream}

  ```

- Controller Stream Segment operations ([Counters](https://micrometer.io/docs/concepts#_counters)) and open/timed out Transactions on a Stream ([Gauge](https://micrometer.io/docs/concepts#_gauges)) Metrics  - all with tags {"scope", $scope, "stream", $stream}:
  ```
    controller.transactions.opened
    controller.transactions.timedout
    controller.segments.count
    controller.segment.splits
    controller.segment.merges
  ```

- Controller Transaction operation latency Metrics:
  ```
    controller.transactions.created_latency_ms
    controller.transactions.committed_latency_ms
    controller.transactions.aborted_latency_ms
  ```

- Controller Transaction operation counter Metrics:
    ```
    controller.transactions.created_global
    controller.transactions.created - with tags {"scope", $scope, "stream", $stream}
    controller.transactions.create_failed_global
    controller.transactions.create_failed - with tags {"scope", $scope, "stream", $stream}
    controller.transactions.committed_global
    controller.transactions.committed - with tags {"scope", $scope, "stream", $stream}
    controller.transactions.commit_failed_global
    controller.transactions.commit_failed - with tags {"scope", $scope, "stream", $stream}
    controller.transactions.commit_failed - with tags {"scope", $scope, "stream", $stream,  "transaction", $txnId}
    controller.transactions.aborted_global
    controller.transactions.aborted - with tags {"scope", $scope, "stream", $stream}
    controller.transactions.abort_failed_global
    controller.transactions.abort_failed - with tags {"scope", $scope, "stream", $stream}
    controller.transactions.abort_failed - with tags {"scope", $scope, "stream", $stream,  "transaction", $txnId}
  ```

- Controller hosts available ([Gauge](https://micrometer.io/docs/concepts#_gauges)) and host failure ([Counter](https://micrometer.io/docs/concepts#_counters)) Metrics:
  ```
    controller.hosts.count
    controller.hosts.failures_global
    controller.hosts.failures - with tags {"host", $host}

  ```

- Controller Container count per host ([Gauge](https://micrometer.io/docs/concepts#_gauges)) and failover ([Counter](https://micrometer.io/docs/concepts#_counters)) Metrics:
  ```
    controller.hosts.container_count
    controller.container.failovers_global
    controller.container.failovers - with tags {"container", $containerId}

  ```
- Controller Zookeeper session expiration ([Counter](https://micrometer.io/docs/concepts#_counters)) metrics:
  ```
  controller.zookeeper.session_expiration
  ```

# Resources

* [Micrometer Metrics](https://micrometer.io/docs)
* [Statsd_spec](https://github.com/b/statsd_spec)
