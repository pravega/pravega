<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
In Pravega Metrics Framework, we use [Micrometer Metrics](https://micrometer.io/docs) as the underlying library, and provide our own API to make it easier to use.
# 1. Metrics interfaces and examples usage
There are four basic interfaces: StatsProvider, StatsLogger (short for Statistics Logger), OpStatsLogger (short for Operation Statistics Logger, 
and it is included in StatsLogger) and Dynamic Logger.
StatsProvider provides us the whole Metric service;
StatsLogger is the place at which we register and get required Metrics 
([Counter](https://micrometer.io/docs/concepts#_counters)/
[Gauge](https://micrometer.io/docs/concepts#_gauges)/
[Timer](https://micrometer.io/docs/concepts#_timers)/
[Distribution Summary](https://micrometer.io/docs/concepts#_distribution_summaries)); 
while OpStatsLogger is a sub-metric for complex ones (Timer/Distribution Summary).
## 1.1. Metrics Service Provider — Interface StatsProvider
The starting point of Pravega Metric framework is the StatsProvider interface, it provides start and stop methods for Metric service.
It also provides startWithoutExporting() for testing purpose, which only stores metrics in memory without exporting them
to external systems. 
Currently we have support for StatsD and InfluxDB registries.

[StatsProvider](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/StatsProvider.java)
- start(): Initializes [MeterRegistry](https://micrometer.io/docs/concepts#_registry) for our Metrics service.
- startWithoutExporting(): Initializes SimpleMeterRegistry that holds the latest value of each meter in memory and does not export the data anywhere, typically for unit tests.
- close(): Shutdown of Metrics service.
- createStatsLogger(): Creates and returns a StatsLogger instance, which is used to retrieve a metric and do metric insertion and collection in Pravega code. 
- createDynamicLogger(): Create a dynamic logger.

## 1.2. Metric Logger — interface StatsLogger 
Using this interface we can register required metrics for simple types like 
[Counter](https://micrometer.io/docs/concepts#_counters) and 
[Gauge](https://micrometer.io/docs/concepts#_gauges) 
and some complex statistics type of Metric OpStatsLogger, through which we provide 
[Timer](https://micrometer.io/docs/concepts#_timers) and 
[Distribution Summary](https://micrometer.io/docs/concepts#_distribution_summaries).

[StatsLogger](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/StatsLogger.java)
- createStats(): Register and get a OpStatsLogger, which is used for complex type of metrics. Notice the optional metric tags.
- createCounter(): Register and get a Counter metric.
- createMeter(): Create and register a Meter metric.
- registerGauge(): Register a Gauge metric.
- createScopeLogger(): Create the stats logger under given scope name.

### 1.3. Metric Sub Logger — OpStatsLogger
OpStatsLogger provides complex statistics type of Metric, usually it is used in operations such as CreateSegment, 
ReadSegment, we could use it to record number of operations, time/duration of each operation.

[OpStatsLogger](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/OpStatsLogger.java)
- reportSuccessEvent() : Used to track Timer of a successful operation and will record the latency in Nanoseconds in required metric. 
- reportFailEvent() : Used to track Timer of a failed operation and will record the latency in Nanoseconds in required metric.  
- reportSuccessValue() : Used to track Histogram of a success value.
- reportFailValue() : Used to track Histogram of a failed value. 
- toOpStatsData() :  Used to support JMX exports and inner test.
- clear : Used to clear stats for this operation.

### 1.4 Metric Logger — interface DynamicLogger
A simple interface that only exposes simple type metrics: Counter/Gauge/Meter.

[DynamicLogger](https://github.com/pravega/pravega/blob/master/shared/metrics/src/main/java/io/pravega/shared/metrics/DynamicLogger.java)
- incCounterValue() : Increase Counter with given value. Notice the optional metric tags.
- updateCounterValue() : Updates the counter with given value.
- freezeCounter() : Notifies that the counter will not be updated.
- reportGaugeValue() : Reports Gauge value.
- freezeGaugeValue() : Notifies that the gauge value will not be updated.
- recordMeterEvents()  : Record the occurrence of a given number of events in Meter.


# 2. Example for starting a Metric service
This example is from file io.pravega.segmentstore.server.host.ServiceStarter. It starts Pravega SegmentStore service 
and a Metrics service is started as a sub service.
```java
public final class ServiceStarter {
    ...
    private final ServiceBuilderConfig builderConfig;
    private StatsProvider statsProvider;
    ...
    public void start() throws Exception {
        ...
        log.info("Initializing metrics provider ...");
        MetricsProvider.initialize(builderConfig.getConfig(MetricsConfig::builder));
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start(); // Here metric service is started as a sub-service
        ...
    }
    private void shutdown() {
        ...
         if (this.statsProvider != null) {
            statsProvider.close();
            statsProvider = null;
            log.info("Metrics statsProvider is now closed.");
         }
         ...
    }
...
}
```

## 2.1. Example for Dynamic Counter and OpStatsLogger(Timer)
This is an example from io.pravega.segmentstore.server.host.stat.SegmentStatsRecorderImpl.java. In this class, we 
registered two metrics: One timer (createStreamSegment), one dynamic counter (dynamicLogger).
```java
public class SegmentStatsRecorderImpl implements SegmentStatsRecorder {
    
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger createStreamSegment = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();
    
    public void recordAppend(String streamSegmentName, long dataLength, int numOfEvents, Duration elapsed) {
        getWriteStreamSegment().reportSuccessEvent(elapsed);
        DynamicLogger dl = getDynamicLogger();
        dl.incCounterValue(globalMetricName(SEGMENT_WRITE_BYTES), dataLength);
        dl.incCounterValue(globalMetricName(SEGMENT_WRITE_EVENTS), numOfEvents);
        if (!StreamSegmentNameUtils.isTransactionSegment(streamSegmentName)) {
            //Don't report segment specific metrics if segment is a transaction
            //The parent segment metrics will be updated once the transaction is merged
            dl.incCounterValue(SEGMENT_WRITE_BYTES, dataLength, segmentTags(streamSegmentName));
            dl.incCounterValue(SEGMENT_WRITE_EVENTS, numOfEvents, segmentTags(streamSegmentName));
            ...
        }
    }
    
    @Override
    public void createSegment(String streamSegmentName, byte type, int targetRate, Duration elapsed) {
        getCreateStreamSegment().reportSuccessEvent(elapsed);
        ...
    }

    …
}
```
From the above example, we can see the required steps to register and use dynamic counter:

1. Get a dynamic logger from MetricsProvider: 
```
DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();
```
2. Increase the counter by providing metric base name and optionally tags associated with the metric.
```
DynamicLogger dl = getDynamicLogger();
dl.incCounterValue(globalMetricName(SEGMENT_WRITE_BYTES), dataLength);
...
dl.incCounterValue(SEGMENT_WRITE_BYTES, dataLength, segmentTags(streamSegmentName));
```
Here SEGMENT_WRITE_BYTES is the base name of the metric. There are two metrics shown here: one is the global counter
which has no tags associated, and a segment specific counter which has a list of segment tags associated.
(Here segmentTags is the method to generate tags based on fully qualified segment name) 

Also we see the required steps to register and use OpStatsLogger(Timer):

1. Get a StatsLogger from MetricsProvider: 
```
StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger("segmentstore");
```
2. Register all the desired metrics through StatsLogger:
```
@Getter(AccessLevel.PROTECTED)
final OpStatsLogger createStreamSegment = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);
```
3. Use these metrics within code at appropriate place where you would like to collect and record the values.
```
getCreateStreamSegment().reportSuccessEvent(elapsed);
```
Here SEGMENT_CREATE_LATENCY is the name of this metric, and createStreamSegment is the metric object, which tracks
operations of createSegment. We will get the time of every createSegment operation happened, how long each operation
takes, and other numbers computed based on them.


### 2.2. Example for Dynamic Gauge 
This is an example from io.pravega.controller.metrics.StreamMetrics. In this class, we report 
a dynamic gauge which represents the open transactions of a stream.
```java
public final class StreamMetrics extends AbstractControllerMetrics implements AutoCloseable {
    ...
    static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
       
    ...
    public void createStream(String scope, String streamName, int minNumSegments, Duration latency) {
        DYNAMIC_LOGGER.reportGaugeValue(OPEN_TRANSACTIONS, 0, streamTags(scope, streamName));
    }
    ...
 }
```

### 2.3 Example for Dynamic Meter
This is an example from io.pravega.segmentstore.server.SegmentStoreMetrics. In this class, we report a Dynamic Meter 
which represents the segments created with a particular container.
```java
public final class SegmentStoreMetrics {
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    public final static class Container {
        private final String[] containerTag;

        public Container(int containerId) {
            this.containerTag = containerTag(containerId);
        }

        public void createSegment() {
            DYNAMIC_LOGGER.recordMeterEvents(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, 1, containerTag);
        }
    }
}
```
 
# 3. Metric Registries and Configurations
With Micrometer, each meter registry is responsible for both storage and exporting of metrics objects. 
In order to have a unified interface Micrometer provides CompositeMeterRegistry for application to interact with,
CompositeMeterRegistry will forward metric operations to all the concrete registries bound to it.
Note that CompositeMeterRegistry has no storage associated, hence in case no registry bound to it, it will become an
NO-OP interface only - Pravega will throw error in such a case.

To Easy unit testing, Micrometer also provides SimpleMeterRegistry, which has memory only storage but no exporting; 
call startWithoutExporting() of StatsProvider to use this feature in test codes. 

Currently Pravega supports StatsD registry in Telegraf flavor; dimensional metrics data model (or metric tags) 
is supported. Communication protocol is UDP.
Direct InfluxDB connection is also supported.

The reporter could be configured through MetricsConfig.
```java
public class MetricsConfig extends ComponentConfig {
    public final static Property<Boolean> ENABLE_STATISTICS = Property.named("enableStatistics", true);
    public final static Property<Long> DYNAMIC_CACHE_SIZE = Property.named("dynamicCacheSize", 10000000L);
    public final static Property<Integer> DYNAMIC_CACHE_EVICTION_DURATION_MINUTES = Property.named("dynamicCacheEvictionDurationMinutes", 30);
    public final static Property<Integer> OUTPUT_FREQUENCY = Property.named("outputFrequencySeconds", 60);
    public final static Property<String> METRICS_PREFIX = Property.named("metricsPrefix", "pravega");
    public final static Property<String> STATSD_HOST = Property.named("statsDHost", "localhost");
    public final static Property<Integer> STATSD_PORT = Property.named("statsDPort", 8125);
    public final static Property<String> INFLUXDB_URI = Property.named("influxDBURI", "http://localhost:8086");
    public final static Property<String> INFLUXDB_NAME = Property.named("influxDBName", "pravega");
    public final static Property<String> INFLUXDB_USERNAME = Property.named("influxDBUserName", "");
    public final static Property<String> INFLUXDB_PASSWORD = Property.named("influxDBPassword", "");
    public final static Property<String> INFLUXDB_RETENTION_POLICY = Property.named("influxDBRetention", "");
    public final static Property<Boolean> ENABLE_STATSD_REPORTER = Property.named("enableStatsDReporter", true);
    public final static Property<Boolean> ENABLE_INFLUXDB_REPORTER = Property.named("enableInfluxDBReporter", false);
    public static final String COMPONENT_CODE = "metrics";
    ...
}
```

# 4. Steps to add your own Metrics

```java
// Step 1. When start a segment store/controller service, start a Metrics service as a sub service. Reference above example in ServiceStarter.start()
public class AddMetrics {
        MetricsProvider.initialize(Config.METRICS_CONFIG);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();
    // Step 2. In the class that need Metrics: get StatsLogger through MetricsProvider; then get Metrics from StatsLogger; at last report it at the right place.

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

# 5. Available Metrics and their names

## Metrics in Segment Store Service

- Segment Store read/write latency of storage operations (histograms):
```
segmentstore.segment.create_latency_ms
segmentstore.segment.read_latency_ms
segmentstore.segment.write_latency_ms 
```

- Segment Store global and per-segment read/write metrics (counters):
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

- Segment Store cache read/write latency metrics (histogram):
```
segmentstore.cache.insert_latency_ms
segmentstore.cache.get_latency
```

- Segment Store cache read/write metrics (counters):
```
segmentstore.cache.write_bytes
segmentstore.cache.read_bytes
```

- Segment Store cache size (gauge) and generation spread (histogram) metrics:
```
segmentstore.cache.size_bytes
segmentstore.cache.gen
```

- Tier-1 DurableDataLog read/write latency and queueing metrics (histogram):	
```
segmentstore.bookkeeper.total_write_latency_ms
segmentstore.bookkeeper.write_latency_ms
segmentstore.bookkeeper.write_queue_size
segmentstore.bookkeeper.write_queue_fill
```

- Tier-1 DurableDataLog read/write (counter) and per-container ledger count metrics (gauge):	
```
segmentstore.bookkeeper.write_bytes
segmentstore.bookkeeper.bookkeeper_ledger_count - with tag {"container", $containerId}
```

- Tier-2 Storage read/write latency metrics (histogram):	
```
segmentstore.storage.read_latency_ms
segmentstore.storage.write_latency_ms
```

- Tier-2 Storage read/write data and file creation metrics (counters):
```
segmentstore.storage.read_bytes
segmentstore.storage.write_bytes
segmentstore.storage.create_count
```

- Segment Store container-specific operation metrics:
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

- Segment Store operation processor (counter) metrics - all with tags {"container", $containerId}
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

- Segment Store active Segments (gauge) and thread pool status (histogram) metrics:
```
// Gauge - with tags {"container", $containerId}
segmentstore.active_segments

// Histograms
segmentstore.thread_pool.queue_size
segmentstore.thread_pool.active_threads
```

## Metrics in Controller Service

- Controller Stream operation latency metrics (histograms):
```
controller.stream.created_latency_ms
controller.stream.sealed_latency_ms
controller.stream.deleted_latency_ms
controller.stream.updated_latency_ms
controller.stream.truncated_latency_ms
```

- Controller global and per-Stream operation metrics (counters):
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

- Controller Stream retention frequency (counter) and truncated size (gauge) metrics:
```
controller.retention.frequency - with tags {"scope", $scope, "stream", $stream}
controller.retention.truncated_size - with tags {"scope", $scope, "stream", $stream}
``` 

- Controller Stream Segment operations (counters) and open/timed out Transactions on a Stream (gauge/counter) metrics - all with tags {"scope", $scope, "stream", $stream}:
```
controller.segments.count - with tags {"scope", $scope, "stream", $stream}
controller.segments.initial_count - with tags {"scope", $scope, "stream", $stream}
controller.segment.splits - with tags {"scope", $scope, "stream", $stream}
controller.segment.merges - with tags {"scope", $scope, "stream", $stream}
```

- Controller Transaction operation latency metrics:
```
controller.transactions.created_latency_ms
controller.transactions.committed_latency_ms
controller.transactions.aborted_latency_ms
```

- Controller Transaction operation counter metrics:
```
controller.transactions.created_global
controller.transactions.created - with tags {"scope", $scope, "stream", $stream}
controller.transactions.create_failed_global
controller.transactions.create_failed - with tags {"scope", $scope, "stream", $stream}
controller.transactions.committed_global
controller.transactions.committed - with tags {"scope", $scope, "stream", $stream}
controller.transactions.commit_failed_global
controller.transactions.commit_failed - with tags {"scope", $scope, "stream", $stream}
controller.transactions.commit_failed - with tags {"scope", $scope, "stream", $stream, "transaction", $txnId}
controller.transactions.aborted_global
controller.transactions.aborted - with tags {"scope", $scope, "stream", $stream}
controller.transactions.abort_failed_global
controller.transactions.abort_failed - with tags {"scope", $scope, "stream", $stream}
controller.transactions.abort_failed - with tags {"scope", $scope, "stream", $stream, "transaction", $txnId}
controller.transactions.opened - with tags {"scope", $scope, "stream", $stream}
controller.transactions.timedout - with tags {"scope", $scope, "stream", $stream}
```

- Controller hosts available (gauge) and host failure (counter) metrics:
```
controller.hosts.count
controller.hosts.failures_global
controller.hosts.failures.$host  - with tags {"host", $host}
```

- Controller Container count per host (gauge) and failover (counter) metrics:
```
controller.hosts.container_count
controller.container.failovers_global
controller.container.failovers.$containerId - with tags {"container", $containerId}
```

- Controller Zookeeper session expiration (counter) metrics:
```
controller.zookeeper.session_expiration
```

# 6. Useful links
* [Micrometer Metrics](https://micrometer.io/docs)
* [Statsd_spec](https://github.com/b/statsd_spec)
