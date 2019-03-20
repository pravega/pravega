<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Metrics

* [Metrics Interfaces and Examples Usage](#metrics-interfaces-and-examples-usage)
   - [Metrics Service Provider — Interface StatsProvider](#metrics-service-provider--interface-statsprovider)
   - [Metric Logger — Interface StatsLogger](#metric-logger--interface-statslogger)
   - [Metric Sub Logger — OpStatsLogger](#metric-sub-logger--opstatslogger)
   - [Metric Logger — Interface DynamicLogger](#metric-logger--interface-dynamiclogger)
* [Example for Starting a Metric Service](#example-for-starting-a-metric-service)
   - [Example for Dynamic Counter and OpStatsLogger(Timer)](#example-for-dynamic-counter-and-opstatsloggertimer)
       - [Output Example of OpStatsLogger](#output-example-of-opstatslogger)
   - [Example for Dynamic Gauge and OpStatsLogger(Histogram)](#example-for-dynamic-gauge-and-opstatsloggerhistogram)
   - [Example for Dynamic Meter](#example-for-dynamic-meter)
* [Metric Reporter and Configurations](#metric-reporter-and-configurations)
* [Configuring Own Metrics](#configuring-own-metrics)
* [Available Metrics and Their Names](#available-metrics-and-their-names)
* [Resources](#resources)

In Pravega Metrics Framework, we use [Dropwizard Metrics](https://metrics.dropwizard.io/3.1.0/apidocs) as the underlying library, and provide our own API to make it easier to use.

# Metrics Interfaces and Examples Usage

Following are four basic interfaces:

1. `StatsProvider`
2. `StatsLogger`
3. `OpStatsLogger`
4. `DynamicLogger`

- `StatsProvider`: The Statistics Provider which provides us the whole Metric service.

- `StatsLogger`: The Statistics Logger is the place at where we register and get the required Metrics ([Counter](https://metrics.dropwizard.io/3.1.0/manual/core/#counters)/[Gauge](https://metrics.dropwizard.io/3.1.0/manual/core/#gauges)/[Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers)/[Histograms](https://metrics.dropwizard.io/3.1.0/manual/core/#histograms)).

- `OpStatsLogger`: The Operation Statistics Logger, is a sub-metric for the complex ones ([Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers)/[Histograms](https://metrics.dropwizard.io/3.1.0/manual/core/#histograms)). It is included in the `StatsLogger` and `DynamicLogger`.



## Metrics Service Provider — Interface StatsProvider

Pravega Metric Framework is initiated using the `StatsProvider` interface, it provides _start_ and _stop_ method for Metric service. Regarding the **Reporters**, currently we have support for [**CSV reporter**](https://metrics.dropwizard.io/3.1.0/manual/core/#csv) and [**StatsD reporter**](https://github.com/b/statsd_spec).

```java
public interface StatsProvider {
    void start();
    void close();
    StatsLogger createStatsLogger(String scope);
    DynamicLogger createDynamicLogger();
}
```

- `start()`: Initializes [MetricRegistry](http://metrics.dropwizard.io/3.1.0/manual/core/#metric-registries) and Reporters for our Metric service.
- `close()`: Performs Shutting down of Metric Service.
- `createStatsLogger()`: Creates and returns a `StatsLogger` instance, which is used to retrieve a metric and performs metric insertion and collection in Pravega code.
- `createDynamicLogger()`: Creates a Dynamic Logger.

## Metric Logger — Interface StatsLogger

Using the following interface we can register required metrics for simple types like [Counter](http://metrics.dropwizard.io/3.1.0/manual/core/#counters) and [Gauge](http://metrics.dropwizard.io/3.1.0/manual/core/#gauges) and some complex statistics type of Metric like `OpStatsLogger`, through which we provide [Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers) and [Histogram](http://metrics.dropwizard.io/3.1.0/manual/core/#histograms).

```java
public interface StatsLogger {
    OpStatsLogger createStats(String name);
    Counter createCounter(String name);
    Meter createMeter(String name);
    <T extends Number> Gauge registerGauge(String name, Supplier<T> value);
    StatsLogger createScopeLogger(String scope);
}
```

- `createStats()`: Register and get a `OpStatsLogger`, which is used for complex type of metrics.
- `createCounter()`: Register and get a [Counter](http://metrics.dropwizard.io/3.1.0/manual/core/#counters) Metric.
- `createMeter()`: Create and register a [Meter](https://metrics.dropwizard.io/3.1.0/manual/core/#meter) Metric.
- `registerGauge()`: Register a [Gauge](http://metrics.dropwizard.io/3.1.0/manual/core/#gauges) Metric.
- `createScopeLogger()`: Create the `StatsLogger` under the given scope name.

## Metric Sub Logger — OpStatsLogger

`OpStatsLogger` provides complex statistics type of Metric, usually it is used in operations such as `CreateSegment`, `ReadSegment`, we could use it to record the _number of operation_ and _time/duration_ of each operation.

```java
public interface OpStatsLogger {
    void reportSuccessEvent(Duration duration);
    void reportFailEvent(Duration duration);
    void reportSuccessValue(long value);
    void reportFailValue(long value);
    OpStatsData toOpStatsData();
    void clear();
}
```

- `reportSuccessEvent()`: It is used to track the [Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers) of a successful operation and will record the latency in nanoseconds in the required metric.
- `reportFailEvent()`: It is used to track the [Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers) of a failed operation and will record the latency in nanoseconds in required metric.  
- `reportSuccessValue()`: It is used to track the [Histogram](http://metrics.dropwizard.io/3.1.0/manual/core/#histograms) of a success value.
- `reportFailValue()`: It is used to track the [Histogram](http://metrics.dropwizard.io/3.1.0/manual/core/#histograms) of a failed value.
- `toOpStatsData()`:  It is used to support the [JMX](https://metrics.dropwizard.io/3.1.0/manual/core/#jmx) exports and inner tests.
- `clear`: It is used to clear the stats for this operation.

## Metric Logger — Interface DynamicLogger

The following is an example of a  simple interface that exposes only simple type metrics: ([Counter](https://metrics.dropwizard.io/3.1.0/manual/core/#counters)/[Gauge](https://metrics.dropwizard.io/3.1.0/manual/core/#gauges)/[Meter](https://metrics.dropwizard.io/3.1.0/manual/core/#meter)).

```java
public interface DynamicLogger {
    void incCounterValue(String name, long delta);
    void updateCounterValue(String name, long value);
    void freezeCounter(String name);
    <T extends Number> void reportGaugeValue(String name, T value);
    void freezeGaugeValue(String name);
    void recordMeterEvents(String name, long number);
}
```

- `incCounterValue()`: Increases the [Counter](https://metrics.dropwizard.io/3.1.0/manual/core/#counters) with the given value.
- `updateCounterValue()`: Updates the [Counter](https://metrics.dropwizard.io/3.1.0/manual/core/#counters) with the given value.
- `freezeCounter()`: Notifies that, the [Counter](https://metrics.dropwizard.io/3.1.0/manual/core/#counters) will not be updated.
- `reportGaugeValue()`: Reports the [Gauge](https://metrics.dropwizard.io/3.1.0/manual/core/#gauges) value.
- `freezeGaugeValue()`: Notifies that, the [Gauge](https://metrics.dropwizard.io/3.1.0/manual/core/#gauges) value will not be updated.
- `recordMeterEvents()`: Records the occurrences of a given number of events in [Meter](https://metrics.dropwizard.io/3.1.0/manual/core/#meters).


# Example for Starting a Metric Service

This example is from file `io.pravega.segmentstore.server.host.ServiceStarter`. It starts Pravega Segment Store service and the Metrics Service is started as a sub service.

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

## Example for Dynamic Counter and OpStatsLogger(Timer)

This is an example from `io.pravega.segmentstore.server.host.handler.PravegaRequestProcessor`. In the class `PravegaRequestProcessor`, we have registered two metrics:

- one Timer (`createStreamSegment`)
- one dynamic counter (`segmentReadBytes`)

```java
public class PravegaRequestProcessor extends FailingRequestProcessor implements RequestProcessor {

    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    private DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();

    private final OpStatsLogger createStreamSegment = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);

    private void handleReadResult(ReadSegment request, ReadResult result) {
            String segment = request.getSegment();
            ArrayList<ReadResultEntryContents> cachedEntries = new ArrayList<>();
            ReadResultEntry nonCachedEntry = collectCachedEntries(request.getOffset(), result, cachedEntries);

            boolean truncated = nonCachedEntry != null && nonCachedEntry.getType() == Truncated;
            boolean endOfSegment = nonCachedEntry != null && nonCachedEntry.getType() == EndOfStreamSegment;
            boolean atTail = nonCachedEntry != null && nonCachedEntry.getType() == Future;

            if (!cachedEntries.isEmpty() || endOfSegment) {
                // We managed to collect some data. Send it.
                ByteBuffer data = copyData(cachedEntries);
                SegmentRead reply = new SegmentRead(segment, request.getOffset(), atTail, endOfSegment, data);
                connection.send(reply);
                dynamicLogger.incCounterValue(nameFromSegment(SEGMENT_READ_BYTES, segment), reply.getData().array().length); // Increasing the counter value for the counter metric SEGMENT_READ_BYTES
            } else if (truncated) {
                // We didn't collect any data, instead we determined that the current read offset was truncated.
                // Determine the current Start Offset and send that back.
                segmentStore.getStreamSegmentInfo(segment, false, TIMEOUT)
                        .thenAccept(info ->
                                connection.send(new SegmentIsTruncated(nonCachedEntry.getStreamSegmentOffset(), segment, info.getStartOffset())))
                        .exceptionally(e -> handleException(nonCachedEntry.getStreamSegmentOffset(), segment, "Read segment", e));
            } else {
                Preconditions.checkState(nonCachedEntry != null, "No ReadResultEntries returned from read!?");
                nonCachedEntry.requestContent(TIMEOUT);
                nonCachedEntry.getContent()
                        .thenAccept(contents -> {
                            ByteBuffer data = copyData(Collections.singletonList(contents));
                            SegmentRead reply = new SegmentRead(segment, nonCachedEntry.getStreamSegmentOffset(), false, endOfSegment, data);
                            connection.send(reply);
                            dynamicLogger.incCounterValue(nameFromSegment(SEGMENT_READ_BYTES, segment), reply.getData().array().length); // Increasing the counter value for the counter metric SEGMENT_READ_BYTES
                        })
                        .exceptionally(e -> {
                            if (Exceptions.unwrap(e) instanceof StreamSegmentTruncatedException) {
                                // The Segment may have been truncated in Storage after we got this entry but before we managed
                                // to make a read. In that case, send the appropriate error back.
                                connection.send(new SegmentIsTruncated(nonCachedEntry.getStreamSegmentOffset(), segment, nonCachedEntry.getStreamSegmentOffset()));
                            } else {
                                handleException(nonCachedEntry.getStreamSegmentOffset(), segment, "Read segment", e);
                            }
                            return null;
                        })
                        .exceptionally(e -> handleException(nonCachedEntry.getStreamSegmentOffset(), segment, "Read segment", e));
            }
        }

    @Override
        public void createSegment(CreateSegment createStreamsSegment) {
            Timer timer = new Timer();

            Collection<AttributeUpdate> attributes = Arrays.asList(
                    new AttributeUpdate(SCALE_POLICY_TYPE, AttributeUpdateType.Replace, ((Byte) createStreamsSegment.getScaleType()).longValue()),
                    new AttributeUpdate(SCALE_POLICY_RATE, AttributeUpdateType.Replace, ((Integer) createStreamsSegment.getTargetRate()).longValue()),
                    new AttributeUpdate(CREATION_TIME, AttributeUpdateType.None, System.currentTimeMillis())
            );

           if (!verifyToken(createStreamsSegment.getSegment(), createStreamsSegment.getRequestId(),
                   createStreamsSegment.getDelegationToken(), READ_UPDATE, "Create Segment")) {
                return;
           }
           log.debug("Creating stream segment {}", createStreamsSegment);
            segmentStore.createStreamSegment(createStreamsSegment.getSegment(), attributes, TIMEOUT)
                    .thenAccept(v -> {
                        createStreamSegment.reportSuccessEvent(timer.getElapsed()); // Reporting success event for Timer metric createStreamSegment
                        connection.send(new SegmentCreated(createStreamsSegment.getRequestId(), createStreamsSegment.getSegment()));
                    })
                    .whenComplete((res, e) -> {
                        if (e == null) {
                            if (statsRecorder != null) {
                                statsRecorder.createSegment(createStreamsSegment.getSegment(),
                                        createStreamsSegment.getScaleType(), createStreamsSegment.getTargetRate());
                            }
                        } else {
                            createStreamSegment.reportFailEvent(timer.getElapsed()); // Reporting fail event for Timer metric createStreamSegment
                            handleException(createStreamsSegment.getRequestId(), createStreamsSegment.getSegment(), "Create segment", e);
                        }
                    });
        }

    …
}
```
Using the above example, following are the required steps to register and use a metric in the desired class and method:

1. Get a `StatsLogger` from `MetricsProvider`.

```
StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger();
```
2. Register all the desired metrics through `StatsLogger`.
```
static final OpStatsLogger CREATE_STREAM_SEGMENT = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);
```
3. Use these metrics within code at the appropriate places where you would like to collect and record the values.

```
Metrics.CREATE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
```
The `CREATE_STREAM_SEGMENT` is the name of this metric, and `CREATE_STREAM_SEGMENT` is the name of our Metrics logger. It will track operations of `createSegment`, and we will get the time (i.e. time taken by each operation and other numbers computed based on them) for each `createSegment` operation happened.

### Output Example of OpStatsLogger

An example output of `OpStatsLogger CREATE_SEGMENT` reported through [CSV](https://metrics.dropwizard.io/3.1.0/manual/core/#csv) reporter.

```
$ cat CREATE_STREAM_SEGMENT.csv
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1480928806,1,8.973952,8.973952,8.973952,0.000000,8.973952,8.973952,8.973952,8.973952,8.973952,8.973952,0.036761,0.143306,0.187101,0.195605,calls/second,millisecond
```


## Example for Dynamic Gauge and OpStatsLogger(Histogram)

This is an example from `io.pravega.controller.store.stream.AbstractStreamMetadataStore`. In the class, `AbstractStreamMetadataStore` we report a Dynamic Gauge which represents:

- Open Transactions
- One Histogram (`CREATE_STREAM`).

```java
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore  {
    ...
    private static final OpStatsLogger CREATE_STREAM = STATS_LOGGER.createStats(MetricsNames.CREATE_STREAM); // get stats logger from MetricsProvider
    private DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger(); // get dynamic logger from MetricsProvider

    ...
    @Override
        public CompletableFuture<CreateStreamResponse> createStream(final String scope,
                                                       final String name,
                                                       final StreamConfiguration configuration,
                                                       final long createTimestamp,
                                                       final OperationContext context,
                                                       final Executor executor) {
            return withCompletion(getStream(scope, name, context).create(configuration, createTimestamp), executor)
                    .thenApply(result -> {
                        if (result.getStatus().equals(CreateStreamResponse.CreateStatus.NEW)) {
                            CREATE_STREAM.reportSuccessValue(1); // Report success event for histogram metric CREATE_STREAM
                            dynamicLogger.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, name), 0); // Report gauge value for Dynamic Gauge metric OPEN_TRANSACTIONS
                        }

                        return result;
                    });
        }
    ...
 }
```

## Example for Dynamic Meter

This is an example from `io.pravega.segmentstore.server.SegmentStoreMetrics`. In the class `SegmentStoreMetrics`, we report a Dynamic Meter which represents the Segments created.

```java
public final class SegmentStoreMetrics {
    private final DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();

    public void createSegment() {
            dynamicLogger.recordMeterEvents(this.createSegmentCount, 1);  // Record event for meter metric createSegmentCount
    }
}
```

# Metric Reporter and Configurations

Reporters are used to export all the measurements being made by the metrics. We currently provide [StatsD](https://github.com/b/statsd_spec) and [CSV](https://metrics.dropwizard.io/3.1.0/manual/core/#csv) output. It is not difficult to add new output formats, such as **[JMX](https://metrics.dropwizard.io/3.1.0/manual/core/#jmx)/[SLF4J](https://metrics.dropwizard.io/3.1.0/manual/core/#slf4j)**.

- [**CSV**](https://metrics.dropwizard.io/3.1.0/manual/core/#csv) reporter will export each Metric into one file.
- [**StatsD**](https://github.com/b/statsd_spec) reporter will export Metrics through UDP/TCP to a StatsD server.
The reporter could be configured using the `MetricsConfig`.

```java
public class MetricsConfig extends ComponentConfig {
    //region Members
    public final static String COMPONENT_CODE = "metrics";
    public final static String ENABLE_STATISTICS = "enableStatistics"; //enable metric, or will report nothing, default = true,  
    public final static Property<Long> DYNAMIC_CACHE_SIZE = "dynamicCacheSize"; //dynamic cache size , default = 10000000L
    public final static Property<Integer> DYNAMIC_CACHE_EVICTION_DURATION_MINUTES = "dynamicCacheEvictionDurationMinutes"; //dynamic cache evcition duration, default = 30
    public final static String OUTPUT_FREQUENCY = "statsOutputFrequencySeconds"; //reporter output frequency, default = 60
    public final static String METRICS_PREFIX = "metricsPrefix"; //Metrics Prefix, default = "pravega"
    public final static String CSV_ENDPOINT = "csvEndpoint"; // CSV reporter output dir, default = "/tmp/csv"
    public final static String STATSD_HOST = "statsDHost"; // StatsD server host for the reporting, default = "localhost"
    public final static String STATSD_PORT = "statsDPort"; // StatsD server port, default = "8125"
    public final static Property<String> GRAPHITE_HOST = "graphiteHost"; // Graphite server host for the reporting, default = "localhost"
    public final static Property<Integer> GRAPHITE_PORT = "graphitePort"; // Graphite server port, default = "2003"
    public final static Property<String> JMX_DOMAIN = "jmxDomain"; // JMX domain for the reporting, default = "domain"
    public final static Property<String> GANGLIA_HOST = "gangliaHost"; // Ganglia server host for the reporting, default = "localhost"
    public final static Property<Integer> GANGLIA_PORT = "gangliaPort"; // Ganglia server port, default = "8649"
    public final static Property<Boolean> ENABLE_CSV_REPORTER = "enableCSVReporter"; // Enables CSV reporter, default = true
    public final static Property<Boolean> ENABLE_STATSD_REPORTER = "enableStatsdReporter"; // Enables StatsD reporter, default = true
    public final static Property<Boolean> ENABLE_GRAPHITE_REPORTER = "enableGraphiteReporter"; // Enables Graphite reporter, default = false
    public final static Property<Boolean> ENABLE_JMX_REPORTER = "enableJMXReporter"; // Enables JMX reporter, default = false
    public final static Property<Boolean> ENABLE_GANGLIA_REPORTER ="enableGangliaReporter"; // Enables Ganglia reporter, default = false
    public final static Property<Boolean> ENABLE_CONSOLE_REPORTER = "enableConsoleReporter"; // Enables Console reporter, default = false
    ...
}
```

# Configuring Own Metrics

1. On the start of a segment store/controller service, start a Metrics service as a sub service. Please check [`ServiceStarter.start()`](#example-for-starting-a-metric-service)

```java
public class AddMetrics {
        statsProvider = MetricsProvider.getProvider();
        statsProvider.start(metricsConfig);    
```

2. In the class that need Metrics: get `StatsLogger` through `MetricsProvider`; then get `Metrics` from `StatsLogger`; in the end report it at the right place.

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

# Available Metrics and Their Names

## Metrics in Segment Store Service

- Segment Store Read/Write latency of storage operations (Histograms):

```
segmentstore.segment.create_latency_ms
segmentstore.segment.read_latency_ms
segmentstore.segment.write_latency_ms
```

- Segment Store global and per-segment Read/Write Metrics (Counters):

```
// Global counters
segmentstore.segment.read_bytes_global.Counter
segmentstore.segment.write_bytes_global.Counter
segmentstore.segment.write_events_global.Counter

// Per segment counters
segmentstore.segment.write_bytes.$scope.$stream.$segment.#epoch.$epoch.Counter
segmentstore.segment.read_bytes.$scope.$stream.$segment.#epoch.$epoch.Counter
segmentstore.segment.write_events.$scope.$stream.$segment.#epoch.$epoch.Counter
```

- Segment Store cache Read/Write latency Metrics (Histogram):

```
segmentstore.cache.insert_latency_ms
segmentstore.cache.get_latency
```

- Segment Store cache Read/Write Metrics (Counters):

```
segmentstore.cache.write_bytes.Counter
segmentstore.cache.read_bytes.Counter
```

- Segment Store cache size (Gauge) and generation spread (Histogram) Metrics:

```
segmentstore.cache.size_bytes.Gauge
segmentstore.cache.gen
```

- Tier 1 Storage `DurableDataLog` Read/Write latency and queueing Metrics (Histogram):

```
segmentstore.bookkeeper.total_write_latency_ms
segmentstore.bookkeeper.write_latency_ms
segmentstore.bookkeeper.write_queue_size
segmentstore.bookkeeper.write_queue_fill
```

- Tier 1 Storage `DurableDataLog` Read/Write (Counter) and per-container ledger count Metrics (Gauge):

```
segmentstore.bookkeeper.write_bytes.Counter
segmentstore.bookkeeper.bookkeeper_ledger_count.$containerId.Gauge
```

- Tier 2 Storage Read/Write latency Metrics (Histogram):

```
segmentstore.storage.read_latency_ms
segmentstore.storage.write_latency_ms
```

- Tier 2 Storage Read/Write data and file creation Metrics (Counters):

```
segmentstore.storage.read_bytes.Counter
segmentstore.storage.write_bytes.Counter
segmentstore.storage.create_count.Counter
```

- Segment Store container-specific operation Metrics:

```
// Histograms
segmentstore.container.process_operations.latency_ms.$containerId
segmentstore.container.process_operations.batch_size.$containerId
segmentstore.container.operation_queue.size.$containerId
segmentstore.container.operation_processor.in_flight.$containerId
segmentstore.container.operation_queue.wait_time.$containerId
segmentstore.container.operation_processor.delay_ms.$containerId
segmentstore.container.operation_commit.latency_ms.$containerId
segmentstore.container.operation.latency_ms.$containerId
segmentstore.container.operation_commit.metadata_txn_count.$containerId
segmentstore.container.operation_commit.memory_latency_ms.$containerId

// Gauge
segmentstore.container.operation.log_size.$containerId.Gauge
```

- Segment Store operation processor (Counter) Metrics:

```
// Counters/Meters
segmentstore.container.append_count.$containerId.Meter
segmentstore.container.append_offset_count.$containerId.Meter
segmentstore.container.update_attributes_count.$containerId.Meter
segmentstore.container.get_attributes_count.$containerId.Meter
segmentstore.container.read_count.$containerId.Meter
segmentstore.container.get_info_count.$containerId.Meter
segmentstore.container.create_segment_count.$containerId.Meter
segmentstore.container.delete_segment_count.$containerId.Meter
segmentstore.container.merge_segment_count.$containerId.Meter
segmentstore.container.seal_count.$containerId.Meter
segmentstore.container.truncate_count.$containerId.Meter

```

- Segment Store active Segments (Gauge) and thread pool status (Histogram) Metrics:
```
// Gauge
segmentstore.active_segments.$containerId.Gauge

// Histograms
segmentstore.thread_pool.queue_size
segmentstore.thread_pool.active_threads
```

## Metrics in Controller Service

- Controller Stream operation latency Metrics (Histograms):
```
controller.stream.created_latency_ms
controller.stream.sealed_latency_ms
controller.stream.deleted_latency_ms
controller.stream.updated_latency_ms
controller.stream.truncated_latency_ms
```

- Controller global and per-Stream operation Metrics (Counters):
```
controller.stream.created.Counter
controller.stream.create_failed_global.Counter
controller.stream.create_failed.$scope.$stream.Counter
controller.stream.sealed.Counter
controller.stream.seal_failed_global.Counter
controller.stream.seal_failed.$scope.$stream.Counter
controller.stream.deleted.Counter
controller.stream.delete_failed_global.Counter
controller.stream.delete_failed.$scope.$stream.Counter
controller.stream.updated_global.Counter
controller.stream.updated.$scope.$stream.Counter
controller.stream.update_failed_global.Counter
controller.stream.update_failed.$scope.$stream.Counter
controller.stream.truncated_global.Counter
controller.stream.truncated.$scope.$stream.Counter
controller.stream.truncate_failed_global.Counter
controller.stream.truncate_failed.$scope.$stream.Counter
```

- Controller Stream retention frequency (Counter) and truncated size (Gauge) Metrics:
```
controller.retention.frequency.$scope.$stream.Counter
controller.retention.truncated_size.$scope.$stream.Gauge
```

- Controller Stream Segment operations (Counters) and open/timed out Transactions on a Stream (Gauge/Counter) Metrics:
```
controller.transactions.opened.$scope.$stream.Gauge
controller.transactions.timedout.$scope.$stream.Counter
controller.segments.count.$scope.$stream.Counter
controller.segment.splits.$scope.$stream.Counter
controller.segment.merges.$scope.$stream.Counter
```

- Controller Transaction operation latency Metrics:
```
controller.transactions.created_latency_ms
controller.transactions.committed_latency_ms
controller.transactions.aborted_latency_ms
```

- Controller Transaction operation counter Metrics:
```
controller.transactions.created_global.Counter
controller.transactions.created.$scope.$stream.Counter
controller.transactions.create_failed_global.Counter
controller.transactions.create_failed.$scope.$stream.Counter
controller.transactions.committed_global.Counter
controller.transactions.committed.$scope.$stream.Counter
controller.transactions.commit_failed_global.Counter
controller.transactions.commit_failed.$scope.$stream.Counter
controller.transactions.commit_failed.$scope.$stream.$txnId.Counter
controller.transactions.aborted_global.Counter
controller.transactions.aborted.$scope.$stream.Counter
controller.transactions.abort_failed_global.Counter
controller.transactions.abort_failed.$scope.$stream.Counter
controller.transactions.abort_failed.$scope.$stream.$txnId.Counter
```

- Controller hosts available (Gauge) and host failure (Counter) Metrics:
```
controller.hosts.count.Gauge
controller.hosts.failures_global.Counter
controller.hosts.failures.$host.Counter
```

- Controller Container count per host (Gauge) and failover (Counter) Metrics:
```
controller.hosts.container_count.Gauge
controller.container.failovers_global.Counter
controller.container.failovers.$containerId.Counter
```

# Resources
* [Dropwizard Metrics](https://metrics.dropwizard.io/3.1.0/apidocs)
* [Statsd_spec](https://github.com/b/statsd_spec)
* [etsy_StatsD](https://github.com/etsy/statsd)
