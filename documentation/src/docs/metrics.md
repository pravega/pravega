<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
In Pravega Metrics Framework, we use [Dropwizard Metrics](https://metrics.dropwizard.io/3.1.0/apidocs) as the underlying library, and provide our own API to make it easier to use.
# 1. Metrics interfaces and examples usage
There are four basic interfaces: StatsProvider, StatsLogger (short for Statistics Logger), OpStatsLogger (short for Operation Statistics Logger, and it is included in StatsLogger) and Dynamic Logger.
StatsProvider provides us the whole Metric service; StatsLogger is the place at which we register and get required Metrics ([Counter](https://metrics.dropwizard.io/3.1.0/manual/core/#counters)/[Gauge](https://metrics.dropwizard.io/3.1.0/manual/core/#gauges)/[Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers)/[Histograms](https://metrics.dropwizard.io/3.1.0/manual/core/#histograms)); while OpStatsLogger is a sub-metric for complex ones (Timer/Histograms).
## 1.1. Metrics Service Provider — Interface StatsProvider
The starting point of Pravega Metric framework is the StatsProvider interface, it provides start and stop method for Metric service. Regarding the reporters, currently we have support for CSV reporter and StatsD reporter.
```java
public interface StatsProvider {
    void start();
    void close();
    StatsLogger createStatsLogger(String scope);
    DynamicLogger createDynamicLogger();
}
```

- start(): Initializes [MetricRegistry](http://metrics.dropwizard.io/3.1.0/manual/core/#metric-registries) and reporters for our Metrics service. 
- close(): Shutdown of Metrics service.
- createStatsLogger(): Creates and returns a StatsLogger instance, which is used to retrieve a metric and do metric insertion and collection in Pravega code. 
- createDynamicLogger(): Create a dynamic logger.

## 1.2. Metric Logger — interface StatsLogger
Using this interface we can register required metrics for simple types like [Counter](http://metrics.dropwizard.io/3.1.0/manual/core/#counters) and [Gauge](http://metrics.dropwizard.io/3.1.0/manual/core/#gauges) and some complex statistics type of Metric OpStatsLogger, through which we provide [Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers) and [Histogram](http://metrics.dropwizard.io/3.1.0/manual/core/#histograms).
```java
public interface StatsLogger {
    OpStatsLogger createStats(String name);
    Counter createCounter(String name);
    Meter createMeter(String name);
    <T extends Number> Gauge registerGauge(String name, Supplier<T> value);
    StatsLogger createScopeLogger(String scope);
}
```

- createStats(): Register and get a OpStatsLogger, which is used for complex type of metrics.
- createCounter(): Register and get a Counter metric.
- createMeter(): Create and register a Meter metric.
- registerGauge(): Register a Gauge metric.
- createScopeLogger(): Create the stats logger under given scope name.

### 1.3. Metric Sub Logger — OpStatsLogger
OpStatsLogger provides complex statistics type of Metric, usually it is used in operations such as CreateSegment, ReadSegment, we could use it to record the number of operation, time/duration of each operation.
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

- reportSuccessEvent() : Used to track Timer of a successful operation and will record the latency in Nanoseconds in required metric. 
- reportFailEvent() : Used to track Timer of a failed operation and will record the latency in Nanoseconds in required metric.  
- reportSuccessValue() : Used to track Histogram of a success value.
- reportFailValue() : Used to track Histogram of a failed value. 
- toOpStatsData() :  Used to support JMX exports and inner test.
- clear : Used to clear stats for this operation.

### 1.4 Metric Logger — interface DynamicLogger
A simple interface that only exposes simple type metrics: Counter/Gauge/Meter.
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

- incCounterValue() : Increase Counter with given value.
- updateCounterValue() : Updates the counter with given value.
- freezeCounter() : Notifies that the counter will not be updated.
- reportGaugeValue() : Reports Gauge value.
- freezeGaugeValue() : Notifies that the gauge value will not be updated.
- recordMeterEvents()  : Record the occurrence of a given number of events in Meter.


# 2. Example for starting a Metric service
This example is from file io.pravega.segmentstore.server.host.ServiceStarter. It starts Pravega SegmentStore service and a Metrics service is started as a sub service.
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
This is an example from io.pravega.segmentstore.server.host.handler.PravegaRequestProcessor. In this class, we registered two metrics: One timer (createStreamSegment), one dynamic counter (segmentReadBytes).
```java
public class PravegaRequestProcessor extends FailingRequestProcessor implements RequestProcessor {
    
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    
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
                DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_READ_BYTES, segment), reply.getData().array().length); // Increasing the counter value for the counter metric SEGMENT_READ_BYTES
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
                            DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_READ_BYTES, segment), reply.getData().array().length); // Increasing the counter value for the counter metric SEGMENT_READ_BYTES
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
From the above example, we can see the required steps to register and use a metric in desired class and method:

1. Get a StatsLogger from MetricsProvider: 
```
StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger();
```
2. Register all the desired metrics through StatsLogger:
```
static final OpStatsLogger CREATE_STREAM_SEGMENT = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);
```
3. Use these metrics within code at appropriate place where you would like to collect and record the values.
```
Metrics.CREATE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
```
Here CREATE_STREAM_SEGMENT is the name of this metric, and CREATE_STREAM_SEGMENT is the name of our Metrics logger, it will track operations of createSegment, and we will get the time of each createSegment operation happened, how long each operation takes, and other numbers computed based on them.

### 2.1.1 Output example of OpStatsLogger 
An example output of OpStatsLogger CREATE_SEGMENT reported through CSV reporter:
```
$ cat CREATE_STREAM_SEGMENT.csv 
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1480928806,1,8.973952,8.973952,8.973952,0.000000,8.973952,8.973952,8.973952,8.973952,8.973952,8.973952,0.036761,0.143306,0.187101,0.195605,calls/second,millisecond
```


### 2.2. Example for Dynamic Gauge and OpStatsLogger(Histogram) 
This is an example from io.pravega.controller.store.stream.AbstractStreamMetadataStore. In this class, we report a Dynamic Gauge which represents the open transactions and  one histogram (CREATE_STREAM).
```java
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore  {
    ...
    private static final OpStatsLogger CREATE_STREAM = STATS_LOGGER.createStats(MetricsNames.CREATE_STREAM); // get stats logger from MetricsProvider
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger(); // get dynamic logger from MetricsProvider
       
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
                            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, name), 0); // Report gauge value for Dynamic Gauge metric OPEN_TRANSACTIONS 
                        }
    
                        return result;
                    });
        }
    ...
 }
```

### 2.3 Example for Dynamic Meter
This is an example from io.pravega.segmentstore.server.SegmentStoreMetrics. In this class, we report a Dynamic Meter which represents the segments created.
```java
public final class SegmentStoreMetrics {
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    
    public void createSegment() {
            DYNAMIC_LOGGER.recordMeterEvents(this.createSegmentCount, 1);  // Record event for meter metric createSegmentCount
    } 
}
```
 
# 3. Metric reporter and Configurations
Reporters are the way through which we export all the measurements being made by the metrics. We currently provide StatsD and CSV output. It is not difficult to add new output formats, such as JMX/SLF4J.
CSV reporter will export each Metric out into one file. 
StatsD reporter will export Metrics through UDP/TCP to a StatsD server.
The reporter could be configured through MetricsConfig.
```java
public class MetricsConfig extends ComponentConfig {
    //region Members
    public final static String COMPONENT_CODE = "metrics";
    public final static String ENABLE_STATISTICS = "enableStatistics"; //enable metric, or will report nothing, default = true,  
    public final static Property<Long> DYNAMIC_CACHE_SIZE = "dynamicCacheSize"; //dynamic cache size , default = 10000000L
    public final static Property<Integer> DYNAMIC_CACHE_EVICTION_DURATION_MINUTES = "dynamicCacheEvictionDurationMs"; //dynamic cache evcition duration, default = 30
    public final static String OUTPUT_FREQUENCY = "statsOutputFrequencySeconds"; //reporter output frequency, default = 60
    public final static String METRICS_PREFIX = "metricsPrefix"; //Metrics Prefix, default = "localhost"
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

# 4. Steps to add your own Metrics
* Step 1. When start a segment store/controller service, start a Metrics service as a sub service. Reference above example in ServiceStarter.start()
```java
public class AddMetrics {
        statsProvider = MetricsProvider.getProvider();
        statsProvider.start(metricsConfig);    
    // Step 2. In the class that need Metrics: get StatsLogger through MetricsProvider; then get Metrics from StatsLogger; at last report it at the right place.

    static final StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger(); // <--- 1
    static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    
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
    DYNAMIC_LOGGER.incCounterValue(Metrics.SEGMENT_READ_BYTES, 1);
    DYNAMIC_LOGGER.reportGaugeValue(OPEN_TRANSACTIONS, 0);
    
    //in case of failure
    Metrics.CREATE_STREAM.reportFailValue(1);
    Metrics.createStreamSegment.reportFailEvent(timer.getElapsed());
    
    //to freeze
    DYNAMIC_LOGGER.freezeCounter(Metrics.SEGMENT_READ_BYTES);
    DYNAMIC_LOGGER.freezeGaugeValue(OPEN_TRANSACTIONS);
}
```

# 5. Available Metrics and their names

- Metrics in Segment Store Service.
```
segmentstore.segment.read_latency_ms
segmentstore.segment.write_latency_ms 
segmentstore.segment.create_latency_ms

//Dynamic Counter
segmentstore.segment.read_bytes.$scope.$stream.$segment.#epoch.$epoch.Counter
segmentstore.segment.write_bytes.$scope.$stream.$segment.#epoch.$epoch.Counter
```

- Tier-2 Storage Metrics: Read/Write Latency, Read/Write Rate.	
```
segmentstore.storage.read_latency_ms
segmentstore.storage.write_latency_ms

//Counter
segmentstore.storage.read_bytes.Counter
segmentstore.storage.write_bytes.Counter
```

- Cache Metrics
```
segmentstore.cache.insert_latency_ms
segmentstore.cache.get_latency
```

- Tier-1 DurableDataLog Metrics: Read/Write Latency, Read/Write Rate.	
```
segmentstore.bookkeeper.total_write_latency_ms
segmentstore.bookkeeper.write_latency_ms
segmentstore.bookkeeper.write_bytes
segmentstore.bookkeeper.write_queue_size
segmentstore.bookkeeper.write_queue_fill

//Dynamic
segmentstore.bookkeeper.bookkeeper_ledger_count.$containerId.Gauge
```

- Container-specific metrics.
```
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
segmentstore.container.operation.log_size.$containerId

//Dynamic
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
segmentstore.active_segments.$containerId.Gauge
```

- Metrics in Controller. 
```
controller.stream.created
controller.stream.sealed
controller.stream.deleted

//Dynamic
controller.transactions.created.$scope.$stream.Counter
controller.transactions.committed.$scope.$stream.Counter
controller.transactions.aborted.$scope.$stream.Counter
controller.transactions.opened.$scope.$stream.Gauge
controller.transactions.timedout.$scope.$stream.Counter
controller.segments.count.$scope.$stream.Gauge
controller.segments.splits.$scope.$stream.Counter
controller.segments.merges.$scope.$stream.Counter
controller.retention.frequency.$scope.$stream.Meter
controller.retention.truncated_size.$scope.$stream.Gauge
```

- General Metrics.
```
segmentstore.cache.size_bytes
segmentstore.cache.gen
segmentstore.thread_pool.queue_size
segmentstore.thread_pool.active_threads
```
# 6. Useful links
* [Dropwizard Metrics](https://metrics.dropwizard.io/3.1.0/apidocs)
* [Statsd_spec](https://github.com/b/statsd_spec)
* [etsy_StatsD](https://github.com/etsy/statsd)
