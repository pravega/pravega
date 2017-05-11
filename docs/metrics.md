<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
In Pravega Metrics Framework, we use [Dropwizard Metrics](http://metrics.dropwizard.io/3.1.0/apidocs) as the underlying library, and provide our own API to make it easier to use.
# 1. Metrics interfaces and use example
There are three basic interfaces: StatsProvider, StatsLogger (short for Statistics Logger) and OpStatsLogger (short for Operation Statistics Logger, and it is included in StatsLogger).
StatsProvider provides us the whole Metric service; StatsLogger is the place at which we register and get required Metrics ([Counter](http://metrics.dropwizard.io/3.1.0/manual/core/#counters)/[Gauge](http://metrics.dropwizard.io/3.1.0/manual/core/#gauges)/[Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers)/[Histograms](http://metrics.dropwizard.io/3.1.0/manual/core/#histograms)); while OpStatsLogger is a sub-metric for complex ones (Timer/Histograms).
## 1.1. Metrics Service Provider — Interface StatsProvider
The starting point of Pravega Metric framework is the StatsProvider interface, it provides start and stop method for Metric service. Regarding the reporters, currently we have support for CSV reporter and StatsD reporter.
```
public interface StatsProvider {
    void start(MetricsConfig conf);
    void close();
    StatsLogger createStatsLogger(String scope);
}
```
* start(): Initializes [MetricRegistry](http://metrics.dropwizard.io/3.1.0/manual/core/#metric-registries) and reporters for our Metrics service. 
* close(): Shutdown of Metrics service.
* createStatsLogger(): Creates and returns a StatsLogger instance, which is used to retrieve a metric and do metric insertion and collection in Pravega code. 

## 1.2. Example for starting a Metric service
This example is from file io.pravega.service.server.host.ServiceStarter. It starts Pravega service and a Metrics service is started as a sub service.
```
public final class ServiceStarter {
    ...
    private StatsProvider statsProvider;
    ...
    private void start() {
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
## 1.3. Metric Logger — interface StatsLogger
Using this interface we can register required metrics for simple types like [Counter](http://metrics.dropwizard.io/3.1.0/manual/core/#counters) and [Gauge](http://metrics.dropwizard.io/3.1.0/manual/core/#gauges) and some complex statistics type of Metric OpStatsLogger, through which we provide [Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers) and [Histogram](http://metrics.dropwizard.io/3.1.0/manual/core/#histograms).
```
public interface StatsLogger {
    OpStatsLogger createStats(String name);
    Counter createCounter(String name);
    <T extends Number> Gauge registerGauge(String name, Supplier<T> value);
}
```
* createStats(): Register and get a OpStatsLogger, which is used for complex type of metrics.
* createCounter(): Register and get a Counter metric.
* registerGauge(): Register a get Gauge metric. 

### 1.3.1. Metric Sub Logger — OpStatsLogger
OpStatsLogger provides complex statistics type of Metric, usually it is used in operations such as CreateSegment, ReadSegment, we could use it to record the number of operation, time/duration of each operation.
```
public interface OpStatsLogger {
    void reportSuccessEvent(Duration duration);
    void reportFailEvent(Duration duration);
    void reportSuccessValue(long value);
    void reportFailValue(long value);
}
```
* reportSuccessEvent() : Used to track Timer of a successful operation and will record the latency in Nanoseconds in required metric. 
* reportFailEvent() : Used to track Timer of a failed operation and will record the latency in Nanoseconds in required metric.  .
* reportSuccessValue() : Used to track Histogram of a success value.
* reportFailValue() : Used to track Histogram of a failed value. 

### 1.3.2. Example for Counter and OpStatsLogger(Timer/Histograms)
This is an example from io.pravega.service.server.host.handler.PravegaRequestProcessor. In this class, we registered four metrics: Two timers (createSegment/readSegment), one histograms (segmentReadBytes) and one counter (allReadBytes).
```
public class PravegaRequestProcessor extends FailingRequestProcessor implements RequestProcessor {
    …
    static final StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger(""); < === 1, get a statsLogger
    public static class Metrics {  < === 2, put all your wanted metric in this static class Metrics
        static final OpStatsLogger CREATE_STREAM_SEGMENT = STATS_LOGGER.createStats(CREATE_SEGMENT);
        static final OpStatsLogger READ_STREAM_SEGMENT = STATS_LOGGER.createStats(READ_SEGMENT);
        static final OpStatsLogger READ_BYTES_STATS = STATS_LOGGER.createStats(SEGMENT_READ_BYTES);
        static final Counter READ_BYTES = STATS_LOGGER.createCounter(ALL_READ_BYTES);
    }
    …
    @Override
    public void readSegment(ReadSegment readSegment) {
        Timer timer = new Timer();  < ===
        final String segment = readSegment.getSegment();
        final int readSize = min(MAX_READ_SIZE, max(TYPE_PLUS_LENGTH_SIZE, readSegment.getSuggestedLength())); 
        CompletableFuture<ReadResult> future = segmentStore.read(segment, readSegment.getOffset(), readSize, TIMEOUT);
        future.thenApply((ReadResult t) -> {
            Metrics.READ_STREAM_SEGMENT. reportSuccessEvent(timer.getElapsedNanos()); < === 3, use the metric
            handleReadResult(readSegment, t);
            return null;
        }).exceptionally((Throwable t) -> {
            Metrics.READ_STREAM_SEGMENT.reportFailEvent(timer.getElapsedNanos()); < ===
            handleException(segment, "Read segment", t);
            return null;
        });
    }
    private ByteBuffer copyData(List<ReadResultEntryContents> contents) {
        int totalSize = contents.stream().mapToInt(ReadResultEntryContents::getLength).sum();
        Metrics.READ_BYTES_STATS.reportSuccessfulValue(totalSize);
        Metrics.READ_BYTES.add(totalSize);
        ByteBuffer data = ByteBuffer.allocate(totalSize);
         ...
    }
    @Override
    public void createSegment(CreateSegment createStreamsSegment) {
        Timer timer = new Timer();
        CompletableFuture<Void> future = segmentStore.createStreamSegment(createStreamsSegment.getSegment(), TIMEOUT);
        future.thenApply((Void v) -> {
            Metrics.CREATE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsedNanos()); < ===
            connection.send(new SegmentCreated(createStreamsSegment.getSegment()));
            return null;
        }).exceptionally((Throwable e) -> {
            Metrics.CREATE_STREAM_SEGMENT.reportFailevent(timer.getElapsedNanos()); < ===
            handleException(createStreamsSegment.getSegment(), "Create segment", e);
            return null;
        });
    }
    …
}
```
From the above example, we can see the reuired steps of how to register and use a metric in desired class and method:

1. Get a StatsLogger from MetricsProvider: 
```
StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger();
```
1. Register all the desired metrics through StatsLogger:
```
static final OpStatsLogger CREATE_STREAM_SEGMENT = STATS_LOGGER.createStats(CREATE_SEGMENT);
```
1. Use these metrics within code at appropriate place where you would like to collect and record the values.
```
Metrics.CREATE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsedNanos());
```
Here CREATE_SEGMENT is the name of this metric, we put all the Metric for host in file io.pravega.service.server.host.PravegaRequestStats, and CREATE_STREAM_SEGMENT is the name of our Metrics logger, it will track operations of createSegment, and we will get the time of each createSegment operation happened, how long each operation takes, and other numbers computed based on them.

### 1.3.3 Output example of OpStatsLogger and Counter
An example output of OpStatsLogger CREATE_SEGMENT reported through CSV reporter:
```
$ cat CREATE_SEGMENT.csv 
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1480928806,1,8.973952,8.973952,8.973952,0.000000,8.973952,8.973952,8.973952,8.973952,8.973952,8.973952,0.036761,0.143306,0.187101,0.195605,calls/second,millisecond
```
READ_STREAM_SEGMENT, and READ_BYTES_STATS are similar to above output. 
An example output of Counter READ_BYTES reported through CSV reporter:
```
$ cat ALL_READ_BYTES.csv
t,count
1480928806,0
1480928866,0
1480928875,1000
```

### 1.3.4. Example for Gauge metrics
This is an example from io.pravega.service.server.host.handler.AppendProcessor. In this class, we registered a Gauge which represent current PendingReadBytes.
```
public class AppendProcessor extends DelegatingRequestProcessor {
    ...
    static final StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger(); < === 1. get logger from MetricsProvider
    static AtomicLong pendBytes = new AtomicLong(); < === 2. create an AtomicLong to reference the value that we want to keep in Gauge
    static { < === 3. use a static statement to execute the register command
        STATS_LOGGER.registerGauge(PENDING_APPEND_BYTES, pendBytes::get);
    }
    ...
    private void pauseOrResumeReading() {
        int bytesWaiting;
        synchronized (lock) {
            bytesWaiting = waitingAppends.values()
                .stream()
                .mapToInt(a -> a.getData().readableBytes())
                .sum();
        }
        // Registered gauge value
        pendBytes.set(bytesWaiting); < === 4. once the wanted value(here it is bytesWaiting) updated, update the registered AtomicLong in Gauge
        ...
    }
    ...
 }
```
This is similar to above example, but Gauge is a special kind of Metric, it only needs to register, unlike other Metrics which need to register and report, and when Metrics reporter do the report, it calls Gauge.getValue() to get former registered value.  
 
# 2. Metric reporter and Configurations
Reporters are the way through which we export all the measurements being made by the metrics. We currently provide StatsD and CSV output. It is not difficult to add new output formats, such as JMX/SLF4J.
CSV reporter will export each Metric out into one file. 
StatsD reporter will export Metrics through UDP/TCP to a StatsD server.
The reporter could be configured through MetricsConfig.
```
public class MetricsConfig extends ComponentConfig {
    //region Members
    public static final String COMPONENT_CODE = "metrics";
    public final static String ENABLE_STATISTICS = "enableStatistics"; < === enable metric, or will report nothing
    public final static String OUTPUT_FREQUENCY = "statsOutputFrequencySeconds"; < === reporter output frequency
    public final static String METRICS_PREFIX = "metricsPrefix"; 
    public final static String CSV_ENDPOINT = "csvEndpoint"; < === CSV reporter output dir
    public final static String STATSD_HOST = "statsDHost"; < === StatsD server host for the reporting
    public final static String STATSD_PORT = "statsDPort"; < === StatsD server port
    public final static boolean DEFAULT_ENABLE_STATISTICS = true;
    public final static int DEFAULT_OUTPUT_FREQUENCY = 60;
    public final static String DEFAULT_METRICS_PREFIX = "host";
    public final static String DEFAULT_CSV_ENDPOINT = "/tmp/csv";
    public final static String DEFAULT_STATSD_HOST = "localhost";
    public final static int DEFAULT_STATSD_PORT = 8125;
    ...
}
```

# 3. Steps to add your own Metrics
* Step 1. When start a segment store/controller service, start a Metrics service as a sub service. Reference above example in ServiceStarter.start()
```
        statsProvider = MetricsProvider.getProvider();
        statsProvider.start(metricsConfig);    
```
* Step 2. In the class that need Metrics: get StatsLogger through MetricsProvider; then get Metrics from StatsLogger; at last report it at the right place.
```
    static final StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger(); < === 1
    public static class Metrics { < === 2
        static final OpStatsLogger CREATE_STREAM_SEGMENT = STATS_LOGGER.createStats(CREATE_SEGMENT);
        static final OpStatsLogger READ_STREAM_SEGMENT = STATS_LOGGER.createStats(READ_SEGMENT);
        static final OpStatsLogger READ_BYTES_STATS = STATS_LOGGER.createStats(SEGMENT_READ_BYTES);
        static final Counter READ_BYTES = STATS_LOGGER.createCounter(ALL_READ_BYTES);
    }
    ...
    Metrics.CREATE_STREAM_SEGMENT.reportFailure(timer.getElapsedNanos()); < === 3
```
# 4. Available Metrics and their names
* Segment Store: Bytes In/Out Rate, Read/Write Latency.
````
DYNAMIC.$scope.$stream.$segment.segment_read_bytes
DYNAMIC.$scope.$stream.$segment.segment_write_bytes
host.segment_read_latency_ms
host.segment_write_latency_ms 
````

* Stream Controllers: Stream creation/deletion/sealed, Segment Merging/Splitting Rate, Transactions Open/Commit/Drop/Abort
````
controller.stream_created
controller.stream_sealed
controller.stream_deleted
DYNAMIC.$scope.$stream.segments_count
DYNAMIC.$scope.$stream.segments_splits
DYNAMIC.$scope.$stream.segments_merges
DYNAMIC.$scope.$stream.transactions_created
DYNAMIC.$scope.$stream.transactions_committed
DYNAMIC.$scope.$stream.transactions_aborted
DYNAMIC.$scope.$stream.transactions_opened
````
* Tier-2 Storage Metrics: Read/Write Latency, Read/Write Rate	
````
hdfs.hdfs_read_latency_ms
hdfs.hdfs_write_latency_ms
hdfs.hdfs_read_bytes
hdfs.hdfs_write_bytes
````

# 5. Useful links
* [Dropwizard Metrics](http://metrics.dropwizard.io/3.1.0/apidocs)
* [Statsd_spec](https://github.com/b/statsd_spec)
* [etsy_StatsD](https://github.com/etsy/statsd)
