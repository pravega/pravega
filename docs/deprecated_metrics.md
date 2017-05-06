In Pravega Metric Framework, we use [Yammer Metrics](http://metrics.dropwizard.io/3.1.0/apidocs) as the underneath, and provide our own API to make it easier to use.
# 1. Metrics interfaces and use example
There are 3 basic Interface: StatsProvider, StatsLogger (short for Statistics Logger) and OpStatsLogger (short for Operation Statistics Logger, and it is included in StatsLogger).
StatsProvider provide us the whole Metric service; StatsLogger is the place that we register and get wanted Metrics( [Counter](http://metrics.dropwizard.io/3.1.0/manual/core/#counters)/[Gauge](http://metrics.dropwizard.io/3.1.0/manual/core/#gauges)/[Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers)/[Histograms](http://metrics.dropwizard.io/3.1.0/manual/core/#histograms) ); while OpStatsLogger is a sub Metric for complex ones(Timer/Histograms).
## 1.1. Metrics Service Provider — interface StatsProvider
The starting point is the StatsProvider interface, it provides start and stop method for Metric service. Regarding the reporters, currently we provide a CSV reporter and a StatsD reporter.
```
public interface StatsProvider {
    public void start(MetricsConfig conf);
    public void close();
    public StatsLogger createStatsLogger(String scope);
}
```
* start(): At start time, it provides [MetricRegistry](http://metrics.dropwizard.io/3.1.0/manual/core/#metric-registries) and reporters for our Metrics. 
* stop(): Mainly stop the reporters.
* createStatsLogger (): Create and Return a StatsLogger, which is the main class to get a Metric and do our Metric insertion and collection. 

## 1.2. Example
This example is in file com.emc.pravega.service.server.host.ServiceStarter, It is the host service start place, when we start a host service, a Metrics service should be started as a sub service.
```
public final class ServiceStarter {
    ...
    private StatsProvider statsProvider;
    ...
    private void start() {
        ...
        MetricsConfig metricsConfig = this.builderConfig.getConfig(MetricsConfig::new);
        statsProvider = (metricsConfig.enableStatistics()) ?  //< === this config determine it will report metric or not.
                        MetricsProvider.getNullProvider() :
                        MetricsProvider.getProvider();
        statsProvider.start(metricsConfig); < === here when start host service, we start Metric service.
        ...
    }
    private void shutdown() {
        if (!this.closed) {
            this.serviceBuilder.close();
            ...
            if (this.statsProvider != null) {
                statsProvider.close(); < ===
                statsProvider = null;
                log.info("Metrics statsProvider is now closed.");
            }
            this.closed = true;
        }
    }
...
}
```
## 1.3. Metric Logger — interface StatsLogger
Through this interface we can register wanted Metrics. It includes simple type [Counter](http://metrics.dropwizard.io/3.1.0/manual/core/#counters)/[Gauge](http://metrics.dropwizard.io/3.1.0/manual/core/#gauges) and some complex statistics type of Metric—OpStatsLogger, through which we provide [Timer](http://metrics.dropwizard.io/3.1.0/manual/core/#timers) and [Histograms](http://metrics.dropwizard.io/3.1.0/manual/core/#histograms).
```
public interface StatsLogger {
    public OpStatsLogger createStats(String name);
    public Counter createCounter(String name);
    public <T extends Number> void registerGauge(String name, Supplier<T> value);
}
```
* createStats (): Through this we register and get a OpStatsLogger, which use for complex statistics type of Metric.
* createCounter (): Through this we register and get a Counter Metric.
* registerGauge(): Through this we register a Gauge Metric. 

### 1.3.1. Metric Sub Logger — OpStatsLogger
As mentioned in StatsLogger, OpStatsLogger mainly provide complex statistics type of Metric, usually it is used in each operation, such as in CreateSegment, ReadSegment, we could use it to record the number of operation, time/duration of each happened operation.
```
public interface OpStatsLogger {
public void reportSuccessEvent(Duration duration);
public void reportFailEvent(Duration duration);
public void reportSuccessValue(long value);
public void reportFailValue(long value);
}
```
* reportSuccessEvent : Use when we want to track Timer of a sucessful operation, it use Yammer.Timmer inside, and will record the latency in Nanoseconds in Yammer metric. 
* reportFailEvent : Use when we want to track Timer of a failed operation, it  use Yammer.Timmer inside, and will record the latency in Nanoseconds in Yammer metric.  .
* reportSuccessValue (): Use when we want to track Histogram of a success value, we reuse the Yammer.Timer to achieve it.
* reportFailValue (): Use when we want to track Histogram of a fail value, we reuse the Yammer.Timer to achieve it. 

### 1.3.2. Example for Counter and OpStatsLogger(Timer/Histograms)
Here is an example in com.emc.pravega.service.server.host.handler.PravegaRequestProcessor. In this class, we registered 4 Metrics: 2 Timer(creaetSegment/ readSegment), 1 Histograms(segmentReadBytes), and 1 Counter(allReadBytes).
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
And here from the above example, you would find steps of how to register and use a metric in wanted class and method:

1. Get a StatsLogger from MetricsProvider: StatsLogger STATS_LOGGER = MetricsProvider.getStatsLogger();
1. Register all the wanted Metrics through StatsLogger, and put all these Metrics in a static class Metrics. public static class Metrics { … } `static final OpStatsLogger CREATE_STREAM_SEGMENT = STATS_LOGGER.createStats(CREATE_SEGMENT);`
1. Use these Metrics at the code place, which you would like to collect and record the values.   `Metrics.CREATE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsedNanos());`
Here CREATE_SEGMENT is the name of this Metrics, we put all the Metric for host in file com.emc.pravega.service.server.host.PravegaRequestStats,
 and CREATE_STREAM_SEGMENT is the name of our Metrics logger, it will track operations of createSegment, and we will get the time of each createSegment operation happened, how long each operation takes, and other numbers computed based on them.

### 1.3.3 Output example of OpStatsLogger and Counter
An example output of this OpStatsLogger CREATE_SEGMENT that reported through CSV reporter is like:
```
$ cat CREATE_SEGMENT.csv 
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1480928806,1,8.973952,8.973952,8.973952,0.000000,8.973952,8.973952,8.973952,8.973952,8.973952,8.973952,0.036761,0.143306,0.187101,0.195605,calls/second,millisecond
```
READ_STREAM_SEGMENT, and READ_BYTES_STATS are similar as this above. 
An example output of Counter READ_BYTES that reported through CSV reporter is like:
```
$ cat ALL_READ_BYTES.csv
t,count
1480928806,0
1480928866,0
1480928875,1000
```

### 1.3.4. Example for Gauge metrics
Here is an example in com.emc.pravega.service.server.host.handler.AppendProcessor. In this class, we registered a Gauge which represent current PendingReadBytes.
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
This is similar to above example, But Gauge is a special kind of Metric, It only need a register, while other Metrics need register and report, and when Metrics reporter do the report, it calls Gauge.getValue() to get former registered value.  
 
# 2. Metric reporter and Configurations
Reporters are the way that we exports all the measurements being made by its metrics. By leverage yammer, we currently provide StatsD and CSV output.It is not hard to add new output formats, such as JMX/SLF4J.
CSV reporter will export each Metric out into 1 file.csv. 
StatsD reporter will export Metrics through UDP/TCP to a StatsD server.
The reporter could be configured through MetricsConfig.
```
public class MetricsConfig extends ComponentConfig {
    //region Members
    public static final String COMPONENT_CODE = "metrics";
    public final static String ENABLE_STATISTICS = "enableStatistics"; < === enable Yammer metric, or will report nothing
    public final static String OUTPUT_FREQUENCY = "yammerStatsOutputFrequencySeconds"; < === reporter output frequency
    public final static String METRICS_PREFIX = "yammerMetricsPrefix"; 
    public final static String CSV_ENDPOINT = "yammerCSVEndpoint"; < === CSV reporter output dir
    public final static String STATSD_HOST = "yammerStatsDHost"; < === StatsD server host for the reporting
    public final static String STATSD_PORT = "yammerStatsDPort"; < === StatsD server port
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
* Step 1. When start a host/controller service, start a Metrics service as a sub service. Reference above example in ServiceStarter.start()
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
# 4. Available Metrics 

ToDo

# 5. Useful links
* [Yammer Metrics](http://metrics.dropwizard.io/3.1.0/apidocs)
* [Statsd_spec](https://github.com/b/statsd_spec)
* [etsy_StatsD](https://github.com/etsy/statsd)
