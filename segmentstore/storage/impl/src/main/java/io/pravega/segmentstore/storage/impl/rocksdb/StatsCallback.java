package io.pravega.segmentstore.storage.impl.rocksdb;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.StatisticsCollectorCallback;
import org.rocksdb.TickerType;

@Slf4j
public class StatsCallback implements StatisticsCollectorCallback {

    public void tickerCallback(TickerType tickerType, long tickerCount) {
        log.info("Ticker type is %s and count %d", tickerType.toString(), tickerCount);
    }

    public void histogramCallback(HistogramType histType,
                                  HistogramData histData) {
        log.info("HistogramType %s and average %f", histType.toString(), histData.getAverage());
        log.info("HistogramType %s and median %f", histType.toString(), histData.getMedian());
        log.info("HistogramType %s and p95 %f", histType.toString(), histData.getPercentile95());
        log.info("HistogramType %s and p99 %f", histType.toString(), histData.getPercentile99());
        log.info("HistogramType %s and std %f", histType.toString(), histData.getStandardDeviation());
    }
}
