package com.emc.logservice.logs;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Sequential Log that contains Data Frames.
 */
public interface DataFrameLog extends SequentialLog<DataFrame, Long> {
    int getLastMagic();

    CompletableFuture<Void> recover(Duration timeout);
}