/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.distributedlog;

import ch.qos.logback.classic.LoggerContext;
import com.twitter.distributedlog.LocalDLMEmulator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.LoggerFactory;

/**
 * Helper class that starts DistributedLog in-process and creates a namespace.
 */
@Slf4j
public final class DistributedLogStarter {
    public static void main(String[] args) throws Exception {
        // We don't need logging here at all.
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();

        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        log.info("Starting LocalDLMEmulator with host='{}', port={}.", host, port);

        // Start DistributedLog in-process.
        ServerConfiguration sc = new ServerConfiguration()
                .setJournalAdaptiveGroupWrites(false)
                .setJournalMaxGroupWaitMSec(0);
        val dlm = LocalDLMEmulator.newBuilder()
                                  .zkPort(port)
                                  .serverConf(sc)
                                  .build();
        dlm.start();

        // Wait forever. This process will be killed externally.
        Thread.sleep(Long.MAX_VALUE);
    }
}
