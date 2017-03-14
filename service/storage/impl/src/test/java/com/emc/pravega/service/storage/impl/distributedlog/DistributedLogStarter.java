/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.distributedlog;

import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.admin.DistributedLogAdmin;
import com.twitter.distributedlog.tools.Tool;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.ReflectionUtils;

/**
 * Helper class that starts DistributedLog in-process and creates a namespace.
 */
@Slf4j
public final class DistributedLogStarter {
    public static void main(String[] args) throws Exception {
        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        final String namespace = args[2];
        log.info("Starting LocalDLMEmulator with host='{}', port={}, namespace='{}'.", host, port, namespace);

        // Start DistributedLog in-process.
        ServerConfiguration sc = new ServerConfiguration()
                .setJournalAdaptiveGroupWrites(false)
                .setJournalMaxGroupWaitMSec(0);
        val dlm = LocalDLMEmulator.newBuilder()
                                  .zkPort(port)
                                  .serverConf(sc)
                                  .build();
        dlm.start();

        // Create a namespace.
        Tool tool = ReflectionUtils.newInstance(DistributedLogAdmin.class.getName(), Tool.class);
        tool.run(new String[]{
                "bind",
                "-l", "/ledgers",
                "-s", String.format("%s:%s", host, port),
                "-c", String.format("distributedlog://%s:%s/%s", host, port, namespace)});

        // Wait forever. This process will be killed externally.
        Thread.sleep(Long.MAX_VALUE);
    }
}
