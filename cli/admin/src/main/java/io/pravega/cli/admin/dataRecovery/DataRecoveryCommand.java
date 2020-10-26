/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.dataRecovery;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.host.StorageLoader;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.StorageFactory;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

import java.io.File;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Base for any data recovery related commands.
 */
@Slf4j
public abstract class DataRecoveryCommand extends AdminCommand {
    protected final static String COMPONENT = "storage";
    private PrintStream out = System.out;

    /**
     * Creates a new instance of the DataRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    DataRecoveryCommand(CommandArgs args) {
        super(args);
    }

    StorageFactory getStorageFactory(ScheduledExecutorService executorService) {
        ServiceBuilder.ConfigSetupHelper configSetupHelper = new ServiceBuilder.ConfigSetupHelper(getServiceBuilderConfig());
        StorageLoader loader = new StorageLoader();
        return loader.load(configSetupHelper, getServiceConfig().getStorageImplementation().toString(),
                getServiceConfig().getStorageLayout(), executorService);
    }

    String setLogging(String commandName) throws Exception {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String fileName = commandName + fileSuffix + ".log";

        String filePath = System.getProperty("user.dir") + "/" + commandName + "_" + fileSuffix;
        if (getArgCount() >= 1) {
            filePath = getCommandArgs().getArgs().get(0);
            if (filePath.endsWith("/")) {
                filePath = filePath.substring(0, filePath.length()-1);
            }
        }

        // Create a directory for storing files.
        File dir = new File(filePath);
        if (!dir.exists()) {
            dir.mkdir();
        }

        File f = new File(filePath + "/" + fileName);
        if (f.exists()) {
            output(Level.INFO, "Logging File '%s' already exists.", f.getAbsolutePath());
            if (!f.delete()) {
                output(Level.ERROR, "Failed to delete the file '%s'.", f.getAbsolutePath());
                throw new Exception("Failed to delete the file " + f.getAbsolutePath());
            }
        }
        if (!f.createNewFile()) {
            output(Level.ERROR, "Failed to create file '%s'.", f.getAbsolutePath());
            throw new Exception("Failed to create file " + f.getAbsolutePath());
        }

        output(Level.DEBUG, "Logs are written to file '%s'", filePath + "/" + fileName);
        System.setProperty("logFilename", filePath + "/" + fileName);
        return filePath;
    }

    protected void output(Level level, String messageTemplate, Object... args) {
        switch (level) {
            case INFO:
                System.out.println(String.format(messageTemplate, args));
                log.info(String.format(messageTemplate, args));
                break;
            case DEBUG:
                System.out.println(String.format(messageTemplate, args));
                log.debug(String.format(messageTemplate, args));
                break;
            case ERROR:
                System.err.println(String.format(messageTemplate, args));
                log.error(String.format(messageTemplate, args));
                break;
        }
    }
}
