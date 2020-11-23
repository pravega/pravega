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

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

/**
 * Base for any data recovery related commands.
 */
public abstract class DataRecoveryCommand extends AdminCommand {
    protected final static String COMPONENT = "storage";
    protected Logger logger;

    /**
     * Creates a new instance of the DataRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    DataRecoveryCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Creates the {@link StorageFactory} instance by reading the config values.
     *
     * @param executorService   A thread pool for execution.
     * @return                  A newly created {@link StorageFactory} instance.
     */
    StorageFactory createStorageFactory(ScheduledExecutorService executorService) {
        ServiceBuilder.ConfigSetupHelper configSetupHelper = new ServiceBuilder.ConfigSetupHelper(getCommandArgs().getState().getConfigBuilder().build());
        StorageLoader loader = new StorageLoader();
        return loader.load(configSetupHelper, getServiceConfig().getStorageImplementation().toString(),
                getServiceConfig().getStorageLayout(), executorService);
    }

    /**
     * Creates logging directory and file for the command to be run. The path to the directory can be supplied on the
     * command run. By default, the path is set as current user path.
     *
     * @param commandName   The name of the command to be run.
     * @return              The path to the directory created.
     * @throws Exception    In case of a failure in creating the directory or the file.
     */
    String setLogging(String commandName) throws Exception {
        logger = Logger.getLogger(commandName);
        logger.setUseParentHandlers(false);

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

        FileHandler fh = new FileHandler(filePath + "/" + commandName + fileSuffix + ".log");
        fh.setLevel(FINER);
        DataRecoveryLogFormatter drFormatter = new DataRecoveryLogFormatter();
        fh.setFormatter(drFormatter);
        logger.addHandler(fh);

        output(FINER, "Logs are written to file '%s'", filePath + "/" + fileName);
        return filePath;
    }

    /**
     * Outputs the message to the console as well as to the log file.
     *
     * @param level             The log level of the message.
     * @param messageTemplate   The message.
     * @param args              The arguments with the message.
     */
    protected void output(Level level, String messageTemplate, Object... args) {
        if (INFO.equals(level)) {
            System.out.println(String.format(messageTemplate, args));
            logger.log(INFO, String.format(messageTemplate, args));
        } else if (FINE.equals(level)) {
            System.out.println(String.format(messageTemplate, args));
            logger.log(FINE, String.format(messageTemplate, args));
        } else if (SEVERE.equals(level)) {
            System.err.println(String.format(messageTemplate, args));
            logger.log(SEVERE, String.format(messageTemplate, args));
        }
    }

    /**
     * A log formatting class for writing log to the file.
     */
    private static class DataRecoveryLogFormatter extends Formatter {
        private final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");

        public String format(LogRecord record) {
            StringBuilder builder = new StringBuilder(1000);
            builder.append(df.format(new Date(record.getMillis()))).append(" - ");
            String source = "";
            try {
                source = Class.forName(record.getSourceClassName()).getSimpleName();
            } catch (ClassNotFoundException e) {
                source = record.getLoggerName();
            }
            builder.append("[").append(source).append(".");
            builder.append(record.getSourceMethodName()).append("] - ");
            builder.append("[").append(record.getLevel()).append("] - ");
            builder.append(formatMessage(record));
            builder.append("\n");
            return builder.toString();
        }

        public String getHead(Handler h) {
            return super.getHead(h);
        }

        public String getTail(Handler h) {
            return super.getTail(h);
        }
    }
}
