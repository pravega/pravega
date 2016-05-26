package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.ConfigurationException;
import com.emc.logservice.storageabstraction.DataLogNotAvailableException;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;

import java.io.IOException;

/**
 * Created by andrei on 5/25/16.
 */
class LogHandle implements AutoCloseable {
    private final DistributedLogManager logManager;
    private boolean closed;

    public LogHandle(DistributedLogNamespace namespace, String logName) throws ConfigurationException, DataLogNotAvailableException {
        try {
            this.logManager = namespace.openLog(logName);
        }
        catch(InvalidStreamNameException ex){
            // configuration issue?
        }
        catch(IOException ex){
            // Log does not exist or some other issue happened. Note that LogNotFoundException inherits from IOException, so it's also handled here.
            throw new DataLogNotAvailableException(String.format("Unable to create DistributedLogManager for log '%s'.", logName), ex);
        }
    }

    @Override
    public void close() throws Exception {
        if (!this.closed) {
            this.logManager.close();
            this.closed = true;
        }
    }
}
