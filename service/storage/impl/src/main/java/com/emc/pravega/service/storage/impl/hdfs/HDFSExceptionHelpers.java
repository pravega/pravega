/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.FileNotFoundException;

/**
 * Helps to translated HDFS specific IOExceptions to StreamSegmentExceptions.
 * */
public class HDFSExceptionHelpers {

    /**
     * API to translated HDFS specific IOExceptions to StreamSegmentExceptions.
     * @param streamSegmentName Name of the stream segment on which the exception occurs.
     * @param e The exception to be translated
     * @return
     */
    public static Exception translateFromException(String streamSegmentName, Exception e) {
        Exception retVal = e;

        if (e instanceof RemoteException) {
            retVal = e = ((RemoteException) e).unwrapRemoteException();
        }

        if (e instanceof PathNotFoundException || e instanceof FileNotFoundException) {
            retVal = new StreamSegmentNotExistsException(streamSegmentName);
        }

        if (e instanceof FileAlreadyExistsException) {
            retVal = new StreamSegmentExistsException(streamSegmentName);
        }

        if (e instanceof AclException) {
            retVal = new StreamSegmentSealedException(streamSegmentName);
        }

        return retVal;

    }
}
