/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.storage.impl.hdfs;

import io.pravega.server.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.server.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.server.segmentstore.contracts.StreamSegmentSealedException;
import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Helps translate HDFS specific Exceptions to Pravega-equivalent Exceptions.
 */
final class HDFSExceptionHelpers {

    /**
     * Translates HDFS specific Exceptions to Pravega-equivalent Exceptions.
     *
     * @param segmentName Name of the stream segment on which the exception occurs.
     * @param e           The exception to be translated.
     * @return The translated exception.
     */
    static Throwable translateFromException(String segmentName, Throwable e) {
        Throwable retVal = e;

        if (e instanceof RemoteException) {
            retVal = e = ((RemoteException) e).unwrapRemoteException();
        }

        if (e instanceof PathNotFoundException || e instanceof FileNotFoundException) {
            retVal = new StreamSegmentNotExistsException(segmentName, e);
        }

        if (e instanceof FileAlreadyExistsException) {
            retVal = new StreamSegmentExistsException(segmentName, e);
        }

        if (e instanceof AclException) {
            retVal = new StreamSegmentSealedException(segmentName, e);
        }

        return retVal;
    }

    /**
     * Returns a new instance of the HDFS equivalent of StreamSegmentExistsException.
     *
     * @param segmentName The name of the segment to construct the Exception for.
     * @return The new exception.
     */
    static FileAlreadyExistsException segmentExistsException(String segmentName) {
        return new FileAlreadyExistsException(segmentName);
    }

    /**
     * Returns a new instance of the HDFS equivalent of StreamSegmentNotExistsException.
     *
     * @param segmentName The name of the segment to construct the Exception for.
     * @return The new exception.
     */
    static FileNotFoundException segmentNotExistsException(String segmentName) {
        return new FileNotFoundException(segmentName);
    }

    /**
     * Returns a new instance of the HDFS equivalent of StreamSegmentSealedException.
     *
     * @param segmentName The name of the segment to construct the Exception for.
     * @return The new exception.
     */
    static AclException segmentSealedException(String segmentName) {
        return new AclException(segmentName);
    }
}
