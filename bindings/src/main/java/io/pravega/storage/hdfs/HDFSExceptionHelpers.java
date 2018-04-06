/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import java.io.FileNotFoundException;
import lombok.Lombok;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Helps translate HDFS specific Exceptions to Pravega-equivalent Exceptions.
 */
final class HDFSExceptionHelpers {

    /**
     * Translates HDFS specific Exceptions to Pravega-equivalent Exceptions and re-throws them as such.
     *
     * @param segmentName Name of the stream segment on which the exception occurs.
     * @param e           The exception to be translated.
     * @return  The exception to be thrown.
     */
    static <T> T throwException(String segmentName, Throwable e) throws StreamSegmentException {
        StreamSegmentException retVal = null;
        if (e instanceof RemoteException) {
            e = ((RemoteException) e).unwrapRemoteException();
        } else if (e instanceof PathNotFoundException || e instanceof FileNotFoundException) {
            throw new StreamSegmentNotExistsException(segmentName, e);
        } else if (e instanceof FileAlreadyExistsException) {
            throw new StreamSegmentExistsException(segmentName, e);
        } else if (e instanceof AclException) {
            throw new StreamSegmentSealedException(segmentName, e);
        } else {
            throw Lombok.sneakyThrow(e);
        }
        return null;
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
